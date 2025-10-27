from typing import Dict, List, Optional
import re
import time
import json
import logging

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

from livetiming.racing import Stat
from livetiming.service import BaseService

# URLs passed to fetchers must be bytes, not strings.
DATA_SOURCE_URL = b'https://results.ris-timing.be/2cvrt/live/live2025.htm'


def _parse_lap_time(txt: str) -> Optional[float]:
    """
    Convert strings like '1:23.456' or '23.456' into seconds (float).
    Returns None if parsing fails or txt is empty/'-'.
    """
    if not txt:
        return None
    t = txt.strip()
    if not t or t in {'-', '—', 'NA', 'N/A'}:
        return None
    # Accept formats: M:SS.xxx  or  SS.xxx
    m = re.match(r'^(?:(\d+):)?(\d{1,2})(?:[.,](\d{1,3}))?$', t)
    if not m:
        return None
    minutes = int(m.group(1) or 0)
    seconds = int(m.group(2))
    millis = int((m.group(3) or '0').ljust(3, '0'))  # normalize to ms
    return minutes * 60 + seconds + millis / 1000.0


def _text(el) -> str:
    return re.sub(r'\s+', ' ', (el.get_text(separator=' ', strip=True) if el else '')).strip()


class Service(BaseService):
    """
    timing71 service for R.I.S. live timing:
    https://results.ris-timing.be/2cvrt/live/live2025.htm
    """

    # We'll just poll on our own by implementing getRaceState() and enabling auto_poll.
    auto_poll = True

    # Give attribution to your data source! Name and optional URL.
    attribution = ['R.I.S. Timing', 'https://results.ris-timing.be/']

    def __init__(self, args, extra_args):
        super().__init__(args, extra_args)
        self._last_fetch_ok = False
        self._last_session_name = None
        self._last_flag = None
        self._last_remaining = None
        # Setup a simple logger that writes each scrape to a file and to stdout
        # so the plugin's scraping output can be observed in real time.
        self._out_logger = logging.getLogger('ris2cvrt')
        if not self._out_logger.handlers:
            self._out_logger.setLevel(logging.INFO)
            fh = logging.FileHandler('ris2cvrt_scrape.log')
            fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
            self._out_logger.addHandler(fh)
            sh = logging.StreamHandler()
            sh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
            self._out_logger.addHandler(sh)

    def getName(self):
        return 'RIS 2CVRT Live'

    def getDefaultDescription(self):
        return 'Live timing scraped from R.I.S. (2CVRT)'

    def getVersion(self):
        return '1.0.0'

    def getPollInterval(self):
        # Run as often as requested: user asked for once-per-second scraping.
        return 1

    def getColumnSpec(self):
        # You can add extra columns later if needed; these are the essentials.
        return [
            Stat.NUM,
            Stat.STATE,
            Stat.DRIVER,   # we’ll map Team/Drivers here
            Stat.LAST_LAP,
            Stat.BEST_LAP,
        ]

    def getTrackDataSpec(self):
        # Not provided by this page
        return []

    # --- Core polling hook ---
    def getRaceState(self):
        """
        Fetch and parse the R.I.S. live HTML and convert to timing71 state.
        """
        url = DATA_SOURCE_URL.decode('utf-8')

        # Try to fetch the XML endpoint first (RIS publishes .xml files frequently).
        # We'll try a couple of reasonable derived locations based on the HTML URL,
        # then fall back to the original HTML parsing if XML is unavailable.
        xml_text = None
        xml_url_tried = None
        try:
            base_no_ext = url.rsplit('.', 1)[0]
            candidates = [f"{base_no_ext}.xml"]
            # Also try replacing the filename with the same name + .xml
            filename = url.rsplit('/', 1)[-1]
            if '.' in filename:
                candidates.append(url.replace(filename, filename.rsplit('.', 1)[0] + '.xml'))

            for xu in candidates:
                try:
                    xr = requests.get(xu, timeout=5)
                    if xr.status_code == 200 and xr.text and xr.text.lstrip().startswith('<?xml'):
                        xml_text = xr.text
                        xml_url_tried = xu
                        break
                except Exception:
                    continue
        except Exception:
            xml_text = None

        if xml_text is not None:
            # Parse XML
            try:
                root = ET.fromstring(xml_text)
            except Exception as e:
                # XML parse failed; log and fall back to HTML scraping
                try:
                    self.log.failure("Exception parsing XML: {log_failure}")
                except Exception:
                    pass
                xml_text = None

        if xml_text is None:
            # fallback to HTML fetch
            try:
                r = requests.get(url, timeout=10)
                r.raise_for_status()
                html = r.text
                self._last_fetch_ok = True
            except Exception as e:
                # Log to twisted logger and our own output logger so the error is visible.
                try:
                    self.log.error('Fetch failed: {e}', e=e)
                except Exception:
                    pass
                try:
                    self._out_logger.exception('Fetch failed')
                except Exception:
                    # Best-effort: print if logger failed
                    print(f'Fetch failed: {e}', flush=True)
                self._last_fetch_ok = False
                # Return whatever we can (empty cars, keep prior session if available)
                return {
                    'cars': [],
                    'session': {
                        'name': self._last_session_name or '2CVRT',
                        'flag': self._last_flag or '',
                        'remaining': self._last_remaining,  # seconds or None
                        'clock': int(time.time()),
                        'source_ok': False,
                    }
                }

        # If xml_text was present we will parse XML below; otherwise parse HTML.
        soup = None
        if xml_text is None:
            soup = BeautifulSoup(html, 'html.parser')

        # --- Try to discover session meta (flag, remaining, label) if present ---
        session_name = None
        flag = None
        remaining = None  # seconds

        # If we have XML, extract session info from <lineinfo>
        if xml_text is not None:
            try:
                lineinfo = root.find('.//lineinfo') or root.find('lineinfo')
                if lineinfo is not None:
                    session_name = (lineinfo.findtext('seance') or lineinfo.findtext('race') or '').strip()
                    # messageDC2 often contains flag text like 'GREEN FLAG'
                    flag = (lineinfo.findtext('messageDC2') or lineinfo.findtext('messageDC1') or '').strip()
                    remain_txt = (lineinfo.findtext('remain') or '').strip()
                    if remain_txt:
                        # Parse HH:MM:SS or MM:SS
                        parts = remain_txt.split(':')
                        try:
                            parts = [int(p) for p in parts]
                            if len(parts) == 3:
                                remaining = parts[0]*3600 + parts[1]*60 + parts[2]
                            elif len(parts) == 2:
                                remaining = parts[0]*60 + parts[1]
                            elif len(parts) == 1:
                                remaining = parts[0]
                        except Exception:
                            remaining = None

            except Exception:
                # don't fail entirely on XML parsing errors
                pass
        else:
            # Many RIS pages have a header line like: "Race :  Session :  DAY TIME :  REMAIN :"
            # If not found, we keep them as None/empty.
            # Heuristic: look for any small header/banner text with "REMAIN" or "DAY TIME"
            banner = soup.find(text=re.compile(r'REMAIN|DAY\s*TIME', re.I))
            if banner:
                # Often values are adjacent; we keep it simple and leave as None if we can’t confidently parse.
                pass

        # --- Find the main live table ---
        table = None
        # 1) Prefer a table with typical RIS headers present
        for candidate in soup.find_all('table'):
            headers = [h for h in candidate.find_all(['th', 'td'])]
            head_txt = ' '.join(_text(h).lower() for h in headers[:15])
            if any(k in head_txt for k in ['pos', 'now', 'num', 'team', 'drivers', 'best', 'last']):
                table = candidate
                break
        # 2) Fallback: the first sizable table
        if table is None:
            tables = soup.find_all('table')
            if tables:
                table = tables[0]

        cars: List[Dict] = []

        # If we parsed XML, try to extract car rows from XML structure first.
        if xml_text is not None:
            try:
                car_nodes = []
                for tag in ('ligne', 'line', 'car', 'row', 'item'):
                    car_nodes.extend(root.findall('.//{}'.format(tag)))

                for node in car_nodes:
                    # gather child text into a dict
                    children = {c.tag.lower(): (c.text or '').strip() for c in list(node)}

                    def cget(*alts):
                        for a in alts:
                            v = children.get(a)
                            if v:
                                return v
                        return ''

                    num = cget('num', 'number', 'car', '#', 'n', 'n°', 'nº')
                    who = cget('team', 'drivers', 'driver', 'pilote')
                    last_raw = cget('last', 'lastlap', 'last_time', 'dernier')
                    best_raw = cget('best', 'bestlap', 'best_time', 'meilleur')
                    row = {
                        Stat.NUM.key: num,
                        Stat.STATE.key: cget('state', 'now', 'status') or '',
                        Stat.DRIVER.key: who,
                        Stat.LAST_LAP.key: _parse_lap_time(last_raw),
                        Stat.BEST_LAP.key: _parse_lap_time(best_raw),
                    }
                    # skip empty rows
                    if num:
                        cars.append(row)
            except Exception:
                # If XML doesn't include car rows or format is unexpected, fall back to HTML parsing below.
                cars = []

        # If we didn't get cars from XML, fall back to HTML table scraping.
        if not cars and table:
            # Build header map (col name -> index), tolerant to wording
            header_row = None
            for tr in table.find_all('tr'):
                # Pick the first row that looks like headers (many th OR recognizable labels)
                ths = tr.find_all('th')
                tds = tr.find_all('td')
                cells = ths if ths else tds
                labels = [_text(c).lower() for c in cells]
                if any(lbl in labels for lbl in ['pos', 'now', 'num', '#', 'team', 'drivers', 'last', 'best', 'best time']):
                    header_row = tr
                    break

            col_idx: Dict[str, int] = {}
            if header_row:
                labels = [_text(c).lower() for c in header_row.find_all(['th', 'td'])]
                def idx(*alts) -> Optional[int]:
                    for a in alts:
                        if a in labels:
                            return labels.index(a)
                    return None

                col_idx = {
                    'pos': idx('pos', 'position'),
                    'state': idx('now', 'state', 'status'),
                    'num': idx('num', '#', 'car', 'n°', 'nº'),
                    'team': idx('team', 'name'),
                    'drivers': idx('drivers', 'driver', 'pilote', 'pilotes'),
                    'last': idx('last', 'last time', 'last lap', 'dernier', 'dernier tour'),
                    'best': idx('best', 'best time', 'best lap', 'meilleur', 'meilleur tour'),
                    'lap': idx('lap', 'laps', 'tour', 'tours'),
                    'gap': idx('gap', 'ecart', 'écart'),
                }

                # Data rows = rows after header_row
                started = False
                for tr in table.find_all('tr'):
                    if tr is header_row:
                        started = True
                        continue
                    if not started:
                        continue

                    tds = tr.find_all('td')
                    if not tds:
                        continue

                    def get(idx_name: str) -> str:
                        i = col_idx.get(idx_name)
                        return _text(tds[i]) if (i is not None and i < len(tds)) else ''

                    num = get('num')
                    if not num:
                        # Skip empty lines or separators
                        continue

                    # Build a row for timing71
                    team_txt = get('team')
                    drv_txt = get('drivers')
                    who = team_txt or drv_txt

                    last_raw = get('last')
                    best_raw = get('best')

                    row = {
                        Stat.NUM.key: num,
                        Stat.STATE.key: get('state') or '',    # RUN, PIT, STOP, etc (if available)
                        Stat.DRIVER.key: who,
                        Stat.LAST_LAP.key: _parse_lap_time(last_raw),
                        Stat.BEST_LAP.key: _parse_lap_time(best_raw),
                    }
                    cars.append(row)

        # Emit the scraped payload to the out logger (file + stdout) so the
        # plugin operator can observe exactly what was parsed in real time.
        try:
            payload = {
                'timestamp': int(time.time()),
                'source_url': url,
                'cars': cars,
                'session': {
                    'name': self._last_session_name or '2CVRT',
                    'flag': self._last_flag or '',
                    'remaining': self._last_remaining,
                    'clock': int(time.time()),
                    'source_ok': True,
                }
            }
            # Log as compact JSON to the file/console
            self._out_logger.info(json.dumps(payload, default=str))
        except Exception:
            # Never let logging break the scraper
            try:
                self._out_logger.exception('Failed to log payload')
            except Exception:
                pass

        # Keep last known values in case the banner/session is sparse
        if session_name:
            self._last_session_name = session_name
        if flag:
            self._last_flag = flag
        if remaining is not None:
            self._last_remaining = remaining

        return {
            'cars': cars,
            'session': {
                'name': self._last_session_name or '2CVRT',
                'flag': self._last_flag or '',
                'remaining': self._last_remaining,  # seconds if known
                'clock': int(time.time()),
                'source_ok': True,
            }
        }

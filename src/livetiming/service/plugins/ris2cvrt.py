from typing import Dict, List, Optional
import re
import time

import requests
from bs4 import BeautifulSoup

from livetiming.racing import Stat
from livetiming.service import BaseService

# URLs passed to fetchers must be bytes, not strings.
DATA_SOURCE_URL = b'https://results.ris-timing.be/2cvrt/live/live2025.htm'


def _parse_lap_time(txt: str) -> Optional[float]:
    if not txt:
        return None
    t = txt.strip()
    if not t or t in {'-', '—', 'NA', 'N/A'}:
        return None
    m = re.match(r'^(?:(\d+):)?(\d{1,2})(?:[.,](\d{1,3}))?$', t)
    if not m:
        return None
    minutes = int(m.group(1) or 0)
    seconds = int(m.group(2))
    millis = int((m.group(3) or '0').ljust(3, '0'))
    return minutes * 60 + seconds + millis / 1000.0


def _text(el) -> str:
    import re
    return re.sub(r'\s+', ' ', (el.get_text(separator=' ', strip=True) if el else '')).strip()


class Service(BaseService):
    auto_poll = True
    attribution = ['R.I.S. Timing', 'https://results.ris-timing.be/']

    def __init__(self, args, extra_args):
        super().__init__(args, extra_args)
        self._last_fetch_ok = False
        self._last_session_name = None
        self._last_flag = None
        self._last_remaining = None

    def getName(self):
        return 'RIS 2CVRT Live'

    def getDefaultDescription(self):
        return 'Live timing scraped from R.I.S. (2CVRT)'

    def getVersion(self):
        return '1.0.0'

    def getPollInterval(self):
        return 3

    def getColumnSpec(self):
        return [
            Stat.NUM,
            Stat.STATE,
            Stat.DRIVER,
            Stat.LAST_LAP,
            Stat.BEST_LAP,
        ]

    def getTrackDataSpec(self):
        return []

    def getRaceState(self):
        url = DATA_SOURCE_URL.decode('utf-8')

        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            html = r.text
            self._last_fetch_ok = True
        except Exception as e:
            self.log.error('Fetch failed: {e}', e=e)
            self._last_fetch_ok = False
            return {
                'cars': [],
                'session': {
                    'name': self._last_session_name or '2CVRT',
                    'flag': self._last_flag or '',
                    'remaining': self._last_remaining,
                    'clock': int(time.time()),
                    'source_ok': False,
                }
            }

        soup = BeautifulSoup(html, 'html.parser')

        session_name = None
        flag = None
        remaining = None

        table = None
        for candidate in soup.find_all('table'):
            headers = [h for h in candidate.find_all(['th', 'td'])]
            head_txt = ' '.join(_text(h).lower() for h in headers[:15])
            if any(k in head_txt for k in ['pos', 'now', 'num', 'team', 'drivers', 'best', 'last']):
                table = candidate
                break

        if table is None:
            tables = soup.find_all('table')
            if tables:
                table = tables[0]

        cars: List[Dict] = []
        if table:
            header_row = None
            for tr in table.find_all('tr'):
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
                        continue

                    team_txt = get('team')
                    drv_txt = get('drivers')
                    who = team_txt or drv_txt

                    last_raw = get('last')
                    best_raw = get('best')

                    row = {
                        Stat.NUM.key: num,
                        Stat.STATE.key: get('state') or '',
                        Stat.DRIVER.key: who,
                        Stat.LAST_LAP.key: _parse_lap_time(last_raw),
                        Stat.BEST_LAP.key: _parse_lap_time(best_raw),
                    }
                    cars.append(row)

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
                'remaining': self._last_remaining,
                'clock': int(time.time()),
                'source_ok': True,
            }
        }

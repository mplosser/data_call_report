"""
01b_download_ffiec_cdr.py

Download FFIEC CDR "Call Reports -- Single Period" bulk ZIPs (2011Q1 onward) from
https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx, the source that 05_parse_ffiec.py
parses. Replaces the manual download the README used to require.

The page is an ASP.NET WebForms form: a GET for the ViewState, a postback that selects the
product (which fills the dates list), and a postback with a date, the tab-delimited format
and the Download button, which returns the ZIP. No credentials, no API key.

Usage:
    python 01b_download_ffiec_cdr.py                 # every quarter the site offers that is not on disk
    python 01b_download_ffiec_cdr.py --check         # newest quarter published vs newest on disk; no download
    python 01b_download_ffiec_cdr.py --quarters 2025Q4 2026Q1
    python 01b_download_ffiec_cdr.py --start 2024Q1  # only quarters from 2024Q1

Files land in data/raw/ffiec/ under the site's own name, e.g.
"FFIEC CDR Call Bulk All Schedules 12312025.zip", which is what 05_parse_ffiec.py expects.
The site offers 2001Q1 onward; this repository uses the Chicago Fed files through 2010Q4
(they carry more items for that era), so the default start is 2011Q1.
"""

import argparse
import io
import re
import sys
import time
import zipfile
from pathlib import Path

import requests

URL = "https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx"
PRODUCT = "ReportingSeriesSinglePeriod"          # "Call Reports -- Single Period"
DEFAULT_OUTPUT_DIR = Path("data/raw/ffiec")
DEFAULT_START = "2011Q1"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/128.0 Safari/537.36"}
DELAY_SECONDS = 2.0


def _hidden(html: str) -> dict:
    return {m.group(1): m.group(2)
            for m in re.finditer(r'<input type="hidden" name="(__[A-Z]+)"[^>]*value="([^"]*)"', html)}


def _quarter_label(mmddyyyy: str) -> str:
    mm, dd, yyyy = mmddyyyy.split("/")
    return f"{yyyy}Q{(int(mm) - 1) // 3 + 1}"


def _site_filename(mmddyyyy: str) -> str:
    return f"FFIEC CDR Call Bulk All Schedules {mmddyyyy.replace('/', '')}.zip"


class CDRSession:
    """One browser-like session against the bulk download form."""

    def __init__(self, timeout: int = 600):
        self.s = requests.Session()
        self.s.headers.update(HEADERS)
        self.timeout = timeout
        self._fields = None
        self._dates = None

    def _post(self, data: dict, **kw):
        r = self.s.post(URL, data=data, timeout=self.timeout, **kw)
        r.raise_for_status()
        return r

    def available_dates(self) -> dict:
        """{'12/31/2025': '150', ...} -- the option value the form wants for each date."""
        if self._dates is None:
            r = self.s.get(URL, timeout=self.timeout)
            r.raise_for_status()
            fields = _hidden(r.text)
            r = self._post({**fields, "__EVENTTARGET": "ctl00$MainContentHolder$ListBox1",
                            "__EVENTARGUMENT": "", "ctl00$MainContentHolder$ListBox1": PRODUCT})
            self._fields = _hidden(r.text)
            block = re.search(r'<select name="ctl00\$MainContentHolder\$DatesDropDownList"[^>]*>(.*?)</select>',
                              r.text, re.S)
            if not block:
                raise RuntimeError("the dates list did not appear after selecting the product; the form has changed")
            self._dates = {label: value for value, label in re.findall(r'<option[^>]*value="([^"]*)"[^>]*>([^<]*)', block.group(1))}
        return self._dates

    def download(self, mmddyyyy: str) -> bytes:
        dates = self.available_dates()
        r = self._post({**self._fields, "__EVENTTARGET": "", "__EVENTARGUMENT": "",
                        "ctl00$MainContentHolder$ListBox1": PRODUCT,
                        "ctl00$MainContentHolder$DatesDropDownList": dates[mmddyyyy],
                        "ctl00$MainContentHolder$FormatType": "TSVRadioButton",
                        "ctl00$MainContentHolder$TabStrip1$Download_0": "Download"}, stream=True)
        if "octet-stream" not in r.headers.get("Content-Type", ""):
            raise RuntimeError(f"expected a ZIP for {mmddyyyy}, got {r.headers.get('Content-Type')}")
        buf = io.BytesIO()
        for chunk in r.iter_content(1 << 20):
            buf.write(chunk)
        data = buf.getvalue()
        zipfile.ZipFile(io.BytesIO(data)).testzip()   # raises if not a valid ZIP
        return data


def on_disk(output_dir: Path) -> dict:
    """{quarter label: path} for the bulk ZIPs already downloaded."""
    out = {}
    for p in output_dir.glob("FFIEC CDR Call Bulk All Schedules *.zip"):
        m = re.search(r"(\d{2})(\d{2})(\d{4})\.zip$", p.name)
        if m:
            out[f"{m.group(3)}Q{(int(m.group(1)) - 1) // 3 + 1}"] = p
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    ap.add_argument("--start", default=DEFAULT_START, help=f"first quarter to consider (default {DEFAULT_START})")
    ap.add_argument("--quarters", nargs="*", help="explicit quarters, e.g. 2025Q4 2026Q1 (default: every missing one)")
    ap.add_argument("--check", action="store_true", help="report newest published vs newest on disk; download nothing")
    ap.add_argument("--force", action="store_true", help="re-download quarters already on disk")
    args = ap.parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cdr = CDRSession()
    try:
        dates = cdr.available_dates()
    except requests.RequestException as e:
        print(f"ERROR: could not reach {URL}: {e}")
        return 2
    published = {_quarter_label(d): d for d in dates}
    have = on_disk(out_dir)
    newest_pub, newest_disk = max(published), (max(have) if have else None)
    wanted = sorted(q for q in published if q >= args.start)
    missing = [q for q in wanted if q not in have]

    print(f"CDR offers {len(published)} quarters, {min(published)} .. {newest_pub}")
    print(f"on disk: {len(have)} quarters, newest {newest_disk}")
    print(f"missing from {args.start}: {', '.join(missing) if missing else 'none'}")
    if args.check:
        return 0 if not missing else 1

    todo = args.quarters or missing
    if args.force and args.quarters:
        pass
    else:
        todo = [q for q in todo if q not in have or args.force]
    bad = [q for q in todo if q not in published]
    if bad:
        print(f"ERROR: not offered by the site: {', '.join(bad)}")
        return 2
    for i, q in enumerate(todo):
        d = published[q]
        dest = out_dir / _site_filename(d)
        print(f"[{i + 1}/{len(todo)}] {q} ({d}) -> {dest.name}", flush=True)
        for attempt in range(3):
            try:
                data = cdr.download(d)
                dest.write_bytes(data)
                print(f"    {len(data) / 1e6:.1f} MB", flush=True)
                break
            except (requests.RequestException, zipfile.BadZipFile, RuntimeError) as e:
                print(f"    attempt {attempt + 1} failed: {e}")
                time.sleep(10 * (attempt + 1))
        else:
            print(f"ERROR: giving up on {q}")
            return 1
        time.sleep(DELAY_SECONDS)
    if todo:
        print(f"\nDone. Parse with: python 05_parse_ffiec.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())

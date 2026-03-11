import os
import re
import time
import tempfile
import datetime
import requests
import feedparser
from rapidfuzz import fuzz
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from PyPDF2 import PdfReader
from multiprocessing import Process, Queue
from dateutil import parser as dateparser

# ---------------- CONFIG ----------------
RSS_URL = 'https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml'
COMPANY_FILE = 'companies.txt'
CALENDAR_ID = 'fcb0ebfa795ba8af091f332acac0c5f0a33c5bd4982ef4db622bb9467188d11c@group.calendar.google.com'
FUZZY_THRESHOLD = 98
EVENT_TAG = "[AUTO:NSE_RSS_SCRIPT]"
GUEST_EMAIL = os.environ.get('GCAL_GUEST_EMAIL', "")
MAX_PDFS_PER_RUN = 10
PDF_PARSE_TIMEOUT = 30
HTTP_CONNECT_TIMEOUT = 10
HTTP_READ_TIMEOUT = 20
# ----------------------------------------


def normalize(text):
    return re.sub(r'[^a-zA-Z0-9]', '', text or '').lower()


def get_company_names():
    print("[STEP] get_company_names: start")
    if not os.path.exists(COMPANY_FILE):
        print(f"[ERROR] Company file {COMPANY_FILE} does not exist.")
        print("[STEP] get_company_names: end (empty list)")
        return []
    with open(COMPANY_FILE, 'r', encoding='utf-8') as f:
        data = f.read()
        if ',' in data:
            companies = [name.strip() for name in data.split(',') if name.strip()]
        else:
            companies = [line.strip() for line in data.splitlines() if line.strip()]
    print(f"[INFO] Loaded companies: {companies}")
    print("[STEP] get_company_names: end")
    return companies


def google_calendar_service():
    print("[STEP] google_calendar_service: start")
    try:
        creds = Credentials.from_service_account_file(
            'service-account.json',
            scopes=['https://www.googleapis.com/auth/calendar']
        )
        service = build('calendar', 'v3', credentials=creds)
        print("[INFO] Google Calendar service initialized.")
        print("[STEP] google_calendar_service: end")
        return service
    except Exception as e:
        print(f"[ERROR] Failed to initialize Google Calendar service: {e}")
        print("[STEP] google_calendar_service: end (error)")
        raise


def fetch_rss_entries():
    print("[STEP] fetch_rss_entries: start")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        print(f"[INFO] Fetching RSS from {RSS_URL} with timeout ({HTTP_CONNECT_TIMEOUT}, {HTTP_READ_TIMEOUT})")
        r = requests.get(RSS_URL, headers=headers, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
        print(f"[INFO] RSS HTTP status: {r.status_code}")
        if r.status_code != 200:
            print(f"[ERROR] Failed to fetch RSS feed. Status: {r.status_code}")
            print("[STEP] fetch_rss_entries: end (empty)")
            return []
        entries = feedparser.parse(r.content).entries
        print(f"[INFO] {len(entries)} entries fetched from RSS.")
        print("[STEP] fetch_rss_entries: end")
        return entries
    except Exception as e:
        print(f"[ERROR] Exception during RSS fetch: {e}")
        print("[STEP] fetch_rss_entries: end (error)")
        return []


def filter_entries(entries, companies):
    print("[STEP] filter_entries: start")
    print(f"[INFO] Companies for this filter call: {companies}")
    allowed_keywords = [
        'analyst', 'analysts', 'institutional', 'investor',
        'concall', 'conference call', 'conferencecall',
        'meet', 'call', 'meetconcall', 'meet/concall', 'Trading'
    ]
    allowed_keywords_norm = [normalize(k) for k in allowed_keywords]
    matches = []

    for idx, entry in enumerate(entries):
        try:
            raw_title = entry.title if hasattr(entry, 'title') else ""
            title = normalize(raw_title)
            summary = normalize(entry.get('summary', ''))
            content = title + " " + summary

            print(f"[DEBUG] Entry {idx}: title='{raw_title}'")

            for company in companies:
                score = fuzz.partial_ratio(normalize(company), title)
                key_hit = any(k in content for k in allowed_keywords_norm)
                if score >= FUZZY_THRESHOLD and key_hit:
                    print(f"[MATCH] {company} — '{raw_title}' (Score={score}, key_hit={key_hit})")
                    matches.append(entry)
                    break
        except Exception as e:
            print(f"[ERROR] While filtering '{getattr(entry, 'title', 'Unknown')}': {e}")

    print(f"[INFO] filter_entries: total matches = {len(matches)}")
    print("[STEP] filter_entries: end")
    return matches


# ---------- safe PDF text extraction with timeout ----------

def _extract_pdf_text_worker(path, q):
    try:
        reader = PdfReader(path)
        text = ""
        for page_index, page in enumerate(reader.pages):
            # Log per page to see if it hangs on a specific one
            print(f"[DEBUG] _extract_pdf_text_worker: extracting page {page_index}")
            text += page.extract_text() or ""
        q.put(text)
    except Exception as e:
        q.put(f"__ERROR__{e}")


def safe_extract_pdf_text(path, timeout=PDF_PARSE_TIMEOUT):
    print(f"[STEP] safe_extract_pdf_text: start (timeout={timeout}s)")
    q = Queue()
    p = Process(target=_extract_pdf_text_worker, args=(path, q))
    p.start()
    p.join(timeout)
    if p.is_alive():
        print("[WARN] PDF parsing exceeded timeout, terminating worker process.")
        p.terminate()
        p.join()
        print("[STEP] safe_extract_pdf_text: end (timeout)")
        return ""
    if not q.empty():
        out = q.get()
        if isinstance(out, str) and out.startswith("__ERROR__"):
            print(f"[WARN] PDF parse error: {out}")
            print("[STEP] safe_extract_pdf_text: end (error)")
            return ""
        print("[STEP] safe_extract_pdf_text: end (success)")
        return out
    print("[WARN] safe_extract_pdf_text: no output from worker.")
    print("[STEP] safe_extract_pdf_text: end (no output)")
    return ""

# -----------------------------------------------------------


def parse_pdf_details(pdf_url):
    print("[STEP] parse_pdf_details: start")
    print(f"[INFO] PDF URL candidate: {pdf_url}")

    # Guard: only try PDFs
    if not pdf_url or not pdf_url.lower().endswith('.pdf'):
        print(f"[WARN] Link does not look like a PDF, skipping PDF parse: {pdf_url}")
        print("[STEP] parse_pdf_details: end (non-pdf)")
        return {'date': '', 'time': '', 'dial_in': '', 'registration_link': '', 'host': '', 'contacts': []}

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; NSECorporateFilingsBot/1.0; +https://www.nseindia.com)",
        "Accept": "application/pdf",
        "Connection": "keep-alive",
    }

    session = requests.Session()
    retries = Retry(total=2, backoff_factor=2, status_forcelist=[500, 502, 503, 504, 429])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    text = ""

    for attempt in range(2):
        try:
            print(f"[INFO] PDF download attempt {attempt+1} with timeout=({HTTP_CONNECT_TIMEOUT},{HTTP_READ_TIMEOUT})")
            with session.get(
                pdf_url,
                headers=headers,
                timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
                stream=True
            ) as response:
                print(f"[INFO] PDF HTTP status (attempt {attempt+1}): {response.status_code}")
                if response.status_code == 200:
                    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                tmp_pdf.write(chunk)
                        tmp_pdf.flush()
                        print(f"[INFO] PDF downloaded successfully (attempt {attempt+1}). Temp file: {tmp_pdf.name}")
                        print("[INFO] Starting PDF text extraction via safe_extract_pdf_text...")
                        text = safe_extract_pdf_text(tmp_pdf.name, timeout=PDF_PARSE_TIMEOUT)
                        print("[INFO] Completed PDF text extraction.")
                    break
                else:
                    print(f"[WARN] HTTP {response.status_code} on attempt {attempt+1}. Retrying...")
        except requests.exceptions.Timeout:
            print(f"[WARN] Download timeout on attempt {attempt+1}. Waiting before retry...")
            time.sleep(5)
        except Exception as e:
            print(f"[WARN] Error on attempt {attempt+1}: {e}")
            time.sleep(5)
    else:
        print("[ERROR] All attempts to fetch PDF failed due to timeout or errors.")
        print("[STEP] parse_pdf_details: end (download failure)")
        return {'date': '', 'time': '', 'dial_in': '', 'registration_link': '', 'host': '', 'contacts': []}

    if not text.strip():
        print("[WARN] PDF appears empty / OCR-only or failed to parse.")
        print("[STEP] parse_pdf_details: end (empty text)")
        return {'date': '', 'time': '', 'dial_in': '', 'registration_link': '', 'host': '', 'contacts': []}

    # --- text parsing ---
    print("[INFO] Starting regex extraction from PDF text...")
    text = re.sub(r'\s+', ' ', text)

    fields = {
        'date': re.search(r'date[:\-\s]*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})', text, re.IGNORECASE),
        'time': re.search(r'(?:at|time)[:\-\s]*([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM|IST)?)', text, re.IGNORECASE),
        'dial_in': re.search(r'(Dial[\s\-]*in[:\-\s]*[^\n]+|Universal Access[:\-\s]*[^\n]+)', text, re.IGNORECASE),
        'registration_link': re.search(r'(https?://[^\s]*diamondpass[^\s]*)', text, re.IGNORECASE),
        'host': re.search(r'(?:Hosted\s*by|Moderator|Organised\s*by)[:\-\s]*([^\n]+)', text, re.IGNORECASE),
    }

    contacts = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', text)
    phones = re.findall(r'\+?\d[\d\s\-\(\)]{7,}\d', text)

    clean = {
        'date': fields['date'].group(1).strip() if fields['date'] else '',
        'time': fields['time'].group(1).strip() if fields['time'] else '',
        'dial_in': fields['dial_in'].group(1).strip() if fields['dial_in'] else '',
        'registration_link': fields['registration_link'].group(1).strip() if fields['registration_link'] else '',
        'host': fields['host'].group(1).strip() if fields['host'] else '',
        'contacts': list(set(contacts + phones))
    }

    print(f"[INFO] PDF details extracted: date='{clean['date']}', time='{clean['time']}'")
    print("[STEP] parse_pdf_details: end")
    return clean


def create_calendar_event(service, calendar_id, company, entry, details, guest_email):
    print("[STEP] create_calendar_event: start")
    print(f"[INFO] Company={company}, entry_title='{entry.title if hasattr(entry,'title') else ''}'")
    print(f"[INFO] guest_email='{guest_email}'")

    try:
        pdf_link = entry.get('link', '')
        dt, tm, dial_in, reg_link, host = (
            details.get('date', ''), details.get('time', ''),
            details.get('dial_in', ''), details.get('registration_link', ''),
            details.get('host', '')
        )
        contacts = ', '.join(details.get('contacts', []))

        summary = f"{company} Analyst/Concall"
        description = (
            f"Announcement link (PDF): {pdf_link}\n"
            f"Date: {dt}\nTime: {tm}\nDial-in info: {dial_in}\n"
            f"Registration link: {reg_link}\nHost: {host}\n"
            f"Contacts: {contacts}\n{EVENT_TAG}"
        )

        print(f"[DEBUG] Event description preview:\n{description}")

        start_dt = datetime.datetime.now()
        try:
            if dt and tm:
                combined = f"{dt.strip()} {tm.strip()} IST"
                print(f"[INFO] Parsing datetime from '{combined}'")
                start_dt = dateparser.parse(combined, dayfirst=False)
        except Exception as e:
            print(f"[WARN] Failed to parse date/time from '{dt} {tm}': {e}")

        end_dt = start_dt + datetime.timedelta(minutes=30)

        attendees = []
        if guest_email:
            attendees.append({'email': guest_email})

        event = {
            'summary': summary,
            'description': description,
            'start': {'dateTime': start_dt.isoformat(), 'timeZone': 'Asia/Kolkata'},
            'end': {'dateTime': end_dt.isoformat(), 'timeZone': 'Asia/Kolkata'},
            'location': 'Virtual',
            'attendees': attendees
        }

        print("[INFO] About to call Google Calendar API events.insert()...")
        service.events().insert(calendarId=calendar_id, body=event).execute()
        print(f"[SUCCESS] Event created: {summary} at {start_dt}")
        print("[STEP] create_calendar_event: end (success)")
    except Exception as e:
        print(f"[ERROR] Creating event failed: {e}")
        print("[STEP] create_calendar_event: end (error)")


def main():
    print("=================================================================")
    print("[START] NSE Concall Automation Script")
    print("=================================================================")
    try:
        print("[STEP] main: init Google Calendar service")
        service = google_calendar_service()

        print("[STEP] main: load companies")
        companies = get_company_names()
        if not companies:
            print("[ERROR] No companies loaded. Exiting.")
            print("[STEP] main: end (no companies)")
            return

        print("[STEP] main: fetch RSS entries")
        entries = fetch_rss_entries()
        if not entries:
            print("[ERROR] No RSS entries fetched. Exiting.")
            print("[STEP] main: end (no RSS)")
            return

        pdfs_processed = 0

        for idx, company in enumerate(companies):
            print("-----------------------------------------------------------------")
            print(f"[LOOP] Company index {idx}, name='{company}'")
            print("-----------------------------------------------------------------")
            print(f"[STEP] main: filter_entries for company '{company}'")
            relevant = filter_entries(entries, [company])

            if not relevant:
                print(f"[NO EVENT] No Analyst/Concall found for: {company}")
                continue

            entry = relevant[0]
            pdf_link = entry.get('link', '')
            print(f"[INFO] Candidate event for {company}: '{entry.title}'")
            print(f"[INFO] RSS link: {pdf_link}")

            if pdfs_processed >= MAX_PDFS_PER_RUN:
                print("[INFO] Max PDF processing limit reached, skipping PDF parse.")
                details = {'date': '', 'time': '', 'dial_in': '', 'registration_link': '', 'host': '', 'contacts': []}
            else:
                print(f"[STEP] main: parse_pdf_details for company '{company}'")
                details = parse_pdf_details(pdf_link)
                pdfs_processed += 1
                print(f"[INFO] pdfs_processed so far: {pdfs_processed}")

            print(f"[STEP] main: create_calendar_event for company '{company}'")
            create_calendar_event(service, CALENDAR_ID, company, entry, details, GUEST_EMAIL)

        print("=================================================================")
        print("[COMPLETE] Script execution finished.")
        print("=================================================================")
    except Exception as e:
        print(f"[FATAL ERROR] Script failed: {e}")
        print("[STEP] main: end (fatal error)")


if __name__ == '__main__':
    main()

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

RSS_URL = 'https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml'
COMPANY_FILE = 'companies.txt'
CALENDAR_ID = 'fcb0ebfa795ba8af091f332acac0c5f0a33c5bd4982ef4db622bb9467188d11c@group.calendar.google.com'
FUZZY_THRESHOLD = 98
EVENT_TAG = "[AUTO:NSE_RSS_SCRIPT]"
GUEST_EMAIL = os.environ.get('GCAL_GUEST_EMAIL', "")
MAX_PDFS_PER_RUN = 10          # safety cap
PDF_PARSE_TIMEOUT = 30         # seconds
HTTP_CONNECT_TIMEOUT = 10
HTTP_READ_TIMEOUT = 20

def normalize(text):
    return re.sub(r'[^a-zA-Z0-9]', '', text or '').lower()

def get_company_names():
    print("[INFO] Reading company names from file...")
    if not os.path.exists(COMPANY_FILE):
        print(f"[ERROR] Company file {COMPANY_FILE} does not exist.")
        return []
    with open(COMPANY_FILE, 'r', encoding='utf-8') as f:
        data = f.read()
        if ',' in data:
            companies = [name.strip() for name in data.split(',') if name.strip()]
        else:
            companies = [line.strip() for line in data.splitlines() if line.strip()]
        print(f"[SUCCESS] Loaded companies: {companies}")
        return companies

def google_calendar_service():
    print("[INFO] Initializing Google Calendar service...")
    try:
        creds = Credentials.from_service_account_file(
            'service-account.json',
            scopes=['https://www.googleapis.com/auth/calendar']
        )
        service = build('calendar', 'v3', credentials=creds)
        print("[SUCCESS] Google Calendar service initialized.")
        return service
    except Exception as e:
        print(f"[ERROR] Failed to initialize Google Calendar service: {e}")
        raise

def fetch_rss_entries():
    print(f"[INFO] Fetching RSS from {RSS_URL} ...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(RSS_URL, headers=headers, timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT))
        if r.status_code != 200:
            print(f"[ERROR] Failed to fetch RSS feed. Status: {r.status_code}")
            return []
        entries = feedparser.parse(r.content).entries
        print(f"[SUCCESS] {len(entries)} entries fetched from RSS.")
        return entries
    except Exception as e:
        print(f"[ERROR] Exception during RSS fetch: {e}")
        return []

def filter_entries(entries, companies):
    print("[INFO] Filtering entries for company and keywords...")
    allowed_keywords = [
        'analyst', 'analysts', 'institutional', 'investor',
        'concall', 'conference call', 'conferencecall',
        'meet', 'call', 'meetconcall', 'meet/concall', 'Trading'
    ]
    allowed_keywords_norm = [normalize(k) for k in allowed_keywords]
    matches = []

    for entry in entries:
        try:
            title = normalize(entry.title if hasattr(entry, 'title') else "")
            summary = normalize(entry.get('summary', ''))
            content = title + " " + summary

            for company in companies:
                score = fuzz.partial_ratio(normalize(company), title)
                key_hit = any(k in content for k in allowed_keywords_norm)
                if score >= FUZZY_THRESHOLD and key_hit:
                    print(f"[MATCH] {company} — '{entry.title}' (Score={score})")
                    matches.append(entry)
                    break
        except Exception as e:
            print(f"[ERROR] While filtering '{getattr(entry, 'title', 'Unknown')}': {e}")

    print(f"[INFO] Filtered to {len(matches)} matches.")
    return matches

# ---------- safe PDF text extraction with timeout ----------

def _extract_pdf_text_worker(path, q):
    try:
        reader = PdfReader(path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        q.put(text)
    except Exception as e:
        q.put(f"__ERROR__{e}")

def safe_extract_pdf_text(path, timeout=PDF_PARSE_TIMEOUT):
    q = Queue()
    p = Process(target=_extract_pdf_text_worker, args=(path, q))
    p.start()
    p.join(timeout)
    if p.is_alive():
        p.terminate()
        p.join()
        print("[WARN] PDF parsing timed out, skipping this PDF.")
        return ""
    if not q.empty():
        out = q.get()
        if isinstance(out, str) and out.startswith("__ERROR__"):
            print(f"[WARN] PDF parse error: {out}")
            return ""
        return out
    return ""

# -----------------------------------------------------------

def parse_pdf_details(pdf_url):
    print(f"[INFO] Preparing to download PDF: {pdf_url}")

    # Guard: only try PDFs
    if not pdf_url.lower().endswith('.pdf'):
        print(f"[WARN] Link does not look like a PDF, skipping PDF parse: {pdf_url}")
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
            print(f"[INFO] PDF download attempt {attempt+1} ...")
            with session.get(
                pdf_url,
                headers=headers,
                timeout=(HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT),
                stream=True
            ) as response:
                if response.status_code == 200:
                    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                tmp_pdf.write(chunk)
                        tmp_pdf.flush()
                        print(f"[INFO] PDF downloaded successfully (attempt {attempt+1}).")
                        print("[INFO] Starting PDF text extraction...")
                        text = safe_extract_pdf_text(tmp_pdf.name, timeout=PDF_PARSE_TIMEOUT)
                        print("[INFO] Finished PDF text extraction.")
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
        return {'date': '', 'time': '', 'dial_in': '', 'registration_link': '', 'host': '', 'contacts': []}

    if not text.strip():
        print("[WARN] PDF appears empty / OCR-only or failed to parse.")
        return {'date': '', 'time': '', 'dial_in': '', 'registration_link': '', 'host': '', 'contacts': []}

    # --- continue with text parsing ---
    text = re.sub(r'\s+', ' ', text)

    fields = {
        # e.g. "October 29, 2025" or "Oct 29, 2025"
        'date': re.search(r'date[:\-\s]*([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})', text, re.IGNORECASE),
        # e.g. "at 4:30 PM IST" or "Time: 4:30 PM"
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

    print(f"[SUCCESS] Extracted PDF details: date='{clean['date']}', time='{clean['time']}'")
    return clean

def create_calendar_event(service, calendar_id, company, entry, details, guest_email):
    print(f"[INFO] Creating calendar event for {company}: {entry.title}")
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

        start_dt = datetime.datetime.now()
        try:
            if dt and tm:
                combined = f"{dt.strip()} {tm.strip()} IST"
                # Let dateutil parse various formats
                start_dt = dateparser.parse(combined, dayfirst=False)
        except Exception as e:
            print(f"[WARN] Failed to parse date/time from '{dt} {tm}': {e}")

        end_dt = start_dt + datetime.timedelta(minutes=30)

        event = {
            'summary': summary,
            'description': description,
            'start': {'dateTime': start_dt.isoformat(), 'timeZone': 'Asia/Kolkata'},
            'end': {'dateTime': end_dt.isoformat(), 'timeZone': 'Asia/Kolkata'},
            'location': 'Virtual',
            'attendees': []  # avoid 403; you can add guest_email if needed
        }

        service.events().insert(calendarId=calendar_id, body=event).execute()
        print(f"[SUCCESS] Event created: {summary} at {start_dt}")
    except Exception as e:
        print(f"[ERROR] Creating event failed: {e}")

def main():
    print("[START] NSE Concall Automation Script")
    try:
        service = google_calendar_service()
        companies = get_company_names()
        if not companies:
            print("[ERROR] No companies loaded. Exiting.")
            return

        print("[INFO] Company list loaded. Proceeding to fetch entries...")
        entries = fetch_rss_entries()
        if not entries:
            print("[ERROR] No RSS entries fetched. Exiting.")
            return

        pdfs_processed = 0

        for company in companies:
            print(f"[INFO] Processing company: {company}")
            relevant = filter_entries(entries, [company])
            if not relevant:
                print(f"[NO EVENT] No Analyst/Concall found for: {company}")
                continue

            entry = relevant[0]
            pdf_link = entry.get('link', '')
            print(f"[INFO] Candidate event: {entry.title} | Link: {pdf_link}")

            if pdfs_processed >= MAX_PDFS_PER_RUN:
                print("[INFO] Max PDF processing limit reached for this run. Creating generic event with no PDF details.")
                details = {'date': '', 'time': '', 'dial_in': '', 'registration_link': '', 'host': '', 'contacts': []}
            else:
                print(f"[INFO] Downloading and parsing PDF for event: {entry.title}")
                details = parse_pdf_details(pdf_link)
                pdfs_processed += 1

            create_calendar_event(service, CALENDAR_ID, company, entry, details, GUEST_EMAIL)

        print("[COMPLETE] Script execution finished.")
    except Exception as e:
        print(f"[FATAL ERROR] Script failed: {e}")

if __name__ == '__main__':
    main()

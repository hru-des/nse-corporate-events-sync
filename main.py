import os
import requests
import feedparser
import datetime
import re
from rapidfuzz import fuzz
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from PyPDF2 import PdfReader
import tempfile

RSS_URL = 'https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml'
COMPANY_FILE = 'companies.txt'
CALENDAR_ID = 'fcb0ebfa795ba8af091f332acac0c5f0a33c5bd4982ef4db622bb9467188d11c@group.calendar.google.com'
FUZZY_THRESHOLD = 90
EVENT_TAG = "[AUTO:NSE_RSS_SCRIPT]"
GUEST_EMAIL = os.environ.get('GCAL_GUEST_EMAIL', "")

# Normalize by removing punctuation/spacing and lowercase
def normalize(text):
    return re.sub(r'[^a-zA-Z0-9]', '', text or '').lower()

# Read companies from a text file, allow comma or lines
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

# Google Calendar API
def google_calendar_service():
    print("[INFO] Initializing Google Calendar service...")
    try:
        creds = Credentials.from_service_account_file('service-account.json', scopes=['https://www.googleapis.com/auth/calendar'])
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
        r = requests.get(RSS_URL, headers=headers)
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
        'concall', 'con. call', 'conferencecall', 'conference call',
        'meet', 'call', 'meetconcall', 'meet/concall'
    ]
    allowed_keywords_norm = [normalize(k) for k in allowed_keywords]
    matches = []
    for entry in entries:
        try:
            orig_title = entry.title if hasattr(entry, 'title') else ""
            orig_summary = entry.get('summary', '')
            title = normalize(orig_title)
            summary = normalize(orig_summary)
            content = title + " " + summary
            for company in companies:
                score = fuzz.partial_ratio(normalize(company), title)
                key_hit = any(k in content for k in allowed_keywords_norm)
                print(f"[DEBUG] Title: '{orig_title}' | Score: {score} | KeyHit: {key_hit} | Content: {content}")
                if score >= FUZZY_THRESHOLD and key_hit:
                    print(f"[MATCH] Company match: {company} - '{orig_title}'")
                    matches.append(entry)
                    break
        except Exception as e:
            print(f"[ERROR] While filtering entry '{getattr(entry, 'title', 'NO_TITLE')}': {e}")
    print(f"[INFO] Filtered to {len(matches)} matches.")
    return matches

def parse_pdf_details(pdf_url):
    print(f"[INFO] Downloading PDF: {pdf_url}")
    try:
        r = requests.get(pdf_url)
        if r.status_code != 200:
            print(f"[ERROR] Could not download PDF (status {r.status_code}).")
            return {}
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_pdf:
            tmp_pdf.write(r.content)
            tmp_pdf.flush()
            reader = PdfReader(tmp_pdf.name)
            text = ""
            for page in reader.pages:
                text += page.extract_text() or ""
        fields = {}
        fields['date'] = re.search(r'Date[:\-\s]*([^\n]+)', text)
        fields['time'] = re.search(r'Time[:\-\s]*([^\n]+)', text)
        fields['dial_in'] = re.search(r'Dial[-\s]*in[:\-\s]*([^\n]+)', text)
        fields['registration_link'] = re.search(r'(Express Join|DiamondPass|Pre[-\s]registration|Registration)[:\-\s]*([^\s\n]+)', text)
        fields['host'] = re.search(r'(Host|Moderator)[:\-\s]*([^\n]+)', text)
        fields['contacts'] = re.findall(r'(Contact|IR)[:\-\s]*([^\n]+)', text)
        clean = {}
        for k, v in fields.items():
            if v is None:
                clean[k] = ""
            elif hasattr(v, 'group') and v.lastindex >= 1:
                clean[k] = v.group(v.lastindex)
            else:
                clean[k] = v
        clean['contacts'] = [c[1] for c in fields['contacts']] if fields.get('contacts') else []
        print(f"[SUCCESS] Extracted PDF details: {clean}")
        return clean
    except Exception as e:
        print(f"[ERROR] PDF extraction {pdf_url}: {e}")
        return {}

def create_calendar_event(service, calendar_id, company, entry, details, guest_email):
    print(f"[INFO] Creating calendar event for {company}: {entry.title}")
    try:
        pdf_link = entry.get('link', '')
        dt = details.get('date', '')
        tm = details.get('time', '')
        dial_in = details.get('dial_in', '')
        reg_link = details.get('registration_link', '')
        host = details.get('host', '')
        contacts = ', '.join(details.get('contacts', []))
        summary = f"{company} Analyst/Concall"
        description = (
            f"Announcement link (PDF): {pdf_link}\n"
            f"Date: {dt}\nTime: {tm}\nDial-in info: {dial_in}\n"
            f"Registration link: {reg_link}\nHost: {host}\nContacts: {contacts}\n{EVENT_TAG}"
        )
        # If unable to parse date/time, just use 'now'
        start_dt = datetime.datetime.now()
        try:
            if dt and tm:
                combined = f"{dt.strip()} {tm.strip()}"
                start_dt = datetime.datetime.strptime(combined, '%d-%b-%Y %I:%M %p')
        except Exception as e:
            print(f"[WARN] Couldn't parse date/time from PDF, using now. Details: {e}")
        end_dt = start_dt + datetime.timedelta(minutes=30)
        event = {
            'summary': summary,
            'description': description,
            'start': {'dateTime': start_dt.isoformat(), 'timeZone': 'Asia/Kolkata'},
            'end': {'dateTime': end_dt.isoformat(), 'timeZone': 'Asia/Kolkata'},
            'location': 'Virtual',
            'attendees': [{'email': guest_email}] if guest_email else []
        }
        service.events().insert(calendarId=calendar_id, body=event).execute()
        print(f"[SUCCESS] Event created: {summary}")
    except Exception as e:
        print(f"[ERROR] Creating event: {e}")

def main():
    print("[START] NSE Concall Automation Script")
    try:
        service = google_calendar_service()
        company_names = get_company_names()
        print("[INFO] Company list loaded. Proceeding to fetch entries...")
        entries = fetch_rss_entries()
        for company in company_names:
            print(f"[INFO] Processing company: {company}")
            relevant_entries = filter_entries(entries, [company])
            if relevant_entries:
                # Create events for ALL matches (not just the first)
                for entry in relevant_entries:
                    print(f"[INFO] Downloading and parsing PDF for event: {entry.title}")
                    details = parse_pdf_details(entry.get('link', ''))
                    create_calendar_event(service, CALENDAR_ID, company, entry, details, GUEST_EMAIL)
            else:
                print(f"[NO EVENT] No Analyst/Concall for: {company}")
        print("[COMPLETE] Script execution finished.")
    except Exception as e:
        print(f"[FATAL ERROR] Script failed: {e}")

if __name__ == '__main__':
    main()

import os
import requests
import feedparser
import datetime
from rapidfuzz import fuzz
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from PyPDF2 import PdfReader
import tempfile

RSS_URL = 'https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml'
HTML_URL = 'https://www.nseindia.com/companies-listing/corporate-filings-announcements'
COMPANY_FILE = 'companies.txt'
CALENDAR_ID = 'fcb0ebfa795ba8af091f332acac0c5f0a33c5bd4982ef4db622bb9467188d11c@group.calendar.google.com'
FUZZY_THRESHOLD = 100
EVENT_TAG = "[AUTO:NSE_RSS_SCRIPT]"
GUEST_EMAIL = os.environ.get('GCAL_GUEST_EMAIL', "")

def google_calendar_service():
    creds = Credentials.from_service_account_file('service-account.json', scopes=['https://www.googleapis.com/auth/calendar'])
    service = build('calendar', 'v3', credentials=creds)
    return service

def get_company_names():
    with open(COMPANY_FILE, 'r', encoding='utf-8') as f:
        data = f.read()
        if ',' in data:
            return [name.strip() for name in data.split(',') if name.strip()]
        return [line.strip() for line in data.splitlines() if line.strip()]

def fetch_rss_entries():
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(RSS_URL, headers=headers)
    return feedparser.parse(r.content).entries

def filter_entries(entries, companies):
    allowed_keywords = ['analysts', 'institutional investor', 'concall']
    matches = []
    for entry in entries:
        title = entry.title.lower()
        summary = entry.get('summary', '').lower()
        content = title + ' ' + summary
        for company in companies:
            score = fuzz.partial_ratio(company.lower(), content)
            if score >= FUZZY_THRESHOLD and any(k in content for k in allowed_keywords):
                # Check for future-dated events, as specified
                dt = entry.get('published_parsed', None)
                # Some feeds may use updated instead of published
                if dt and datetime.datetime(*dt[:6]) > datetime.datetime.now():
                    matches.append(entry)
                break
    # Sort to most recent future-dated
    matches.sort(key=lambda x: x.published_parsed, reverse=True)
    return matches

def parse_pdf_details(pdf_url):
    try:
        r = requests.get(pdf_url)
        if r.status_code != 200:
            return {}
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_pdf:
            tmp_pdf.write(r.content)
            tmp_pdf.flush()
            reader = PdfReader(tmp_pdf.name)
            text = ""
            for page in reader.pages:
                text += page.extract_text() or ""
        fields = {}
        # crude regex field extraction (customise with actual NSE PDF text structure)
        import re
        fields['date'] = re.search(r'Date[:\-\s]*([^\n]+)', text)
        fields['time'] = re.search(r'Time[:\-\s]*([^\n]+)', text)
        fields['dial_in'] = re.search(r'Dial[-\s]*in[:\-\s]*([^\n]+)', text)
        fields['registration_link'] = re.search(r'(Express Join|DiamondPass|Pre[-\s]registration|Registration)[:\-\s]*([^\s\n]+)', text)
        fields['host'] = re.search(r'(Host|Moderator)[:\-\s]*([^\n]+)', text)
        fields['contacts'] = re.findall(r'(Contact|IR)[:\-\s]*([^\n]+)', text)
        # cleanup
        clean = {}
        for k, v in fields.items():
            if v is None:
                clean[k] = ""
            elif hasattr(v, 'group') and v.lastindex >= 1:
                clean[k] = v.group(v.lastindex)
            else:
                clean[k] = v
        clean['contacts'] = [c[1] for c in fields['contacts']] if fields.get('contacts') else []
        return clean
    except Exception as e:
        print(f"[ERROR PDF]: {e}")
        return {}

def create_calendar_event(service, calendar_id, company, entry, details, guest_email):
    pdf_link = entry.get('link', '')
    dt = details.get('date', '')
    tm = details.get('time', '')
    dial_in = details.get('dial_in', '')
    reg_link = details.get('registration_link', '')
    host = details.get('host', '')
    contacts = ', '.join(details.get('contacts', []))
    summary = f"{company} Analyst/Concall"  # customizable
    description = (
        f"Announcement link (PDF): {pdf_link}\n"
        f"Date: {dt}\nTime: {tm}\nDial-in info: {dial_in}\n"
        f"Registration link: {reg_link}\nHost: {host}\nContacts: {contacts}\n{EVENT_TAG}"
    )
    # Set correct datetime logic (fallback if missing: now +30m)
    start_dt = datetime.datetime.now()
    try:
        if dt and tm:
            combined = f"{dt.strip()} {tm.strip()}"
            # Try parse - adapt for NSE format
            start_dt = datetime.datetime.strptime(combined, '%d-%b-%Y %I:%M %p')
    except Exception: pass
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
    print(f"Event created: {summary}")

def main():
    service = google_calendar_service()
    company_names = get_company_names()
    entries = fetch_rss_entries()
    for company in company_names:
        relevant_entries = filter_entries(entries, [company])
        if relevant_entries:
            entry = relevant_entries[0]  # most recent future-dated analyst/concall
            details = parse_pdf_details(entry.get('link', ''))
            create_calendar_event(service, CALENDAR_ID, company, entry, details, GUEST_EMAIL)
        else:
            print(f"[NO EVENT] No future-dated Analyst/Concall for: {company}")

if __name__ == '__main__':
    
    main()

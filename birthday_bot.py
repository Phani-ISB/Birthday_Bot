# birthday_bot.py
"""
Birthday WhatsApp Bot (end-to-end)
- Reads contacts.xlsx (sheet 'contacts')
- Checks birthdays for today (optionally by timezone)
- Uses OpenAI to generate personalized message (or uses template)
- Sends via Twilio WhatsApp (optional: Meta HTTP API alternative)
- Persists sends in sqlite to avoid duplicate sends per year
"""

import os
import sqlite3
import sys
from datetime import datetime, date
from typing import Optional
import pandas as pd
import requests
import time
import logging
import pytz
import random

# Optional LLM client: openai
try:
    import openai
except Exception:
    openai = None

# ---------- CONFIG ----------
# Environment variables (set these in your deployment environment)
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM")  # e.g. "whatsapp:+1415XXXX"
# If using Meta WhatsApp Cloud API instead:
META_WABA_TOKEN = os.getenv("META_WABA_TOKEN")
META_PHONE_NUMBER_ID = os.getenv("META_PHONE_NUMBER_ID")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # or "gpt-5-..."; adjust as available

CONTACTS_FILE = os.getenv("CONTACTS_FILE", "contacts.xlsx")
CONTACTS_SHEET = os.getenv("CONTACTS_SHEET", "contacts")
DB_PATH = os.getenv("DB_PATH", "sent.db")

# Safety / limits
MAX_MESSAGES_PER_RUN = int(os.getenv("MAX_MESSAGES_PER_RUN", "200"))
SEND_DELAY_SECONDS = float(os.getenv("SEND_DELAY_SECONDS", "0.6"))  # gentle pacing

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("birthday-bot")

# ---------- DB helpers ----------
def init_db(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS sent (
        phone TEXT NOT NULL,
        year INTEGER NOT NULL,
        sent_at TEXT NOT NULL,
        message TEXT,
        PRIMARY KEY (phone, year)
    )
    """)
    conn.commit()

def already_sent(conn, phone, year):
    cur = conn.execute("SELECT 1 FROM sent WHERE phone=? AND year=? LIMIT 1", (phone, year))
    return cur.fetchone() is not None

def record_send(conn, phone, year, message):
    conn.execute("INSERT OR REPLACE INTO sent (phone, year, sent_at, message) VALUES (?, ?, ?, ?)",
                 (phone, year, datetime.utcnow().isoformat(), message))
    conn.commit()

# ---------- LLM personalization ----------
def generate_personal_message(name: str, notes: Optional[str]=None, template: Optional[str]=None) -> str:
    """
    If template provided, use with {name} substitution. Otherwise call OpenAI (if configured)
    to generate a short WhatsApp-style birthday message (1-2 lines), include name.
    """
    if template:
        return template.replace("{name}", name)

    # If no OpenAI key or library, fallback to simple templates
    if not OPENAI_API_KEY or openai is None:
        log.info("OpenAI not configured — using builtin template")
        templates = [
            "Happy Birthday, {name}! 🎉 Wishing you a fantastic day filled with joy.",
            "Many happy returns, {name}! Hope you have a wonderful birthday.",
            "Happy Birthday {name}! May your day be full of fun and surprises."
        ]
        t = random.choice(templates)
        if notes:
            t = t + f" PS: {notes}"
        return t.format(name=name)

    # Use OpenAI to craft message
    openai.api_key = OPENAI_API_KEY
    prompt = (
        f"Write a short (1-2 lines), friendly WhatsApp birthday message for {name}."
        + (f" Mention this about them: {notes}." if notes else "")
        + " Keep it casual, emoji-friendly, 40-120 characters, and include the name. No signature."
    )

    try:
        resp = openai.ChatCompletion.create(
            model=OPENAI_MODEL,
            messages=[{"role":"system","content":"You are a helpful assistant that writes friendly WhatsApp messages."},
                      {"role":"user","content":prompt}],
            max_tokens=80,
            temperature=0.8,
        )
        text = resp['choices'][0]['message']['content'].strip()
        return text
    except Exception as e:
        log.exception("OpenAI call failed, using fallback template.")
        templates = [
            "Happy Birthday, {name}! 🎉 Wishing you a fantastic day filled with joy.",
            "Many happy returns, {name}! Hope you have a wonderful birthday."
        ]
        t = random.choice(templates)
        return t.format(name=name)

# ---------- WhatsApp senders ----------
def send_whatsapp_twilio(phone: str, message: str) -> bool:
    """
    Sends via Twilio WhatsApp API.
    phone: recipient e.g. +919876543210
    message: text content
    """
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_WHATSAPP_FROM):
        log.error("Twilio credentials not configured.")
        return False
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json"
    data = {
        "From": TWILIO_WHATSAPP_FROM,  # e.g. "whatsapp:+1415XXXX"
        "To": f"whatsapp:{phone}",
        "Body": message
    }
    resp = requests.post(url, data=data, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))
    if resp.status_code in (200, 201):
        return True
    else:
        log.error("Twilio send failed: %s %s", resp.status_code, resp.text)
        return False

def send_whatsapp_meta(phone: str, message: str) -> bool:
    """
    Sends via Meta (WhatsApp Cloud API). Requires META_WABA_TOKEN and META_PHONE_NUMBER_ID.
    """
    if not (META_WABA_TOKEN and META_PHONE_NUMBER_ID):
        log.error("Meta WABA credentials not configured.")
        return False
    url = f"https://graph.facebook.com/v16.0/{META_PHONE_NUMBER_ID}/messages"
    headers = {"Authorization": f"Bearer {META_WABA_TOKEN}", "Content-Type":"application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "text",
        "text": {"body": message}
    }
    resp = requests.post(url, json=payload, headers=headers)
    if resp.status_code in (200, 201):
        return True
    else:
        log.error("Meta send failed: %s %s", resp.status_code, resp.text)
        return False

def send_whatsapp(phone: str, message: str) -> bool:
    """Choose provider based on configuration."""
    # Prefer Twilio if configured
    if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_WHATSAPP_FROM:
        return send_whatsapp_twilio(phone, message)
    elif META_WABA_TOKEN and META_PHONE_NUMBER_ID:
        return send_whatsapp_meta(phone, message)
    else:
        log.error("No WhatsApp provider configured.")
        return False

# ---------- Date utilities ----------
def parse_birthday(bday_value):
    # Accepts datetime/date strings or pandas Timestamp
    if pd.isna(bday_value):
        return None
    if isinstance(bday_value, (pd.Timestamp, datetime, date)):
        dt = pd.to_datetime(bday_value)
        return dt.date()
    try:
        return pd.to_datetime(str(bday_value)).date()
    except Exception:
        return None

def is_birthday_today(bday_date: date, tz_name: Optional[str]=None):
    # Compare month/day to current date in provided timezone (if any)
    if tz_name:
        try:
            tz = pytz.timezone(tz_name)
        except Exception:
            tz = pytz.utc
    else:
        tz = pytz.utc
    now = datetime.now(tz)
    return (bday_date.month == now.month) and (bday_date.day == now.day)

# ---------- Main run ----------
def main():
    log.info("Starting birthday bot.")
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # Load contacts sheet
    try:
        df = pd.read_excel(CONTACTS_FILE, sheet_name=CONTACTS_SHEET, engine="openpyxl")
    except Exception as e:
        log.exception("Failed to load contacts file: %s", CONTACTS_FILE)
        return

    # Normalize column names
    df = df.rename(columns={c: c.strip().lower() for c in df.columns})
    # Expect columns: name, phone, birthday, timezone, notes, template
    required = ["name","phone","birthday"]
    for r in required:
        if r not in df.columns:
            log.error("Contacts file missing required column: %s", r)
            return

    today_year = datetime.utcnow().year
    to_send = []
    for idx, row in df.iterrows():
        name = str(row.get("name", "")).strip()
        phone = str(row.get("phone", "")).strip()
        bday_raw = row.get("birthday")
        timezone = row.get("timezone", None)
        notes = row.get("notes", None)
        template = row.get("template", None)

        bday = parse_birthday(bday_raw)
        if not bday:
            continue
        try:
            send_flag = is_birthday_today(bday, timezone)  # compare month/day
        except Exception:
            send_flag = is_birthday_today(bday, None)
        if send_flag:
            if already_sent(conn, phone, today_year):
                log.info("Already sent to %s (%s) this year; skipping.", name, phone)
                continue
            to_send.append((name, phone, notes, template))

    log.info("Found %d recipients for today.", len(to_send))
    sent_count = 0
    for (name, phone, notes, template) in to_send[:MAX_MESSAGES_PER_RUN]:
        try:
            msg = generate_personal_message(name=name, notes=notes, template=template)
            success = send_whatsapp(phone=phone, message=msg)
            if success:
                record_send(conn, phone, today_year, msg)
                sent_count += 1
                log.info("Sent to %s (%s).", name, phone)
            else:
                log.error("Failed to send to %s (%s).", name, phone)
        except Exception as e:
            log.exception("Exception sending to %s (%s).", name, phone)
        time.sleep(SEND_DELAY_SECONDS)

    log.info("Run complete. Sent %d messages.", sent_count)
    conn.close()

if __name__ == "__main__":
    main()

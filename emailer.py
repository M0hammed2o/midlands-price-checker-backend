import os
import smtplib
from email.message import EmailMessage
from typing import List


def _split_recipients(value: str) -> List[str]:
    """
    Allows:
      REORDER_TO_EMAIL=a@x.com
    or:
      REORDER_TO_EMAIL=a@x.com,b@y.com;c@z.com
    """
    if not value:
        return []
    raw = value.replace(";", ",")
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def send_reorder_email(subject: str, body: str) -> None:
    """
    Sends email via Gmail SMTP using an App Password.

    Required env:
      - GMAIL_SMTP_USER
      - GMAIL_SMTP_APP_PASSWORD
      - REORDER_TO_EMAIL (single or comma-separated list)
    Optional:
      - REORDER_FROM_NAME (default: "Midlands Price Checker")
    """
    user = (os.getenv("GMAIL_SMTP_USER") or "").strip()
    app_pw = (os.getenv("GMAIL_SMTP_APP_PASSWORD") or "").strip()
    to_raw = (os.getenv("REORDER_TO_EMAIL") or "").strip()
    from_name = (os.getenv("REORDER_FROM_NAME") or "Midlands Price Checker").strip()

    recipients = _split_recipients(to_raw)

    if not user or not app_pw or not recipients:
        missing = []
        if not user:
            missing.append("GMAIL_SMTP_USER")
        if not app_pw:
            missing.append("GMAIL_SMTP_APP_PASSWORD")
        if not recipients:
            missing.append("REORDER_TO_EMAIL")
        raise RuntimeError("Email not configured. Missing: " + ", ".join(missing))

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{user}>"
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(user, app_pw)
        smtp.send_message(msg)
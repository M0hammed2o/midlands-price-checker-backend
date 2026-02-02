import os
import smtplib
from email.message import EmailMessage


def send_reorder_email(*, product_code: str, full_description: str, barcode: str | None, qty: int, unit_price: float | None):
    smtp_user = os.getenv("GMAIL_SMTP_USER", "").strip()
    smtp_pass = os.getenv("GMAIL_SMTP_APP_PASSWORD", "").strip()
    to_raw = os.getenv("REORDER_TO_EMAIL", "").strip()
    from_name = os.getenv("REORDER_FROM_NAME", "Midlands Price Checker").strip()

    if not smtp_user or not smtp_pass:
        raise RuntimeError("Missing GMAIL_SMTP_USER or GMAIL_SMTP_APP_PASSWORD")
    if not to_raw:
        raise RuntimeError("Missing REORDER_TO_EMAIL")

    to_list = [x.strip() for x in to_raw.split(",") if x.strip()]

    subject = f"REORDER: {full_description} ({product_code}) x{qty}"

    body_lines = [
        "Please reorder the item below:",
        "",
        f"Description : {full_description}",
        f"Product Code: {product_code}",
        f"Barcode     : {barcode or '-'}",
        f"Quantity    : {qty}",
    ]
    if unit_price is not None:
        body_lines.append(f"Unit Price  : R {unit_price:.2f}")
    body_lines.append("")
    body_lines.append("Sent from Midlands Price Checker.")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{from_name} <{smtp_user}>"
    msg["To"] = ", ".join(to_list)
    msg.set_content("\n".join(body_lines))

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)
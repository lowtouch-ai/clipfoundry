"""
Email Utilities for Video Companion
Handles all Gmail API interactions and email sending functionality
"""

import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
import json
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def authenticate_gmail(gmail_credentials):
    """
    Authenticate Gmail API.
    
    Args:
        gmail_credentials: Gmail credentials (dict or JSON string)
        
    Returns:
        Gmail service object or None on failure
    """
    try:
        # 1. Determine if credentials are Dict or String
        if isinstance(gmail_credentials, dict):
            creds_info = gmail_credentials
        else:
            # Parse string (e.g. from Airflow Variable) into Dict
            creds_info = json.loads(gmail_credentials)

        # 2. Pass the Dict directly (Do NOT json.load it again)
        creds = Credentials.from_authorized_user_info(creds_info)
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logging.info(f"Authenticated Gmail: {profile.get('emailAddress')}")
        return service
    except Exception as e:
        logging.error(f"Gmail authentication failed: {e}")
        return None


def send_email(service, from_address, recipient_email, subject, html_content, 
               thread_headers=None, attachment_path=None, attachment_name=None):
    """
    Send email with optional attachment.
    
    Args:
        service: Gmail service object
        from_address: Sender email address
        recipient_email: Recipient email address
        subject: Email subject
        html_content: HTML email body
        thread_headers: Optional thread headers for conversation threading
        attachment_path: Optional path to attachment file
        attachment_name: Optional custom attachment filename
        
    Returns:
        True on success, False on failure
    """
    try:
        msg = MIMEMultipart()
        msg["From"] = f"Video Companion <{from_address}>"
        msg["To"] = recipient_email
        msg["Subject"] = subject
        
        if thread_headers:
            original_message_id = thread_headers.get("Message-ID", "")
            if original_message_id:
                msg["In-Reply-To"] = original_message_id
                references = thread_headers.get("References", "")
                msg["References"] = f"{references} {original_message_id}".strip() if references else original_message_id
        
        msg.attach(MIMEText(html_content, "html"))
        
        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={attachment_name or os.path.basename(attachment_path)}")
            msg.attach(part)
            logging.info(f"Attached file: {attachment_name}")
        
        raw = base64.urlsafe_b64encode(msg.as_string().encode()).decode()
        result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
        
        logging.info(f"Email sent successfully: {result.get('id')}")
        return True
    except Exception as e:
        logging.error(f"Failed to send email: {e}", exc_info=True)
        return False


def mark_message_as_read(service, message_id):
    """
    Mark email as read.
    
    Args:
        service: Gmail service object
        message_id: Gmail message ID
        
    Returns:
        True on success, False on failure
    """
    try:
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'removeLabelIds': ['UNREAD']}
        ).execute()
        logging.info(f"Marked message {message_id} as read")
        return True
    except Exception as e:
        logging.error(f"Failed to mark as read: {e}")
        return False

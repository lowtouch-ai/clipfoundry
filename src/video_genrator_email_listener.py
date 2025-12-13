import base64
from email import message_from_bytes
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import json
import time
import logging
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import re
from PIL import Image
import io

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "video_companion_developers",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retry_delay": timedelta(seconds=15),
    "retries": 1
}

VIDEO_COMPANION_FROM_ADDRESS = Variable.get("video.companion.from.address")
GMAIL_CREDENTIALS = Variable.get("video.companion.gmail.credentials")
LAST_PROCESSED_EMAIL_FILE = "/appz/cache/video_companion_last_processed_email.json"
SHARED_IMAGES_DIR = "/appz/shared_images"

def authenticate_gmail():
    """Authenticate Gmail API and verify correct email account."""
    try:
        creds = Credentials.from_authorized_user_info(json.loads(GMAIL_CREDENTIALS))
        service = build("gmail", "v1", credentials=creds)
        profile = service.users().getProfile(userId="me").execute()
        logged_in_email = profile.get("emailAddress", "")
        if logged_in_email.lower() != VIDEO_COMPANION_FROM_ADDRESS.lower():
            raise ValueError(f"Wrong Gmail account! Expected {VIDEO_COMPANION_FROM_ADDRESS}, but got {logged_in_email}")
        logging.info(f"Authenticated Gmail account: {logged_in_email}")
        return service
    except Exception as e:
        logging.error(f"Failed to authenticate Gmail: {str(e)}")
        return None

def get_last_checked_timestamp():
    """Retrieve last processed timestamp in milliseconds."""
    if os.path.exists(LAST_PROCESSED_EMAIL_FILE):
        with open(LAST_PROCESSED_EMAIL_FILE, "r") as f:
            last_checked = json.load(f).get("last_processed", None)
            if last_checked:
                logging.info(f"Retrieved last processed timestamp: {last_checked}")
                return last_checked
    current_timestamp_ms = int(time.time() * 1000)
    update_last_checked_timestamp(current_timestamp_ms)
    return current_timestamp_ms

def update_last_checked_timestamp(timestamp):
    """Update last processed timestamp in milliseconds."""
    os.makedirs(os.path.dirname(LAST_PROCESSED_EMAIL_FILE), exist_ok=True)
    with open(LAST_PROCESSED_EMAIL_FILE, "w") as f:
        json.dump({"last_processed": timestamp}, f)
    logging.info(f"Updated last processed timestamp: {timestamp}")

def decode_email_payload(msg):
    """Decode email payload to extract text content."""
    try:
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type in ["text/plain", "text/html"]:
                    try:
                        return part.get_payload(decode=True).decode()
                    except UnicodeDecodeError:
                        return part.get_payload(decode=True).decode('latin-1')
        else:
            try:
                return msg.get_payload(decode=True).decode()
            except UnicodeDecodeError:
                return msg.get_payload(decode=True).decode('latin-1')
        return ""
    except Exception as e:
        logging.error(f"Error decoding email payload: {e}")
        return ""

def extract_latest_reply(email_content):
    """Extract only the latest reply from email, removing quoted history."""
    if not email_content:
        return ""
    
    separators = [
        '\r\n\r\nOn ',
        '\n\nOn ',
        '\r\n\r\n>',
        '\n\n>',
        '\r\n\r\nFrom:',
        '\n\nFrom:',
        '________________________________',
    ]
    
    latest_content = email_content
    earliest_position = len(email_content)
    
    for separator in separators:
        pos = email_content.find(separator)
        if pos != -1 and pos < earliest_position:
            earliest_position = pos
            latest_content = email_content[:pos]
    
    latest_content = latest_content.strip()
    
    lines = latest_content.split('\n')
    clean_lines = []
    for line in lines:
        stripped = line.lstrip()
        if not stripped.startswith('>'):
            clean_lines.append(line)
        else:
            break
    
    return '\n'.join(clean_lines).strip()

def save_image_attachments(service, message_id, thread_id, parts):
    """Save image attachments to thread-specific directory."""
    try:
        thread_image_dir = os.path.join(SHARED_IMAGES_DIR, thread_id)
        os.makedirs(thread_image_dir, exist_ok=True)
        
        saved_images = []
        image_counter = 1
        
        for part in parts:
            filename = part.get("filename", "")
            mime_type = part.get("mimeType", "")
            
            # Check if it's an image
            if mime_type.startswith("image/") and part.get("body", {}).get("attachmentId"):
                attachment_id = part["body"]["attachmentId"]
                
                try:
                    attachment_data = service.users().messages().attachments().get(
                        userId="me", 
                        messageId=message_id, 
                        id=attachment_id
                    ).execute()
                    
                    file_data = base64.urlsafe_b64decode(attachment_data["data"].encode("UTF-8"))
                    
                    # Determine file extension
                    extension = filename.split('.')[-1] if '.' in filename else 'jpg'
                    if extension.lower() not in ['jpg', 'jpeg', 'png', 'gif', 'bmp']:
                        extension = 'jpg'
                    
                    # Save with standardized naming
                    image_filename = f"input_{image_counter}.{extension}"
                    image_path = os.path.join(thread_image_dir, image_filename)
                    
                    with open(image_path, "wb") as f:
                        f.write(file_data)

                    if os.path.exists(image_path):
                        file_size = os.path.getsize(image_path)
                        logging.info(f"IMAGE SAVED SUCCESSFULLY")
                        logging.info(f"   Thread ID   : {thread_id}")
                        logging.info(f"   Full path   : {image_path}")
                        logging.info(f"   Size        : {file_size:,} bytes")
                    else:
                        logging.error(f"FAILED TO SAVE IMAGE – file not found after write: {image_path}")
                        continue
                    
                    # Validate image
                    try:
                        with Image.open(image_path) as img:
                            width, height = img.size
                            logging.info(f"Saved valid image: {image_path} ({width}x{height})")
                    except Exception as img_error:
                        logging.error(f"Invalid image file: {image_path} - {img_error}")
                        os.remove(image_path)
                        continue
                    
                    saved_images.append({
                        "filename": image_filename,
                        "path": image_path,
                        "mime_type": mime_type,
                        "original_filename": filename
                    })
                    
                    image_counter += 1
                    
                except Exception as attach_error:
                    logging.error(f"Failed to download attachment {attachment_id}: {attach_error}")
                    continue
        
        logging.info(f"Saved {len(saved_images)} images for thread {thread_id}")
        return saved_images
        
    except Exception as e:
        logging.error(f"Error saving image attachments: {e}", exc_info=True)
        return []

def get_email_thread(service, email_data):
    """Retrieve full email thread history."""
    try:
        if not email_data or "headers" not in email_data:
            logging.error("Invalid email_data: 'headers' key missing")
            return []

        thread_id = email_data.get("threadId")
        headers = email_data.get("headers", {})
        
        logging.info(f"Processing email thread: {thread_id}")

        # Extract message IDs from References and In-Reply-To headers
        references = headers.get("References", "")
        in_reply_to = headers.get("In-Reply-To", "")
        current_message_id = headers.get("Message-ID", "")
        
        message_ids = []
        if references:
            message_ids = re.findall(r'<([^>]+)>', references)
        if in_reply_to:
            reply_to_id = re.search(r'<([^>]+)>', in_reply_to)
            if reply_to_id and reply_to_id.group(1) not in message_ids:
                message_ids.append(reply_to_id.group(1))
        
        current_id = re.search(r'<([^>]+)>', current_message_id)
        if current_id:
            message_ids.append(current_id.group(1))
        
        logging.info(f"Found {len(message_ids)} message IDs in thread")
        
        processed_thread = []
        for msg_id in message_ids:
            try:
                search_query = f'rfc822msgid:{msg_id}'
                search_result = service.users().messages().list(
                    userId="me",
                    q=search_query,
                    maxResults=1
                ).execute()
                
                messages = search_result.get("messages", [])
                if not messages:
                    logging.warning(f"Could not find message with ID: {msg_id}")
                    continue
                
                gmail_msg_id = messages[0]["id"]
                raw_message = service.users().messages().get(
                    userId="me",
                    id=gmail_msg_id,
                    format="raw"
                ).execute()
                
                raw_msg = base64.urlsafe_b64decode(raw_message["raw"])
                email_msg = message_from_bytes(raw_msg)
                
                metadata = service.users().messages().get(
                    userId="me",
                    id=gmail_msg_id,
                    format="metadata",
                    metadataHeaders=["From", "Subject", "Date", "Message-ID"]
                ).execute()
                
                msg_headers = {}
                for h in metadata.get("payload", {}).get("headers", []):
                    msg_headers[h["name"]] = h["value"]
                
                content = decode_email_payload(email_msg)
                from_address = msg_headers.get("From", "").lower()
                is_from_bot = VIDEO_COMPANION_FROM_ADDRESS.lower() in from_address
                timestamp = int(metadata.get("internalDate", 0))

                processed_thread.append({
                    "headers": msg_headers,
                    "content": content.strip(),
                    "timestamp": timestamp,
                    "from_bot": is_from_bot,
                    "message_id": gmail_msg_id,
                    "role": "assistant" if is_from_bot else "user"
                })
                
                logging.info(f"Retrieved message {len(processed_thread)}/{len(message_ids)}")
                
            except Exception as e:
                logging.error(f"Error fetching message {msg_id}: {e}")
                continue

        processed_thread.sort(key=lambda x: x.get("timestamp", 0))
        
        logging.info(f"Processed thread {thread_id} with {len(processed_thread)} messages")
        return processed_thread

    except Exception as e:
        logging.error(f"Error retrieving thread: {e}", exc_info=True)
        return []

def format_chat_history(thread_history):
    """Convert thread history to chat history format."""
    chat_history = []
    
    for msg in thread_history[:-1]:
        message = {
            "role": msg["role"],
            "content": msg["content"]
        }
        chat_history.append(message)
    
    logging.info(f"Formatted chat history with {len(chat_history)} messages")
    return chat_history

def fetch_unread_emails(**kwargs):
    """Fetch unread emails and extract full thread history with images."""
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed")
        kwargs['ti'].xcom_push(key="unread_emails", value=[])
        return []
    
    last_checked_timestamp = get_last_checked_timestamp()
    last_checked_seconds = last_checked_timestamp // 1000 if last_checked_timestamp > 1000000000000 else last_checked_timestamp
    
    query = f"is:unread after:{last_checked_seconds}"
    logging.info(f"Fetching emails with query: {query}")
    
    try:
        results = service.users().messages().list(userId="me", labelIds=["INBOX"], q=query).execute()
        messages = results.get("messages", [])
    except Exception as e:
        logging.error(f"Error fetching messages: {e}")
        kwargs['ti'].xcom_push(key="unread_emails", value=[])
        return []
    
    unread_emails = []
    max_timestamp = last_checked_timestamp
    
    logging.info(f"Found {len(messages)} unread messages")
    
    for msg in messages:
        msg_id = msg["id"]
        
        try:
            msg_data = service.users().messages().get(
                userId="me", 
                id=msg_id, 
                format="full"
            ).execute()
            
            headers = {h["name"]: h["value"] for h in msg_data["payload"]["headers"]}
            sender = headers.get("From", "").lower()
            timestamp = int(msg_data["internalDate"])
            thread_id = msg_data.get("threadId", "")
            
            logging.info(f"Processing message: {msg_id}, Thread: {thread_id}")
            
            if timestamp <= last_checked_timestamp:
                logging.info(f"Skipping old message {msg_id}")
                continue
            
            if "no-reply" in sender or "noreply" in sender:
                logging.info(f"Skipping no-reply email: {sender}")
                continue
            
            if sender == VIDEO_COMPANION_FROM_ADDRESS.lower():
                logging.info(f"Skipping email from bot: {sender}")
                continue
            
            # Save image attachments
            saved_images = []
            if "parts" in msg_data["payload"]:
                saved_images = save_image_attachments(
                    service, 
                    msg_id, 
                    thread_id, 
                    msg_data["payload"]["parts"]
                )
            
            email_object = {
                "id": msg_id,
                "threadId": thread_id,
                "headers": headers,
                "content": "",
                "timestamp": timestamp,
                "images": saved_images
            }
            
            thread_history = get_email_thread(service, email_object)
            
            if not thread_history:
                logging.error(f"Failed to retrieve thread history for {msg_id}")
                continue
            
            chat_history = format_chat_history(thread_history)
            latest_message = thread_history[-1]
            latest_message_content = extract_latest_reply(latest_message["content"])
            
            email_object["content"] = latest_message_content
            
            is_reply = len(thread_history) > 1
            subject = headers.get("Subject", "")
            is_reply = is_reply or subject.lower().startswith("re:")
            
            email_object.update({
                "is_reply": is_reply,
                "thread_history": thread_history,
                "chat_history": chat_history,
                "thread_length": len(thread_history)
            })
            
            logging.info(f"Email {msg_id}: is_reply={is_reply}, images={len(saved_images)}")
            
            unread_emails.append(email_object)
            
            if timestamp > max_timestamp:
                max_timestamp = timestamp
                
        except Exception as e:
            logging.error(f"Error processing message {msg_id}: {e}", exc_info=True)
            continue
    
    if messages:
        update_timestamp = max_timestamp + 1
        update_last_checked_timestamp(update_timestamp)
    
    kwargs['ti'].xcom_push(key="unread_emails", value=unread_emails)
    logging.info(f"Processed {len(unread_emails)} emails")
    
    return unread_emails

def branch_function(**kwargs):
    """Decide whether to process emails or skip."""
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")
    
    if unread_emails and len(unread_emails) > 0:
        logging.info(f"Found {len(unread_emails)} unread emails, triggering processor")
        return "trigger_video_processor"
    else:
        logging.info("No unread emails found")
        return "no_email_found_task"

def trigger_video_processor(**kwargs):
    """Trigger video processor DAG for each email."""
    ti = kwargs['ti']
    unread_emails = ti.xcom_pull(task_ids="fetch_unread_emails", key="unread_emails")
    
    if not unread_emails:
        logging.info("No unread emails to process")
        return
    
    for email in unread_emails:
        task_id = f"trigger_processor_{email['id'].replace('-', '_')}"
        
        trigger_conf = {
            "email_data": email,
            "chat_history": email.get("chat_history", []),
            "thread_history": email.get("thread_history", []),
            "thread_id": email.get("threadId", ""),
            "message_id": email.get("id", ""),
            "images": email.get("images", [])
        }
        
        logging.info(f"Triggering video processor for email {email['id']}")
        
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="script_builder",
            conf=trigger_conf,
        )
        trigger_task.execute(context=kwargs)
    
    logging.info(f"Triggered processor for {len(unread_emails)} emails")

def no_email_found(**kwargs):
    """Log when no emails are found."""
    logging.info("No new emails to process")

readme_content = """
# Video Companion Monitor Mailbox DAG

This DAG monitors the Gmail inbox for video generation requests.

## Features
- Monitors inbox for unread emails
- Extracts thread history and conversation context
- Saves image attachments to thread-specific directories
- Triggers video processor DAG for each email

## Schedule
Runs every 1 minute
"""

with DAG(
    "video_companion_monitor_mailbox",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    doc_md=readme_content,
    tags=["video", "companion", "monitor", "email"]
) as dag:

    fetch_emails_task = PythonOperator(
        task_id="fetch_unread_emails",
        python_callable=fetch_unread_emails,
        provide_context=True
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_function,
        provide_context=True
    )

    trigger_processor_task = PythonOperator(
        task_id="trigger_video_processor",
        python_callable=trigger_video_processor,
        provide_context=True
    )

    no_email_found_task = PythonOperator(
        task_id="no_email_found_task",
        python_callable=no_email_found,
        provide_context=True
    )

    fetch_emails_task >> branch_task >> [trigger_processor_task, no_email_found_task]
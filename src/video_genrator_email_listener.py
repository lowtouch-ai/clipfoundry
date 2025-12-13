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
import shutil

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

# Workspace configuration - similar to Cucumber approach
VIDEO_WORKSPACES_BASE = "/appz/video_companion/workspaces"
VIDEO_TEMPLATE_DIR = "/appz/video_companion/template"

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

def create_workspace_for_thread(thread_id, email_id):
    """
    Create a new workspace or return existing workspace for a thread.
    Uses thread_id as the workspace directory name - no mapping file needed!
    Based on Cucumber's workspace approach.
    """
    try:
        # Use thread_id directly as the workspace identifier
        workspace_path = f'{VIDEO_WORKSPACES_BASE}/{thread_id}'
        
        # Check if workspace already exists for this thread
        if os.path.exists(workspace_path):
            logging.info(f"Reusing existing workspace for thread {thread_id} at {workspace_path}")
            return {
                "workspace_id": thread_id,  # thread_id IS the workspace_id
                "workspace_path": workspace_path,
                "is_existing": True
            }
        
        # Create new workspace with thread_id as directory name
        os.makedirs(VIDEO_WORKSPACES_BASE, exist_ok=True)
        
        logging.info(f"Creating new workspace for thread: {thread_id}, email: {email_id}")
        logging.info(f"Workspace path: {workspace_path}")
        
        # If you have a template directory, copy it; otherwise just create the directory
        if os.path.exists(VIDEO_TEMPLATE_DIR):
            logging.info(f"Copying from template {VIDEO_TEMPLATE_DIR} to {workspace_path}")
            shutil.copytree(
                VIDEO_TEMPLATE_DIR,
                workspace_path,
                dirs_exist_ok=True,
                ignore=shutil.ignore_patterns(
                    '*.tmp',
                    '.DS_Store',
                    '__pycache__',
                    '*.pyc'
                )
            )
        else:
            # Just create the directory structure
            os.makedirs(workspace_path, exist_ok=True)
            os.makedirs(f"{workspace_path}/images", exist_ok=True)
            os.makedirs(f"{workspace_path}/videos", exist_ok=True)
            logging.info(f"Created new workspace structure at {workspace_path}")
        
        logging.info(f"New workspace created successfully for thread: {thread_id}")
        
        return {
            "workspace_id": thread_id,  # thread_id IS the workspace_id
            "workspace_path": workspace_path,
            "is_existing": False
        }
        
    except Exception as e:
        error_msg = f"Failed to create/get workspace for thread {thread_id}, email {email_id}: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

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


def save_current_message_images(service, message_id, workspace_path, parts):
    """
    Save ONLY the images from the CURRENT message to the workspace.
    Uses workspace images directory instead of shared directory.
    
    Args:
        service: Gmail API service
        message_id: Current message ID
        workspace_path: Workspace directory path
        parts: Message parts containing attachments
    
    Returns:
        List of newly saved image info dicts
    """
    try:
        # Create workspace images directory
        workspace_image_dir = os.path.join(workspace_path, "images")
        os.makedirs(workspace_image_dir, exist_ok=True)
        
        logging.info(f"Saving images to workspace: {workspace_path}")
        logging.info(f"Workspace image directory: {workspace_image_dir}")
        
        # Determine next available counter by checking existing files
        existing_files = []
        if os.path.exists(workspace_image_dir):
            existing_files = [f for f in os.listdir(workspace_image_dir) 
                            if f.startswith("input_") and '.' in f]
        
        image_counter = 1
        if existing_files:
            for filename in existing_files:
                match = re.match(r'input_(\d+)\.', filename)
                if match:
                    num = int(match.group(1))
                    if num >= image_counter:
                        image_counter = num + 1
        
        logging.info(f"Starting image counter at {image_counter}")
        
        saved_images = []
        
        if not parts:
            logging.info(f"No parts found in message {message_id}")
            return []
        
        def process_part(part):
            """Recursively process message parts to find and save images"""
            nonlocal image_counter
            
            filename = part.get("filename", "")
            mime_type = part.get("mimeType", "")
            
            logging.debug(f"Processing part: filename={filename}, mime_type={mime_type}")
            
            # Check if it's an image attachment
            if mime_type.startswith("image/"):
                attachment_id = part.get("body", {}).get("attachmentId")
                
                if attachment_id:
                    try:
                        logging.info(f"Downloading attachment: {attachment_id}")
                        
                        # Download attachment from Gmail
                        attachment_data = service.users().messages().attachments().get(
                            userId="me", 
                            messageId=message_id, 
                            id=attachment_id
                        ).execute()
                        
                        # Decode base64 data
                        file_data = base64.urlsafe_b64decode(attachment_data["data"].encode("UTF-8"))
                        
                        # Determine file extension
                        if filename and '.' in filename:
                            extension = filename.split('.')[-1].lower()
                        else:
                            mime_to_ext = {
                                'image/jpeg': 'jpg',
                                'image/jpg': 'jpg',
                                'image/png': 'png',
                                'image/gif': 'gif',
                                'image/bmp': 'bmp',
                                'image/webp': 'webp'
                            }
                            extension = mime_to_ext.get(mime_type, 'jpg')
                        
                        # Validate extension
                        valid_extensions = ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp']
                        if extension not in valid_extensions:
                            extension = 'jpg'
                        
                        # Create standardized filename
                        image_filename = f"input_{image_counter}.{extension}"
                        image_path = os.path.join(workspace_image_dir, image_filename)
                        
                        logging.info(f"Saving image to: {image_path}")
                        
                        # Write file to disk
                        with open(image_path, "wb") as f:
                            f.write(file_data)
                        
                        # Verify file was saved
                        if not os.path.exists(image_path):
                            logging.error(f"FAILED: File not found after write: {image_path}")
                            return None
                        
                        file_size = os.path.getsize(image_path)
                        
                        # Validate image file
                        try:
                            with Image.open(image_path) as img:
                                width, height = img.size
                                img_format = img.format
                                logging.info(f"✓ NEW IMAGE SAVED: {image_filename} ({width}x{height}, {file_size:,} bytes)")
                        except Exception as img_error:
                            logging.error(f"Invalid image file: {image_path} - {img_error}")
                            os.remove(image_path)
                            return None
                        
                        # Create image info object
                        image_info = {
                            "filename": image_filename,
                            "path": image_path,
                            "mime_type": mime_type,
                            "original_filename": filename or f"attachment_{image_counter}.{extension}",
                            "size": file_size,
                            "dimensions": f"{width}x{height}"
                        }
                        
                        image_counter += 1
                        return image_info
                        
                    except Exception as attach_error:
                        logging.error(f"Failed to download/save attachment {attachment_id}: {attach_error}")
                        import traceback
                        logging.error(traceback.format_exc())
                        return None
                else:
                    # Image data might be inline (embedded)
                    inline_data = part.get("body", {}).get("data")
                    if inline_data:
                        try:
                            file_data = base64.urlsafe_b64decode(inline_data.encode("UTF-8"))
                            
                            # Determine extension
                            extension = 'jpg'
                            if filename and '.' in filename:
                                extension = filename.split('.')[-1].lower()
                            
                            image_filename = f"input_{image_counter}.{extension}"
                            image_path = os.path.join(workspace_image_dir, image_filename)
                            
                            with open(image_path, "wb") as f:
                                f.write(file_data)
                            
                            file_size = os.path.getsize(image_path)
                            logging.info(f"✓ Saved inline image: {image_path} ({file_size} bytes)")
                            
                            image_info = {
                                "filename": image_filename,
                                "path": image_path,
                                "mime_type": mime_type,
                                "original_filename": filename or f"inline_{image_counter}.{extension}",
                                "size": file_size
                            }
                            
                            image_counter += 1
                            return image_info
                            
                        except Exception as inline_error:
                            logging.error(f"Failed to save inline image: {inline_error}")
                            return None
            
            # Recursively process nested parts (multipart messages)
            if "parts" in part:
                for nested_part in part["parts"]:
                    result = process_part(nested_part)
                    if result:
                        saved_images.append(result)
            
            return None
        
        # Process all message parts
        for part in parts:
            result = process_part(part)
            if result:
                saved_images.append(result)
        
        logging.info(f"Saved {len(saved_images)} new images for message {message_id}")
        return saved_images
        
    except Exception as e:
        logging.error(f"Error in save_current_message_images: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return []


def load_all_workspace_images(workspace_path):
    """
    Load ALL images from the workspace images directory.
    
    Args:
        workspace_path: Workspace directory path
    
    Returns:
        List of all image info dicts found in the workspace
    """
    try:
        workspace_image_dir = os.path.join(workspace_path, "images")
        
        if not os.path.exists(workspace_image_dir):
            logging.info(f"No image directory found in workspace {workspace_path}")
            return []
        
        all_images = []
        
        # Get all files in directory
        files = os.listdir(workspace_image_dir)
        image_files = [f for f in files if f.startswith("input_") and 
                      f.split('.')[-1].lower() in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp']]
        
        logging.info(f"Found {len(image_files)} image files in {workspace_image_dir}")
        
        for filename in sorted(image_files):
            image_path = os.path.join(workspace_image_dir, filename)
            
            # Validate image exists and is readable
            if not os.path.exists(image_path):
                logging.warning(f"Image file missing: {image_path}")
                continue
            
            try:
                # Validate it's a real image
                with Image.open(image_path) as img:
                    width, height = img.size
                    img_format = img.format
                
                # Get file size
                file_size = os.path.getsize(image_path)
                
                # Determine mime type from extension
                ext = filename.split('.')[-1].lower()
                mime_map = {
                    'jpg': 'image/jpeg', 
                    'jpeg': 'image/jpeg',
                    'png': 'image/png', 
                    'gif': 'image/gif',
                    'bmp': 'image/bmp', 
                    'webp': 'image/webp'
                }
                
                image_info = {
                    "filename": filename,
                    "path": image_path,
                    "mime_type": mime_map.get(ext, 'image/jpeg'),
                    "original_filename": filename,
                    "size": file_size,
                    "dimensions": f"{width}x{height}",
                    "format": img_format
                }
                
                all_images.append(image_info)
                logging.debug(f"Loaded image: {filename} ({width}x{height}, {file_size} bytes)")
                
            except Exception as e:
                logging.warning(f"Skipping invalid image file {filename}: {e}")
                continue
        
        logging.info(f"✓ Loaded {len(all_images)} total images from workspace")
        return all_images
        
    except Exception as e:
        logging.error(f"Error loading workspace images: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return []


def get_email_thread(service, thread_id):
    """
    Retrieve full email thread messages using Gmail API.
    This is now the SINGLE SOURCE OF TRUTH for thread content and length.
    """
    try:
        logging.info(f"Fetching full thread {thread_id} using threads.get()")

        thread = service.users().threads().get(
            userId="me",
            id=thread_id,
            format="full"
        ).execute()

        thread_msgs = thread.get("messages", [])
        logging.info(f"✓ threads.get() returned {len(thread_msgs)} messages for thread {thread_id}")

        if not thread_msgs:
            logging.warning(f"Empty thread {thread_id}")
            return []

        processed_thread = []

        for gmail_msg in thread_msgs:
            try:
                # === CORRECT WAY: Fetch raw and parse properly ===
                raw_message = service.users().messages().get(
                    userId="me",
                    id=gmail_msg["id"],
                    format="raw"
                ).execute()

                raw_bytes = base64.urlsafe_b64decode(raw_message["raw"])
                email_msg = message_from_bytes(raw_bytes)

                # Now safely use decode_email_payload
                content = decode_email_payload(email_msg)

                headers = {}
                for h in gmail_msg["payload"].get("headers", []):
                    headers[h["name"]] = h["value"]

                from_address = headers.get("From", "").lower()
                is_from_bot = VIDEO_COMPANION_FROM_ADDRESS.lower() in from_address
                timestamp = int(gmail_msg.get("internalDate", 0))
                message_id = gmail_msg["id"]

                processed_thread.append({
                    "headers": headers,
                    "content": content.strip(),
                    "timestamp": timestamp,
                    "from_bot": is_from_bot,
                    "message_id": message_id,
                    "role": "assistant" if is_from_bot else "user"
                })

            except Exception as msg_error:
                logging.error(f"Error processing message {gmail_msg.get('id')} in thread {thread_id}: {msg_error}", exc_info=True)
                continue

        processed_thread.sort(key=lambda x: x["timestamp"])

        logging.info(f"✓ Successfully processed thread {thread_id} with {len(processed_thread)} messages")
        for idx, msg in enumerate(processed_thread, 1):
            preview = msg["content"][:60].replace("\n", " ")
            logging.info(f"   [{idx}] {msg['role']}: {preview}...")

        return processed_thread

    except Exception as e:
        logging.error(f"Failed to retrieve thread {thread_id}: {e}", exc_info=True)
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
    service = authenticate_gmail()
    if not service:
        logging.error("Gmail authentication failed")
        kwargs['ti'].xcom_push(key="unread_emails", value=[])
        return []

    last_checked_timestamp = get_last_checked_timestamp()
    last_checked_seconds = last_checked_timestamp // 1000
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
            msg_data = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
            headers = {h["name"]: h["value"] for h in msg_data["payload"]["headers"]}
            sender = headers.get("From", "").lower()
            timestamp = int(msg_data["internalDate"])
            thread_id = msg_data.get("threadId", "")

            if not thread_id:
                logging.warning(f"Message {msg_id} has no threadId, skipping")
                continue

            if "no-reply" in sender or "noreply" in sender:
                logging.info(f"Skipping no-reply email: {sender}")
                continue

            if sender == VIDEO_COMPANION_FROM_ADDRESS.lower():
                logging.info(f"Skipping own email: {sender}")
                continue

            # === LYNX-STYLE: Use thread_id directly ===
            logging.info(f"Using Gmail thread_id {thread_id} as workspace identifier")

            workspace_info = create_workspace_for_thread(thread_id, msg_id)
            workspace_id = workspace_info["workspace_id"]
            workspace_path = workspace_info["workspace_path"]
            is_existing_workspace = workspace_info.get("is_existing", False)

            # Get full thread history
            thread_history = get_email_thread(service, thread_id)
            if not thread_history:
                logging.error(f"Failed to get thread history for {thread_id}")
                continue

            actual_thread_length = len(thread_history)
            is_reply = actual_thread_length > 1

            # Save new images
            payload = msg_data.get("payload", {})
            parts = payload.get("parts", [])
            if payload.get("mimeType", "").startswith("image/"):
                parts = [payload]

            new_images = save_current_message_images(service, msg_id, workspace_path, parts)
            all_workspace_images = load_all_workspace_images(workspace_path)

            latest_message = thread_history[-1]
            latest_content = extract_latest_reply(latest_message["content"])

            email_object = {
                "id": msg_id,
                "threadId": thread_id,
                "headers": headers,
                "content": latest_content,
                "timestamp": timestamp,
                "is_reply": True,
                "thread_history": thread_history,
                "chat_history": format_chat_history(thread_history),
                "thread_length": actual_thread_length,
                "images": all_workspace_images,
                "new_images_count": len(new_images),
                "total_images_count": len(all_workspace_images),
                "workspace_id": workspace_id,
                "workspace_path": workspace_path,
                "is_existing_workspace": is_existing_workspace
            }

            logging.info(f"Processed email {msg_id} | Thread {thread_id} | Images: +{len(new_images)} (total {len(all_workspace_images)}) | Reply: {is_reply}")

            unread_emails.append(email_object)
            try:
                service.users().messages().modify(
                    userId="me",
                    id=msg_id,
                    body={"removeLabelIds": ["UNREAD"]}
                ).execute()
                logging.info(f"Marked message {msg_id} as READ")
            except Exception as e:
                logging.warning(f"Failed to mark message {msg_id} as READ: {e}")

            # Update the latest timestamp seen
            if timestamp > max_timestamp:
                max_timestamp = timestamp

        except Exception as e:
            logging.error(f"Error processing message {msg_id}: {e}", exc_info=True)
            continue

    if messages:
        update_last_checked_timestamp(max_timestamp + 1)

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
    """Trigger video processor DAG for each email with workspace info."""
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
            "images": email.get("images", []),
            "workspace_id": email.get("workspace_id"),
            "workspace_path": email.get("workspace_path"),
            "is_existing_workspace": email.get("is_existing_workspace", False)
        }
        
        logging.info(f"Triggering video processor for email {email['id']}, workspace {email.get('workspace_id')} (reused: {email.get('is_existing_workspace', False)})")
        
        trigger_task = TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="video_companion_processor",
            conf=trigger_conf,
        )
        trigger_task.execute(context=kwargs)
    
    logging.info(f"Triggered processor for {len(unread_emails)} emails")


def no_email_found(**kwargs):
    """Log when no emails are found."""
    logging.info("No new emails to process")


readme_content = """
# Video Companion Monitor Mailbox DAG - Simplified

This DAG monitors the Gmail inbox for video generation requests.

## Simplified Image Handling:
1. Fetch messages from Gmail API (thread history)
2. Save NEW images from current message to thread directory
3. Load ALL images from thread directory path

## Key Functions:
- `save_current_message_images()`: Saves only NEW images from current message
- `load_all_thread_images()`: Loads ALL images from thread directory
- `get_email_thread()`: Fetches message history (no image handling)

## Schedule:
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
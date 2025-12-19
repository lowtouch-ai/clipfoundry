from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google import genai
from google.genai import types
import base64
from email.mime.text import MIMEText
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json
import logging
from datetime import datetime

# --- CONFIGURATION ---
default_args = {
    'owner': 'clipfoundry.ai',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# --- CONSTANTS ---
AVG_WPM = 140  # Words Per Minute speaking pace

def send_gmail_via_api(to_email, subject, html_content):
    """
    Sends an email using the Gmail API and OAuth2 credentials stored in Airflow Variables.
    """
    try:
        # 1. Load Credentials from Airflow Variable
        creds_info = Variable.get("video.companion.gmail.credentials", deserialize_json=True)
        sender_email = Variable.get("video.companion.from.address")

        if not creds_info or not sender_email:
            logging.error("Missing Airflow Variables: video.companion.gmail.credentials or video.companion.from.address")
            return False
        
        # 2. Reconstruct Credentials Object
        creds = Credentials(
            token=creds_info.get("access_token"),
            refresh_token=creds_info.get("refresh_token"),
            token_uri=creds_info.get("token_uri"),
            client_id=creds_info.get("client_id"),
            client_secret=creds_info.get("client_secret"),
            scopes=creds_info.get("scopes")
        )

        # 3. Build Gmail Service
        service = build('gmail', 'v1', credentials=creds)

        # 4. Create Email Message
        message = MIMEText(html_content, 'html')
        message['to'] = to_email
        message['from'] = sender_email
        message['subject'] = subject

        # 5. Encode and Send
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        body = {'raw': raw_message}
        
        sent_message = service.users().messages().send(userId="me", body=body).execute()
        logging.info(f"Email sent successfully. Message Id: {sent_message['id']}")
        return True

    except Exception as e:
        logging.error(f"Failed to send Gmail via API: {e}")
        return False

# --- TASK 1: FULL SCRIPT GENERATION ---
def generate_full_script(**kwargs):
    """
    Analyzes intent and generates a COMPLETE script for a specific duration.
    No segmentation.
    """
    ti = kwargs['ti']
    dag_run_conf = kwargs['dag_run'].conf or {}
    
    chat_inputs = dag_run_conf.get('chat_inputs', {})
    user_message = chat_inputs.get('message', "Explain how AI works.")
    image_path = chat_inputs.get('args', {}).get('image_path', None)
    
    logging.info(f"Instruction: {user_message}")
    
    # Initialize Gemini
    api_key = Variable.get("GOOGLE_API_KEY", default_var=None)
    client = genai.Client(api_key=api_key)

    # --- SYSTEM PROMPT: DURATION FOCUSED ---
    system_instruction = f"""
    You are 'ClipFoundry', an expert Video Scriptwriter.
    
    GOAL: Write a compelling, single-flow video script based on the user's request.
    
    INSTRUCTIONS:
    1. ANALYZE the user request to determine the Target Duration (in seconds).
       - If explicitly stated (e.g., "30s video"), use that.
       - If NOT stated, infer a standard duration (e.g., 30s for a Reel, 60s for an explainer).
    2. WRITE the script to fit that duration at a speaking pace of {AVG_WPM} words per minute.
    3. Do NOT split into segments. Provide the script as one cohesive block of text.
    4. Provide brief visual direction for the whole video (overall vibe/setting).
    
    Output strictly valid JSON:
    {{
        "video_meta": {{
            "title": "string",
            "target_duration": int,
            "video_type": "string"
        }},
        "script_content": "Full script text here...",
        "visual_direction": "Overall visual style description..."
    }}
    """
    
    prompt_content = f"User Request: {user_message}. Context Image: {image_path}"

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                response_mime_type="application/json"
            ),
            contents=prompt_content
        )
        result = json.loads(response.text)
        result['status'] = 'draft'
        
    except Exception as e:
        logging.error(f"Gemini API failed: {e}")
        # Fallback
        result = {
            "status": "error",
            "video_meta": {"title": "Error Fallback", "target_duration": 15, "video_type": "Error"},
            "script_content": "We encountered an error generating your script. Please try again.",
            "visual_direction": "Static error screen."
        }

    ti.xcom_push(key='draft_script', value=result)


# --- TASK 2: DURATION VALIDATION & REPAIR ---
def validate_script_duration(**kwargs):
    """
    Validates the TOTAL script duration. 
    If it exceeds the target, it rewrites the script to fit.
    """
    ti = kwargs['ti']
    
    # Pull data
    data = ti.xcom_pull(task_ids='generate_full_script', key='draft_script')
    
    if data.get('status') == 'error':
        ti.xcom_push(key='validated_script', value=data)
        return

    script_text = data.get('script_content', '')
    target_duration = data['video_meta'].get('target_duration', 30)
    
    # 1. Calculate Estimated Duration
    word_count = len(script_text.split())
    est_seconds = (word_count / AVG_WPM) * 60
    
    logging.info(f"Target: {target_duration}s | Actual: {est_seconds:.1f}s")

    # 2. Validation Logic (Tolerance +15%)
    limit = target_duration * 1.15
    
    if est_seconds > limit:
        logging.warning(f"Script too long ({est_seconds:.1f}s). rewriting...")
        
        api_key = Variable.get("GOOGLE_API_KEY", default_var=None)
        client = None
        if api_key:
            try:
                client = genai.Client(api_key=api_key)
                
                repair_prompt = (
                    f"The following video script is too long ({int(est_seconds)}s). "
                    f"The target is exactly {target_duration}s.\n"
                    f"Rewrite the script to be tighter and more impactful.\n"
                    f"CRITICAL: Return ONLY the raw script text. "
                    f"Do NOT include explanations, 'Here is the script', or markdown formatting.\n\n"
                    f"ORIGINAL SCRIPT:\n{script_text}"
                )
                
                resp = client.models.generate_content(
                    model="gemini-2.5-flash",
                    config=types.GenerateContentConfig(response_mime_type="text/plain"),
                    contents=repair_prompt
                )
                
                # Clean any accidental leading/trailing quotes or whitespace
                clean_new_script = resp.text.strip().strip('"')
                
                # Update the data
                data['script_content'] = clean_new_script
                
                # Recalculate duration for correct metadata
                new_word_count = len(clean_new_script.split())
                est_seconds = (new_word_count / AVG_WPM) * 60
                logging.info(f"Repaired Duration: {est_seconds:.1f}s")
                
            except Exception as e:
                logging.error(f"Repair failed: {e}")

    # Update metadata
    data['video_meta']['actual_duration'] = est_seconds
    data['video_meta']['word_count'] = len(data['script_content'].split())
    
    ti.xcom_push(key='validated_script', value=data)


# --- TASK 3: UNIFIED RESPONSE FORMAT ---
def request_user_approval(**kwargs):
    ti = kwargs['ti']
    dag_run_conf = kwargs['dag_run'].conf or {}
    
    # 1. Pull Validated Data
    data = ti.xcom_pull(task_ids='validate_script_duration', key='validated_script')
    
    if not data:
        return {"status": "error", "message": "No data received."}

    # Extract Data
    meta = data.get('video_meta', {})
    title = meta.get('title', 'Untitled')
    target = meta.get('target_duration', 0)
    actual = round(meta.get('actual_duration', 0), 1)
    script = data.get('script_content', '').replace("\n", "<br>") # Handle line breaks for HTML
    visuals = data.get('visual_direction', '')

    # ---------------------------------------------------------
    # VERSION 1: MARKDOWN (For Agent / Open WebUI)
    # ---------------------------------------------------------
    agent_markdown = f"""### 🎬 Script: {title}
**Target:** {target}s | **Estimated:** {actual}s | **Type:** {meta.get('video_type', 'Video')}

---
**📝 Script:**
> {data.get('script_content', '')}

**🎨 Visual Direction:**
_{visuals}_
"""

    # ---------------------------------------------------------
    # VERSION 2: HTML (For Email Clients)
    # ---------------------------------------------------------
    email_html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; line-height: 1.6; color: #333;">
        <h2 style="color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 10px;">🎬 Script: {title}</h2>
        
        <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <p style="margin: 0;">
                <b>Target:</b> {target}s &nbsp;|&nbsp; 
                <b>Estimated:</b> {actual}s &nbsp;|&nbsp; 
                <b>Type:</b> {meta.get('video_type', 'Video')}
            </p>
        </div>

        <h3 style="color: #2980b9; margin-bottom: 10px;">📝 Script</h3>
        <blockquote style="background: #fff; border-left: 5px solid #2980b9; margin: 0; padding: 15px; font-style: italic; color: #555; font-size: 16px;">
            {script}
        </blockquote>

        <h3 style="color: #27ae60; margin-top: 25px; margin-bottom: 10px;">🎨 Visual Direction</h3>
        <div style="background: #eafaf1; padding: 15px; border-radius: 5px; border: 1px solid #d5f5e3;">
            {visuals}
        </div>
        
        <p style="margin-top: 30px; font-size: 12px; color: #999; text-align: center;">
            Generated by ClipFoundry AI
        </p>
    </div>
    """

    # --- ROUTING LOGIC ---

    # A. Email Mode
    if "email_data" in dag_run_conf:
        logging.info("Trigger Source: EMAIL.")
        email_payload = dag_run_conf.get("email_data", {})
        user_email = email_payload.get("sender") or dag_run_conf.get('agent_headers', {}).get('X-LTAI-User')

        if user_email and "@" in user_email:
            # CALL CUSTOM GMAIL SENDER
            success = send_gmail_via_api(
                to_email=user_email,
                subject=f"Script Ready: {title}",
                html_content=email_html
            )
            
            if success:
                return {"status": "email_sent", "recipient": user_email}
            else:
                return {"status": "email_failed", "error": "Check logs for API error"}
        else:
            return {"status": "skipped", "reason": "no_email_found"}
            
    # B. API / Agent Mode
    else:
        logging.info("Trigger Source: API. Returning Markdown.")
        
        return {
            "status": "success",
            "markdown_output": agent_markdown,  # <--- Returning the Markdown version
            "raw_data": data 
        }


# --- DAG DEFINITION ---
with DAG(
    dag_id="script_generation_v1",
    default_args=default_args,
    description="script_generation:v0.1",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["agent", "script-only", "conversational"],
) as dag:

    step_1 = PythonOperator(
        task_id="generate_full_script",
        python_callable=generate_full_script,
        provide_context=True,
    )

    step_2 = PythonOperator(
        task_id="validate_script_duration",
        python_callable=validate_script_duration,
        provide_context=True,
    )
    
    step_3 = PythonOperator(
        task_id="request_user_approval",
        python_callable=request_user_approval,
        provide_context=True,
    )

    step_1 >> step_2 >> step_3
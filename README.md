# 🎬 ClipFoundry.ai

**The Autonomous Video Production Agent for Creative Studios.**

ClipFoundry.ai is an open-source, no-code video generation agent designed to act as your digital video producer. Whether you need to turn a quick idea into a polished reel or generate assets instantly from images, ClipFoundry handles the research, scripting, and video synthesis so you can focus on the creative vision.

Licensed under **GNU GPLv3**.

---

## 🚀 Two Ways to CreateClipFoundry offers two distinct workflows: a conversational "producer" mode via email, and a "fire-and-go" studio mode via chat.

### 1. The Conversational Producer (Email) 📧*Best for: Refining ideas, script iteration, and hands-off research.*

This workflow allows you to interact with ClipFoundry just like you would with a human video producer. You provide a simple idea, and the agent handles the research, persona creation, and script drafting before production begins.

#### How it Works:
**Step 1: Send a Minimal Prompt**
Send an email to the ClipFoundry address with a simple subject and body. You don't need to write the script yourself.

> **To:** `[INSERT_EMAIL_ADDRESS_HERE]`
> **Subject:** New Reel Request
> **Body:** “Create me a reel on NVIDIA CEO’s recent comment on the five layers of AI.”

**Step 2: Automated Intelligence**
Upon receiving your email, ClipFoundry automatically:

* **Detects Format:** Identifies this as a "Reel" and sets a default duration (e.g., 20 seconds).
* **Researches:** Gathers context on the topic to write a strong, influencer-style script.
* **Assigns Persona:** Selects the best narrator voice (e.g., "Tech Insider" or "AI Futurist").
* **Writes:** Drafts a punchy, perfectly paced script optimized for video models.

**Step 3: Review the Proposal**
You will receive a reply email containing the proposed script:

> *Subject: Re: New Reel Request*
> "Here is the script I propose for your reel:
> [Full script text in a clean single paragraph...]"

**Step 4: The Feedback Loop**
You have two options to reply:

* **Option A (Refine):** Reply with feedback like *"Make it sound more energetic"* or *"Focus more on the 4th layer."* The agent will iterate and email you a new version.
* **Option B (Approve):** Reply with **“Looks good”**.

**Step 5: Production**
Once you approve, the agent locks the script, generates the visual clips using AI video models, stitches them together, and emails you a secure download link for the final MP4.

---

### 2. The Fire-and-Go Studio (Chat) 💬*Best for: Quick generation, using specific images, and instant previews.*

Access ClipFoundry via the **OpenWebUI** interface for a faster, direct workflow. This is perfect when you already have assets or want immediate results without the email back-and-forth.

> **Access the Chat:** `[INSERT_CHAT_URL_HERE]`

#### How it Works:
**Step 1: Upload Assets (Optional)**
Drag and drop specific model or background images into the chat window if you want specific visuals included in your video.

**Step 2: Enter Your Prompt**
Type your request. You can provide a full script or just a high-level idea.

> **Example Prompt:**
> "Create a 20s energetic promo for a new coffee brand called Morning Jolt using these images."

**Step 3: Processing**
The Agent will:

1. Analyze your prompt and uploaded assets.
2. Generate a script (if one wasn't provided).
3. **Split** the script into scenes.
4. **Generate** video segments for every scene (animating your images or creating new ones).
5. **Stitch** the segments into a final MP4.

**Step 4: Download & Preview**
When the process is complete, the Agent will respond directly in the chat with a markdown preview and a download link

---

## 🛠️ Under the HoodClipFoundry runs as a self-contained appliance on your infrastructure, ensuring privacy and control.

* **Orchestrator:** Apache Airflow manages the complex workflows (Inbox Monitor, VideoGen DAG).
* **Scripting Brain:** Google Gemini (LLM) handles intent classification, research, and scriptwriting.
* **Video Engine:** Integrates with models like Nano Banana and Google Veo for high-quality video synthesis.
* **Storage:** A shared storage layer ensures assets are securely passed between agents and stages.

## 🆘 Troubleshooting
* **Email not received?** Check your spam folder. Ensure you are sending from a allow-listed email address.
* **Video generation failed?** If the chat returns an error, try reducing the complexity of the prompt or ensuring uploaded images are in standard formats (JPG/PNG).
* **Script too long?** If the generated script feels rushed, reply to the email asking the agent to "shorten it to 15 seconds."

---

*Powered by ClipFoundry.ai*
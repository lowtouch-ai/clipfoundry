import google.generativeai as genai
import os

# Configure your API key
# Get your API key from: https://aistudio.google.com/app/apikey
API_KEY = os.getenv('GEMINI_API_KEY')  # Or set it directly: API_KEY = 'your-api-key-here'
if not API_KEY:
    raise ValueError("Please set the GEMINI_API_KEY environment variable or replace with your API key.")

genai.configure(api_key=API_KEY)

# Initialize the Gemini 2.5 Pro model
model = genai.GenerativeModel('gemini-2.5-pro')

# Sample prompt
prompt = "Explain quantum computing in simple terms."

# Generate content
response = model.generate_content(prompt)

# Print the response
print("Gemini 2.5 Pro Response:")
print(response.text)

# Optional: For chat-like interaction
# chat = model.start_chat()
# user_input = input("Enter your message: ")
# response = chat.send_message(user_input)
# print(response.text)
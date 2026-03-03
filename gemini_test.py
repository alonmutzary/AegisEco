# This script tests the connection to the Gemini API.

import os
from dotenv import load_dotenv
from google import genai

# 1. Load environment variables from the .env file
load_dotenv()

# 2. Fetch the API key from the environment
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("Gemini API key not found! Please check your .env file.")

# 3. Initialize the client
client = genai.Client(api_key=api_key)

try:
    print("Sending request to Gemini API...")
    
    # 4. Send the prompt to the updated model
    response = client.models.generate_content(
        model='gemini-2.5-flash',
        contents="You are AegisEco's core AI. Hello, are you ready to process flood data?"
    )

    # 5. Print the response
    print("\nResponse from Agent:")
    print(response.text)

except Exception as e:
    print(f"An error occurred: {e}")
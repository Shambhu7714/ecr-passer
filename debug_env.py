import os
from dotenv import load_dotenv
print(f"Current Dir: {os.getcwd()}")
print(f".env exists: {os.path.exists('.env')}")
load_dotenv()
print(f"GEMINI_API_KEY from env: {os.getenv('GEMINI_API_KEY') is not None}")

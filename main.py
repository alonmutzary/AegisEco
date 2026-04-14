"""
This is the main entry point for the AegisEco system. 
Running 'python main.py' in the terminal will immediately execute a single system cycle (fetching data and running the AI agents), 
and then start a background scheduler to automatically run the cycle at the top of every hour.
"""

import os
import sys
import time
import signal
from datetime import datetime, timedelta
from dotenv import load_dotenv

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_EXECUTED
from crewai import LLM

base_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(base_dir)

# Disable litellm's remote fetch before importing crewai components
os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"

from src.crew.aegiseco_crew import AegisEcoCrew

last_sigint_time = 0
scheduler = BackgroundScheduler()
def run_system_cycle():
    """
    Executes the main system cycle.
    Rotates between different Gemini models by updating the environment variables
    if the primary model fails.
    """
    current_time = datetime.now().strftime('%H:%M:%S')
    print(f"\n[{current_time}] Starting AegisEco System Cycle...")

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY is missing from .env file.")
        return

    # Ensure the API key is explicitly set in the environment for LiteLLM
    os.environ["GEMINI_API_KEY"] = api_key

    # Define the models to try in order of preference
    models_to_try = [
        "gemini/gemini-2.5-flash-lite", # Primary: Fastest, lowest cost
        "gemini/gemini-2.5-flash",      # Backup 1: Standard flash
        "gemini/gemini-2.0-pro-exp"     # Backup 2: Most capable, highest cost
    ]

    result = None

    for attempt_idx, model_name in enumerate(models_to_try):
        attempt = attempt_idx + 1
        try:
            print(f"--- Attempt {attempt} (Model: {model_name}) ---")
            
            # The Magic Fix: Update the environment variable BEFORE creating the crew.
            # CrewAI 1.14.1+ agents will automatically pick up this default model if 
            # no explicit LLM object is passed to them.
            os.environ["MODEL"] = model_name
            
            # Initialize the project AFTER setting the environment variable
            aegis_project = AegisEcoCrew()
            full_crew = aegis_project.crew()
            
            # Note: We NO LONGER loop through full_crew.agents and inject the LLM manually.
            # The agents will use the MODEL environment variable we just set.
                
            result = full_crew.kickoff()
            
            finish_time = datetime.now().strftime('%H:%M:%S')
            print(f"\n[{finish_time}] Cycle Completed Successfully!")
            print("================ SUMMARY ================")
            print(result)
            print("=========================================\n")
            
            break  # Exit loop on success
            
        except Exception as e:
            error_msg = str(e)
            print(f"\nAttempt {attempt} Failed: {error_msg[:150]}...")
            
            if attempt < len(models_to_try):
                print(f"Model failure detected. Switching to backup model: {models_to_try[attempt_idx + 1]}...")
                time.sleep(2) # Brief pause before trying the next model
            else:
                print("All models failed. System cycle aborted.")
                
def print_next_run_time(event=None):
    """
    Calculates and prints the scheduled time for the next cycle.
    """
    now = datetime.now()
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    run_time_str = next_hour.strftime('%H:%M:%S')
    print(f"\nSleeping... Next system cycle is scheduled for: {run_time_str}\n")

if __name__ == "__main__":
    load_dotenv()
    
    print("AegisEco Controller Started (Paid Tier Enabled).")
    print("Monitoring Israel Floods...")
    print("Press Ctrl+C to exit.\n")
    
    run_system_cycle()
    
    scheduler.add_job(run_system_cycle, 'cron', minute=0) 
    scheduler.add_listener(print_next_run_time, EVENT_JOB_EXECUTED)
    
    print_next_run_time()
    
    scheduler.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        current_time = time.time()
        if current_time - last_sigint_time < 3:
            print("\nConfirmed. AegisEco Controller Shutting Down...")
            scheduler.shutdown(wait=False)
            sys.exit(0)
        else:
            print("\n[?] Press Ctrl+C again within 3 seconds to confirm exit.")
            last_sigint_time = current_time
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nConfirmed. AegisEco Controller Shutting Down...")
                scheduler.shutdown(wait=False)
                sys.exit(0)
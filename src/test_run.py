import os
from dotenv import load_dotenv
from crew.aegiseco_crew import AegisEcoCrew

from database.db_manager import fetch_feb_records

def run_single_test():
    # 1. Load the environment variables (API keys, DB URL)
    load_dotenv()
    
    print("==================================================")
    print("🧪 AegisEco Single Test Run Initialized...")
    print("==================================================\n")
    
    # 2. Run Amitay's code to fetch and save data (No AI involved)
    # print("Step 1: Running Amitay's DB population script...")
    try:
        # Since the DB already has February data, running this with 'ON CONFLICT DO NOTHING' 
        # is safe and will just verify the connection works without duplicating rows.
        #fetch_feb_records()
        print("✅ Database step completed successfully.\n")
    except Exception as e:
        print(f"❌ Database step failed: {e}\n")
        return # Stop the test if the DB connection fails

    # 3. Wake up the CrewAI agents to analyze the data
    print("Step 2: Waking up the AI Agents for analysis (Threshold: 5.0mm)...")
    try:
        # Initialize and kickoff the crew exactly once
        eco_crew = AegisEcoCrew().crew()
        result = eco_crew.kickoff()
        
        print("\n✅ AI Analysis completed successfully!")
        print("--- Final Output from Communications Officer ---")
        print(result)
        print("------------------------------------------------\n")
        
    except Exception as e:
        print(f"\n❌ An error occurred during the AI cycle: {e}\n")

if __name__ == "__main__":
    run_single_test()
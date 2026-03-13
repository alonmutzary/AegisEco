# AegisEco

before using agents run in terminal:
```bash
pip install -r requirements.txt
```

Project Structure:

```text
src/
│
├── main.py                 # Runs the whole system: gets the data, then starts the AI.
├── test_run.py             # A safe way to test the AI agents.
│
├── crew/                   # 🧠 THE AI TEAM (CrewAI)
│   ├── aegiseco_crew.py    # The code that creates the AI agents and starts their work.
│   │
│   ├── config/             # Text instructions for the AI
│   │   ├── agents.yaml     # Defines WHO the AI agents are (e.g., "Hydrological Analyst").
│   │   └── tasks.yaml      # Defines WHAT the AI agents need to do.
│   │
│   └── tools/              # Custom skills we teach the AI
│       └── db_tools.py     # A tool that lets the AI read our database.
│
├── database/               # 💾 SAVING DATA
│   └── db_manager.py       # Connects to our cloud database (Neon) to save the data.
│
└── data_sentinel/          # 👁️ GETTING DATA
    └── ims_client.py       # Downloads live rain data from the weather service (IMS).
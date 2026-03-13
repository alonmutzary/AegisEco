import os
from crewai import Agent, Crew, Process, Task
from crewai.project import CrewBase, agent, crew, task
from langchain_google_genai import ChatGoogleGenerativeAI

# 1. Import the mock tool for testing purposes (until the real IMS tool is ready)
from crew.tools.db_tools import get_high_rainfall_events
# 2. Initialize the Gemini 2.5 Flash model
gemini_llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    google_api_key=os.getenv("GEMINI_API_KEY")
)

@CrewBase
class AegisEcoCrew():
    """AegisEco Flood Detection Crew"""
    
    # 3. Define the paths to the YAML configuration files
    agents_config = 'config/agents.yaml'
    tasks_config = 'config/tasks.yaml'

    @agent
    def data_engineer(self) -> Agent:
        return Agent(
            config=self.agents_config['data_engineer'],
            llm=gemini_llm,
            # Real tools will be added here once developed
            tools=[] 
        )

    @agent
    def flood_analyst(self) -> Agent:
        return Agent(
            config=self.agents_config['flood_analyst'],
            llm=gemini_llm,
            # Give the analyst the real tool so it can "query" the database
            tools=[get_high_rainfall_events] 
        )

    @agent
    def communications_officer(self) -> Agent:
        return Agent(
            config=self.agents_config['communications_officer'],
            llm=gemini_llm,
            tools=[]
        )

    @task
    def fetch_and_store_task(self) -> Task:
        return Task(config=self.tasks_config['fetch_and_store_task'])

    @task
    def analyze_risk_task(self) -> Task:
        return Task(config=self.tasks_config['analyze_risk_task'])

    @task
    def alert_task(self) -> Task:
        return Task(config=self.tasks_config['alert_task'])

    @crew
    def crew(self) -> Crew:
        """Creates the AegisEco crew"""
        return Crew(
            agents=self.agents,
            tasks=self.tasks,
            process=Process.sequential, # Tasks are executed one after the other
            verbose=True
        )
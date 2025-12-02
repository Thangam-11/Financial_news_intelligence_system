from agents_lang.ingestion_agent import IngestionAgent

agent = IngestionAgent()

state = {}
new_state = agent.run(state)

print("Returned keys:", new_state.keys())
print("Number of articles:", len(new_state.get("articles", [])))


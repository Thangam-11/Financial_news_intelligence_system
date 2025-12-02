from agents_lang.ingestion_agent import IngestionAgent

ing = IngestionAgent()
out = ing.run({})
print("Articles fetched:", len(out["articles"]))

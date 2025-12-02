# src/agents/ingestion_agent.py

from utils.logger_system import get_logger

logger = get_logger("IngestionAgent")

def safe_import_ingestion():
    """Lazy import to avoid circular import issues."""
    from data_ingestion.data_ingestion_full import run_ingestion
    return run_ingestion

class IngestionAgent:
    def run(self, state=None):
        logger.info("IngestionAgent: Starting ingestion...")

        run_ingestion = safe_import_ingestion()
        articles = run_ingestion()


        if state is None:
            state = {}

        state["articles"] = articles

        logger.info(f"IngestionAgent: stored {len(articles)} articles in state")

        return state

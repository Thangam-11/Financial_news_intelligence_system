import time
import logging
from orchestrator.langgraph_workflow import WorkflowState
from data_ingestion.data_ingestion_full import run_ingestion

logger = logging.getLogger("IngestionAgent")


def ingestion_agent(state: WorkflowState) -> WorkflowState:
    logger.info("=== [AGENT 1] INGESTION START ===")
    start = time.time()

    try:
        # Run ingestion
        articles = run_ingestion()

        if not isinstance(articles, list):
            raise TypeError("run_ingestion must return a list of articles")

        # Update state
        state.raw_articles = articles
        state.current_stage = "ingestion_complete"

        logger.info(f"📥 Ingested {len(articles)} articles successfully")

    except Exception as e:
        msg = f"Ingestion failed: {e}"
        logger.error(msg)
        state.errors.append(msg)

    # Save stage timing
    state.stage_durations.append({
        "stage": "ingestion",
        "duration": time.time() - start
    })

    return state

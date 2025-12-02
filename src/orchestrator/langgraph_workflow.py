# langgraph_workflow_safe.py (improved)
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime
import time
import logging
import inspect
import traceback

logger = logging.getLogger("LangGraphWorkflow")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

# ---------- Safe imports with clear error messages ----------
def safe_import(module_path: str, symbol: str):
    try:
        mod = __import__(module_path, fromlist=[symbol])
        return getattr(mod, symbol)
    except Exception as e:
        logger.error(f"Failed to import {symbol} from {module_path}: {e}")
        return None

# Replace these paths if your package structure differs
run_ingestion = safe_import("data_ingestion.data_ingestion_full", "run_ingestion")
run_preprocessing = safe_import("preprocessing.preprocessing_full", "run_preprocessing")
run_deduplication = safe_import("deduplication.removing_duplicate", "run_deduplication")
run_ner_pipeline = safe_import("name_ner.ner_full", "run_ner_pipeline")
run_embedding = safe_import("embedding.embedder", "run_embedding")
run_impact_pipeline = safe_import("impact_scores.mapping_score", "run_impact_pipeline")
semantic_query = safe_import("query_engine.engine", "semantic_query")

# ---------- Dataclass state ----------
@dataclass
class WorkflowState:
    raw_articles: List[Dict[str, Any]] = field(default_factory=list)
    preprocessed_articles: List[Dict[str, Any]] = field(default_factory=list)
    deduplicated_articles: List[Dict[str, Any]] = field(default_factory=list)
    extracted_entities: List[Dict[str, Any]] = field(default_factory=list)
    embeddings_indexed: bool = False
    impact_scores: List[Dict[str, Any]] = field(default_factory=list)
    query: str = ""
    query_results: List[Dict[str, Any]] = field(default_factory=list)
    current_stage: str = "initialized"
    stage_durations: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)

# ---------- Helpers ----------
def call_flexible(fn: Optional[Callable], *args, stage_name: str = ""):
    """
    Call fn with args if it accepts them; else call without args.
    Returns the function result or None if fn is None or raised.
    """
    if fn is None:
        logger.error(f"{stage_name}: function is not available (import failed).")
        return None

    try:
        sig = inspect.signature(fn)
        # if fn accepts any parameters, pass them; otherwise call with no args
        if len(sig.parameters) > 0:
            return fn(*args)
        else:
            return fn()
    except TypeError:
        # fallback: try calling with no args
        try:
            return fn()
        except Exception:
            logger.error(f"{stage_name}: fallback no-arg call failed:\n{traceback.format_exc()}")
            return None
    except Exception:
        logger.error(f"{stage_name}: call failed:\n{traceback.format_exc()}")
        return None

# Utility: measure wrapper with richer error capture
def timed_stage(stage_name: str):
    def decorator(fn: Callable[[WorkflowState], WorkflowState]):
        def wrapper(state: WorkflowState) -> WorkflowState:
            logger.info(f"\n=== [{stage_name}] START ===")
            t0 = time.time()
            try:
                res = fn(state)
                duration = time.time() - t0
                state.stage_durations.append({"stage": stage_name, "duration": duration})
                logger.info(f"✅ [{stage_name}] done in {duration:.3f}s")
                return res
            except Exception as e:
                duration = time.time() - t0
                tb = traceback.format_exc()
                err = f"{stage_name}: {str(e)}\n{tb}"
                logger.exception(f"❌ [{stage_name}] failed: {e}")
                state.errors.append(err)
                state.stage_durations.append({"stage": stage_name, "duration": duration, "error": True})
                return state
        return wrapper
    return decorator

# ---------- Agents ----------
@timed_stage("ingestion")
def ingestion_agent(state: WorkflowState) -> WorkflowState:
    # run_ingestion may or may not accept args; we call flexibly
    articles = call_flexible(run_ingestion, stage_name="ingestion")
    if articles is None:
        articles = []
    if not isinstance(articles, list):
        logger.warning("ingestion returned non-list, converting to list if possible")
        if hasattr(articles, "to_dict"):
            articles = articles.to_dict("records")
        else:
            articles = list(articles) if articles else []
    state.raw_articles = articles
    state.current_stage = "ingestion_complete"
    state.stats["raw_articles_count"] = len(articles)
    return state

@timed_stage("preprocessing")
def preprocessing_agent(state: WorkflowState) -> WorkflowState:
    articles = state.raw_articles
    if not articles:
        logger.warning("No raw articles to preprocess; skipping")
        state.preprocessed_articles = []
        return state

    result = call_flexible(run_preprocessing, articles, stage_name="preprocessing")
    if result is None:
        # attempt to continue gracefully with original articles
        state.preprocessed_articles = articles
        logger.warning("preprocessing returned None — using raw_articles as fallback")
        return state

    if hasattr(result, "to_dict"):
        state.preprocessed_articles = result.to_dict('records')
    elif isinstance(result, list):
        state.preprocessed_articles = result
    else:
        try:
            state.preprocessed_articles = list(result)
        except Exception:
            raise TypeError("run_preprocessing must return DataFrame/list/iterable")
    state.current_stage = "preprocessing_complete"
    return state

@timed_stage("deduplication")
def deduplication_agent(state: WorkflowState) -> WorkflowState:
    input_list = state.preprocessed_articles or state.raw_articles
    if not input_list:
        logger.warning("No articles available for deduplication; skipping")
        state.deduplicated_articles = []
        return state

    result = call_flexible(run_deduplication, input_list, stage_name="deduplication")
    if result is None:
        # fallback to input_list
        state.deduplicated_articles = input_list
        logger.warning("deduplication returned None — using input_list as fallback")
        return state

    if hasattr(result, "to_dict"):
        state.deduplicated_articles = result.to_dict("records")
    elif isinstance(result, list):
        state.deduplicated_articles = result
    else:
        try:
            state.deduplicated_articles = list(result)
        except Exception:
            raise TypeError("run_deduplication must return DataFrame/list/iterable")
    state.current_stage = "deduplication_complete"
    return state

@timed_stage("ner")
def ner_agent(state: WorkflowState) -> WorkflowState:
    articles = state.deduplicated_articles or state.preprocessed_articles or state.raw_articles
    if not articles:
        logger.warning("No articles available for NER; skipping")
        return state

    result = call_flexible(run_ner_pipeline, articles, stage_name="ner")
    if isinstance(result, list):
        state.extracted_entities = result
    else:
        # If NER writes to DB and returns None, that's ok — just log
        logger.info("NER pipeline returned None or non-list; assuming it persisted results")
    state.current_stage = "ner_complete"
    return state

@timed_stage("embedding")
def embedding_agent(state: WorkflowState) -> WorkflowState:
    articles = state.deduplicated_articles or state.preprocessed_articles or state.raw_articles
    if not articles:
        logger.warning("No articles for embedding; skipping embedding")
        state.embeddings_indexed = False
        return state

    result = call_flexible(run_embedding, articles, stage_name="embedding")
    # Accept truthy returned value or assume True if embedding succeeded without return
    state.embeddings_indexed = bool(result) if result is not None else True
    state.current_stage = "embedding_complete"
    return state

@timed_stage("impact_scoring")
def impact_scoring_agent(state: WorkflowState) -> WorkflowState:
    articles = state.deduplicated_articles or state.preprocessed_articles or state.raw_articles
    if not articles:
        logger.warning("No articles for impact scoring; skipping")
        state.impact_scores = []
        return state

    result = call_flexible(run_impact_pipeline, articles, stage_name="impact_scoring")
    if result is None:
        state.impact_scores = []
        logger.warning("impact scoring returned None — no impact_scores")
        return state

    if hasattr(result, "to_dict"):
        state.impact_scores = result.to_dict("records")
    elif isinstance(result, list):
        state.impact_scores = result
    else:
        try:
            state.impact_scores = list(result)
        except Exception:
            raise TypeError("run_impact_pipeline must return DataFrame/list/iterable")
    state.current_stage = "impact_complete"
    return state

@timed_stage("query")
def query_agent(state: WorkflowState) -> WorkflowState:
    query = (state.query or "").strip()
    if not query:
        logger.info("No query provided; skipping query agent")
        return state

    if semantic_query is None:
        logger.error("semantic_query function not available — skipping query")
        state.query_results = []
        return state

    if not state.embeddings_indexed:
        logger.warning("Embeddings not indexed; query may return stale or empty results")

    raw = call_flexible(semantic_query, query, stage_name="query")
    results = []

    if raw:
        # robust parsing: accept several shapes
        docs = raw.get("documents") or raw.get("docs") or raw.get("documents_list") or []
        ids = raw.get("ids") or raw.get("doc_ids") or []
        distances = raw.get("distances") or raw.get("scores") or []
        metadatas = raw.get("metadatas") or raw.get("metadata") or []

        # unwrap nested lists
        if docs and isinstance(docs[0], list):
            docs = docs[0]
        if ids and isinstance(ids[0], list):
            ids = ids[0]
        if distances and isinstance(distances[0], list):
            distances = distances[0]
        if metadatas and isinstance(metadatas[0], list):
            metadatas = metadatas[0]

        n = max(len(docs), len(ids), len(distances), len(metadatas))
        for i in range(n):
            results.append({
                "id": ids[i] if i < len(ids) else None,
                "document": docs[i] if i < len(docs) else None,
                "distance": distances[i] if i < len(distances) else None,
                "metadata": metadatas[i] if i < len(metadatas) else None,
            })
    else:
        logger.info("semantic_query returned no data")

    state.query_results = results
    state.current_stage = "query_complete"
    return state

# ---------- Finalize ----------
def finalize_agent(state: WorkflowState) -> WorkflowState:
    logger.info("\n=== [finalize] Aggregating stats ===")
    total_duration = sum(float(s.get("duration", 0)) for s in state.stage_durations)
    stats = {
        "workflow_complete": True,
        "total_duration_seconds": total_duration,
        "stages_completed": len(state.stage_durations),
        "errors_count": len(state.errors),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "stage_breakdown": state.stage_durations,
        "raw_articles_count": len(state.raw_articles),
        "unique_articles_count": len(state.deduplicated_articles),
        "query_results_count": len(state.query_results),
    }
    state.stats = stats
    state.current_stage = "finalized"
    logger.info(f"Workflow finished in {total_duration:.3f}s with {len(state.errors)} errors")
    if state.errors:
        logger.warning("Errors captured during run:")
        for e in state.errors:
            logger.warning("---\n" + (e if isinstance(e, str) else str(e)))
    return state

# ---------- Orchestration ----------
def run_full_pipeline(query: Optional[str] = None) -> WorkflowState:
    st = WorkflowState(query=query or "")
    st = ingestion_agent(st)
    st = preprocessing_agent(st)
    st = deduplication_agent(st)
    st = ner_agent(st)
    st = embedding_agent(st)
    st = impact_scoring_agent(st)
    # conditional query
    if st.query:
        st = query_agent(st)
    st = finalize_agent(st)
    return st

if __name__ == "__main__":
    state = run_full_pipeline(query="What happened to Tesla today?")
    import pprint
    pprint.pprint(asdict(state), width=120)

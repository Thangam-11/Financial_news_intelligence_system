# ner_full.py

import sqlite3
import pandas as pd
import spacy
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
from pathlib import Path

from utils.logger_system import get_logger
logger = get_logger("NER")

# ============================
# PATH SETUP
# ============================
DATA_DIR = Path(r"C:\Users\thang\Desktop\hackthon_project\data")

UNIQUE_DB_PATH = DATA_DIR / "unique_articles.db"
NER_DB_PATH = DATA_DIR / "ner_entities.db"


# ============================
# LOAD SPACY + FINBERT MODELS
# ============================
logger.info("Loading SpaCy model (en_core_web_lg)...")
nlp = spacy.load("en_core_web_lg")

logger.info("Loading FinBERT Financial NER model...")
tokenizer = AutoTokenizer.from_pretrained("dslim/bert-base-NER")
finbert_model = AutoModelForTokenClassification.from_pretrained("dslim/bert-base-NER")
finbert_ner = pipeline("ner", model=finbert_model, tokenizer=tokenizer, aggregation_strategy="simple")

logger.info("NER models loaded successfully!")


# ============================
# 1. LOAD UNIQUE ARTICLES
# ============================
def load_unique_articles():
    conn = sqlite3.connect(UNIQUE_DB_PATH)
    df = pd.read_sql_query("SELECT * FROM unique_articles", conn)
    conn.close()
    logger.info(f"Loaded {len(df)} unique articles for NER")
    return df


# ============================
# 2. APPLY NER
# ============================
def extract_entities(text):
    spacy_doc = nlp(text)
    spacy_ents = [(ent.text, ent.label_) for ent in spacy_doc.ents]

    finbert_ents = finbert_ner(text)
    finbert_ents = [(x["word"], x["entity_group"], x["score"]) for x in finbert_ents]

    return spacy_ents, finbert_ents


# Normalize entity names
def normalize_entity(entity):
    entity = entity.strip()
    entity = entity.replace("\n", " ").replace("\t", " ")

    # Optional custom normalization
    replacements = {
        "Reliance Industries Ltd": "Reliance",
        "Reliance Industries": "Reliance",
        "Tata Consultancy Services": "TCS",
        "Apple Inc": "Apple",
    }
    return replacements.get(entity, entity)


# ============================
# 3. STORE ENTITIES
# ============================
def save_entities(article_id, entities):
    conn = sqlite3.connect(NER_DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ner_entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            entity TEXT,
            label TEXT,
            confidence REAL,
            published_at TEXT
        )
    """)

    for ent, label, conf, published_at in entities:
        cur.execute("""
            INSERT INTO ner_entities (article_id, entity, label, confidence, published_at)
            VALUES (?, ?, ?, ?, ?)
        """, (article_id, ent, label, conf, published_at))

    conn.commit()
    conn.close()


# ============================
# 4. MAIN PIPELINE
# ============================
def run_ner_pipeline():
    logger.info("Starting NER pipeline...")

    df = load_unique_articles()

    for _, row in df.iterrows():
        text = row["clean_text"]
        article_id = row["id"]
        published_at = row["published_at"]

        # Extract entities
        spacy_ents, finbert_ents = extract_entities(text)

        final_entities = []

        # SpaCy entities
        for ent, label in spacy_ents:
            final_entities.append((normalize_entity(ent), label, 0.99, published_at))

        # FinBERT entities
        for ent, label, score in finbert_ents:
            final_entities.append((normalize_entity(ent), label, score, published_at))

        save_entities(article_id, final_entities)

        logger.info(f"Extracted & saved {len(final_entities)} entities from article {article_id}")

    logger.info("NER pipeline completed successfully!")


# ============================
# RUN SCRIPT
# ============================
if __name__ == "__main__":
    run_ner_pipeline()

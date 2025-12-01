# dedupe_full.py

import sqlite3
import pandas as pd
import hashlib
from pathlib import Path
from sentence_transformers import SentenceTransformer, util

# Import Logger
from utils.logger_system import get_logger
logger = get_logger("Deduplication")

# ================================
# PATH SETUP
# ================================
DATA_DIR = Path(r"C:\Users\thang\Desktop\hackthon_project\data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

PROCESSED_DB_PATH = DATA_DIR / "articles.db"        # contains processed_articles table
UNIQUE_DB_PATH    = DATA_DIR / "unique_articles.db" # final output DB
CSV_PATH          = DATA_DIR / "unique_articles.csv"
JSON_PATH         = DATA_DIR / "unique_articles.json"

# Load MiniLM embedding model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


# ======================================
# 1. LOAD CLEAN ARTICLES
# ======================================
def load_processed_articles():
    try:
        conn = sqlite3.connect(PROCESSED_DB_PATH)
        df = pd.read_sql_query("SELECT * FROM processed_articles", conn)
        conn.close()
        logger.info(f"Loaded {len(df)} processed articles")
        return df
    except Exception as e:
        logger.error(f"Error loading processed articles: {e}")
        raise


# ======================================
# 2. EXACT DUPLICATE REMOVAL (HASH)
# ======================================
def generate_hash(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def remove_exact_duplicates(df):
    logger.info("Running exact deduplication using MD5 hashing...")

    df["hash"] = df["clean_text"].apply(generate_hash)

    before = len(df)
    df = df.drop_duplicates(subset=["hash"])
    after = len(df)

    logger.info(f"Exact duplicates removed: {before - after}")
    return df


# ======================================
# 3. SEMANTIC DUPLICATE REMOVAL
# ======================================
def remove_semantic_duplicates(df, threshold=0.82):
    logger.info("Starting semantic deduplication...")

    texts = df["clean_text"].tolist()
    embeddings = model.encode(texts, convert_to_tensor=True)

    keep = []
    removed = 0

    for i in range(len(df)):
        if i in keep:
            continue

        keep.append(i)
        sims = util.cos_sim(embeddings[i], embeddings)[0].cpu().numpy()

        for j in range(i + 1, len(df)):
            if j in keep:
                continue
            if sims[j] > threshold:
                removed += 1
                logger.debug(f"Removed semantic duplicate: row {j} similar to row {i}")

    logger.info(f"Semantic duplicates removed: {removed}")

    unique_df = df.iloc[keep]
    return unique_df


# ======================================
# 4. SAVE UNIQUE ARTICLES → SQLITE
# ======================================
def save_unique_to_sqlite(df):
    logger.info("Saving unique articles to SQLite database...")

    conn = sqlite3.connect(UNIQUE_DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS unique_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            processed_id INTEGER,
            clean_text TEXT,
            source TEXT,
            published_at TEXT
        )
    """)

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO unique_articles (processed_id, clean_text, source, published_at)
            VALUES (?, ?, ?, ?)
        """, (
            row["id"],
            row["clean_text"],
            row["source"],
            row["published_at"]
        ))

    conn.commit()
    conn.close()
    logger.info(f"Saved {len(df)} unique articles into SQLite → {UNIQUE_DB_PATH}")


# ======================================
# 5. SAVE TO CSV
# ======================================
def save_unique_to_csv(df):
    df.to_csv(CSV_PATH, index=False, encoding="utf-8")
    logger.info(f"Saved unique articles CSV → {CSV_PATH}")


# ======================================
# 6. SAVE TO JSON
# ======================================
def save_unique_to_json(df):
    df.to_json(JSON_PATH, orient="records", indent=4, force_ascii=False)
    logger.info(f"Saved unique articles JSON → {JSON_PATH}")


# ======================================
# 7. MAIN PIPELINE RUNNER
# ======================================
def run_deduplication():
    logger.info("Starting deduplication pipeline...")

    df = load_processed_articles()
    df = remove_exact_duplicates(df)
    df = remove_semantic_duplicates(df)

    logger.info(f"Final unique article count: {len(df)}")

    save_unique_to_sqlite(df)
    save_unique_to_csv(df)
    save_unique_to_json(df)

    logger.info("Deduplication completed successfully.")
    return df


# ======================================
# EXECUTE SCRIPT
# ======================================
if __name__ == "__main__":
    df = run_deduplication()
    print(df.head())

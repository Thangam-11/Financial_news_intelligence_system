import sqlite3
import pandas as pd
from pathlib import Path
from sentence_transformers import SentenceTransformer
from chromadb import PersistentClient

# Logger
from utils.logger_system import get_logger
logger = get_logger("Embedding")

# ===============================
# PATHS
# ===============================

DATA_DIR = Path(r"C:\Users\thang\Desktop\hackthon_project\data")
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "unique_articles.db"
CHROMA_DIR = DATA_DIR / "chroma_store"


# ===============================
# LOAD ARTICLES FROM SQLITE
# ===============================

def load_unique_articles():
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM unique_articles", conn)
        conn.close()

        logger.info(f"Loaded {len(df)} unique articles for embedding")
        return df
    except Exception as e:
        logger.error(f"Error loading unique articles: {e}")
        raise


# ===============================
# LOAD EMBEDDING MODEL
# ===============================

model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


# ===============================
# INIT NEW CHROMA CLIENT (LATEST)
# ===============================

client = PersistentClient(path=str(CHROMA_DIR))

collection = client.get_or_create_collection(
    name="financial_news",
    metadata={"hnsw:space": "cosine"}
)


# ===============================
# EMBEDDING & SAVE TO CHROMA
# ===============================

def embed_and_store(df):
    texts = df["clean_text"].tolist()
    ids = df["id"].astype(str).tolist()

    logger.info("Generating embeddings...")
    embeds = model.encode(texts, show_progress_bar=True).tolist()

    logger.info("Storing embeddings into ChromaDB...")

    collection.add(
        ids=ids,
        documents=texts,
        embeddings=embeds,
        metadatas=[
            {
                "source": df.iloc[i].source,
                "published_at": df.iloc[i].published_at
            }
            for i in range(len(df))
        ]
    )

    logger.info(f"Successfully saved {len(df)} embeddings to ChromaDB")


# ===============================
# MAIN PIPELINE
# ===============================

def run_embedding():
    logger.info("Starting embedding pipeline...")

    df = load_unique_articles()
    embed_and_store(df)

    logger.info("Embedding pipeline completed successfully.")


# ===============================
# RUN SCRIPT
# ===============================

if __name__ == "__main__":
    run_embedding()

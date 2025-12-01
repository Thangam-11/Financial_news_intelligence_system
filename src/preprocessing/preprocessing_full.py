# preprocessing_full.py

import sqlite3
import pandas as pd
import re
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

DB_PATH = Path(r"C:\Users\thang\Desktop\hackthon_project\data\articles.db")


# ============================
# 1. LOAD RAW ARTICLES
# ============================

def load_raw_articles():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM raw_articles", conn)
    conn.close()
    return df


# ============================
# 2. CLEAN HTML TAGS
# ============================

def clean_html(text):
    if not text:
        return ""

    soup = BeautifulSoup(text, "html.parser")

    # remove JS & CSS
    for script in soup(["script", "style"]):
        script.extract()

    cleaned = soup.get_text(separator=" ")
    return cleaned.strip()


# ============================
# 3. TEXT NORMALIZATION
# ============================

def normalize_text(text):
    if not text:
        return ""

    text = re.sub(r"\s+", " ", text)  # collapse spaces
    text = re.sub(r"[\r\n\t]", " ", text)
    text = re.sub(r"http\S+", "", text)  # remove URLs

    # remove disclaimers
    bad_phrases = [
        "ADVERTISEMENT",
        "Read more on ET Markets",
        "Download The Economic Times",
        "Read more",
        "Reuters Graphics"
    ]
    for phrase in bad_phrases:
        text = text.replace(phrase, "")

    return text.strip()


# ============================
# 4. CLEAN + MERGE TITLE + CONTENT
# ============================

def preprocess_article(row):
    title = clean_html(row["title"])
    content = clean_html(row["content"])

    title = normalize_text(title)
    content = normalize_text(content)

    merged = f"{title}. {content}".strip()

    return merged


# ============================
# 5. SAVE CLEAN ARTICLES → SQLite
# ============================

def save_processed(df):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_id INTEGER,
            clean_text TEXT,
            source TEXT,
            published_at TEXT
        )
    """)

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO processed_articles
            (raw_id, clean_text, source, published_at)
            VALUES (?, ?, ?, ?)
        """, (row["id"], row["clean_text"], row["source"], row["published_at"]))

    conn.commit()
    conn.close()

    print("Processed articles saved → processed_articles table")


# ============================
# 6. RUN PIPELINE
# ============================

def run_preprocessing():
    print("Loading raw articles...")
    df = load_raw_articles()

    print("Cleaning & normalizing...")
    df["clean_text"] = df.apply(preprocess_article, axis=1)

    # remove empty
    df = df[df["clean_text"].str.len() > 50]

    print(f"Final cleaned count: {len(df)}")

    save_processed(df)

    return df


if __name__ == "__main__":
    cleaned_df = run_preprocessing()
    print(cleaned_df.head())

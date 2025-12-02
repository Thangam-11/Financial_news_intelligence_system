# impact_full.py

import sqlite3
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from pathlib import Path

from utils.logger_system import get_logger
logger = get_logger("ImpactAnalysis")

# ======================================================
# PATH SETUP
# ======================================================
DATA_DIR = Path(r"C:\Users\thang\Desktop\hackthon_project\data")
UNIQUE_DB_PATH = DATA_DIR / "unique_articles.db"
IMPACT_DB_PATH = DATA_DIR / "impact_scores.db"

# ======================================================
# LOAD FINBERT
# ======================================================
model_name = "ProsusAI/finbert"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

logger.info("FinBERT loaded for sentiment scoring")


# ======================================================
# FINBERT SENTIMENT
# ======================================================
def get_sentiment(text):
    inputs = tokenizer(text[:512], return_tensors="pt", truncation=True)
    outputs = model(**inputs)
    scores = torch.nn.functional.softmax(outputs.logits, dim=1)[0]

    labels = ["negative", "neutral", "positive"]
    sentiment = labels[scores.argmax().item()]
    sentiment_score = float(scores.max().item())

    return sentiment, sentiment_score


# ======================================================
# SECTOR MAPPING
# ======================================================
SECTOR_MAP = {
    "TCS": "IT",
    "Infosys": "IT",
    "Wipro": "IT",
    "HDFC": "Banking",
    "SBI": "Banking",
    "RBI": "Banking",
    "Reliance": "Energy",
    "ONGC": "Energy",
    "Tata Motors": "Automobile",
    "Maruti": "Automobile",
    "Sun Pharma": "Pharma",
    "Dr Reddy": "Pharma"
}

def detect_sector(text):
    for keyword, sector in SECTOR_MAP.items():
        if keyword.lower() in text.lower():
            return sector
    return "General"


# ======================================================
# URGENCY SCORING
# ======================================================
HIGH_URGENCY_KEYWORDS = [
    "breaking", "urgent", "crash", "bankruptcy", "ceo resigns",
    "fraud", "fine", "regulator", "lawsuit", "merger"
]

MEDIUM_URGENCY_KEYWORDS = [
    "forecast", "upgrade", "downgrade", "quarterly", "earnings"
]

def get_urgency(text):
    t = text.lower()

    if any(x in t for x in HIGH_URGENCY_KEYWORDS):
        return "High"
    if any(x in t for x in MEDIUM_URGENCY_KEYWORDS):
        return "Medium"
    return "Low"


# ======================================================
# PRICE IMPACT SCORING
# ======================================================
def price_impact(sentiment, urgency):
    if sentiment == "positive" and urgency == "High":
        return "High"
    if sentiment == "negative" and urgency == "High":
        return "High"
    if urgency == "Medium":
        return "Medium"
    return "Low"


# ======================================================
# LOAD UNIQUE ARTICLES
# ======================================================
def load_unique_articles():
    conn = sqlite3.connect(UNIQUE_DB_PATH)
    df = pd.read_sql_query("SELECT * FROM unique_articles", conn)
    conn.close()
    logger.info(f"Loaded {len(df)} unique articles for impact scoring")
    return df


# ======================================================
# SAVE IMPACT RESULTS
# ======================================================
def save_impact_results(df):
    conn = sqlite3.connect(IMPACT_DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS impact_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            sentiment TEXT,
            sentiment_score REAL,
            sector TEXT,
            urgency TEXT,
            price_impact TEXT
        )
    """)

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO impact_scores
            (article_id, sentiment, sentiment_score, sector, urgency, price_impact)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            row["id"],
            row["sentiment"],
            row["sentiment_score"],
            row["sector"],
            row["urgency"],
            row["price_impact"]
        ))

    conn.commit()
    conn.close()
    logger.info("Impact scores saved → impact_scores.db")


# ======================================================
# MAIN PIPELINE
# ======================================================
def run_impact_pipeline():
    logger.info("Starting impact scoring pipeline...")

    df = load_unique_articles()

    results = []

    for _, row in df.iterrows():
        text = row["clean_text"]

        sentiment, score = get_sentiment(text)
        sector = detect_sector(text)
        urgency = get_urgency(text)
        impact = price_impact(sentiment, urgency)

        results.append({
            "id": row["id"],
            "sentiment": sentiment,
            "sentiment_score": score,
            "sector": sector,
            "urgency": urgency,
            "price_impact": impact
        })

    result_df = pd.DataFrame(results)

    save_impact_results(result_df)

    logger.info("Impact scoring pipeline completed successfully")

    return result_df


# ======================================================
# RUN SCRIPT
# ======================================================
if __name__ == "__main__":
    df = run_impact_pipeline()
    print(df.head())

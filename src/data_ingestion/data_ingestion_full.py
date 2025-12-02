# ============================================================
#  DATA INGESTION PIPELINE - CLEAN & IMPORT-SAFE VERSION
# ============================================================

from pathlib import Path
import requests
import feedparser
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import json
import sqlite3
import pandas as pd
from utils.logger_system import get_logger

logger = get_logger("DataIngestion")


# ============================================================
#  RSS INGESTOR
# ============================================================
def fetch_rss(rss_feeds):
    all_articles = []
    for url in rss_feeds:
        try:
            feed = feedparser.parse(url)
            logger.info(f"[RSS] {url} → {len(feed.entries)} articles")

            for entry in feed.entries:
                article = {
                    "title": entry.title,
                    "content": getattr(entry, "summary", ""),
                    "link": entry.link,
                    "source": "RSS",
                    "published_at": datetime.utcnow().isoformat()
                }
                all_articles.append(article)
        except Exception as e:
            logger.exception(f"RSS error: {url} — {e}")
    return all_articles


# ============================================================
#  API INGESTOR (GNews)
# ============================================================
def fetch_api(api_urls):
    all_articles = []
    for url in api_urls:
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()
            items = data.get("articles", [])

            logger.info(f"[API] {url} → {len(items)} articles")

            for item in items:
                article = {
                    "title": item.get("title"),
                    "content": item.get("description") or item.get("content", ""),
                    "link": item.get("url"),
                    "source": "API",
                    "published_at": datetime.utcnow().isoformat()
                }
                all_articles.append(article)
        except Exception as e:
            logger.exception(f"API error: {url} — {e}")
    return all_articles


# ============================================================
#  REUTERS SCRAPER
# ============================================================
def get_reuters_links(homepage_url):
    try:
        resp = requests.get(homepage_url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(resp.text, "html.parser")

        links = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/markets/") and href.count("-") > 2:
                links.add(urljoin("https://www.reuters.com", href))

        logger.info(f"[SCRAPER] {homepage_url} → {len(links)} links found")
        return list(links)

    except Exception as e:
        logger.exception(f"Scraper link error: {homepage_url} — {e}")
        return []


def scrape_reuters_article(url):
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(resp.text, "html.parser")

        title = soup.find("h1")
        article = soup.find("article")

        if not title or not article:
            return None

        paragraphs = article.find_all("p")
        content = " ".join(p.text.strip() for p in paragraphs)

        if len(content) < 150:
            return None

        return {
            "title": title.text.strip(),
            "content": content,
            "link": url,
            "source": "SCRAPER",
            "published_at": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.exception(f"Scraper article error: {url} — {e}")
        return None


def fetch_scraper(pages):
    all_articles = []
    for page in pages:
        links = get_reuters_links(page)

        for link in links:
            article = scrape_reuters_article(link)
            if article:
                all_articles.append(article)

        logger.info(f"[SCRAPER] Total so far → {len(all_articles)}")
    return all_articles


# ============================================================
#  DATA STORAGE PATHS
# ============================================================
DATA_DIR = Path(r"C:\Users\thang\Desktop\hackthon_project\data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "articles.db"
CSV_PATH = DATA_DIR / "articles_output.csv"
JSON_PATH = DATA_DIR / "articles_output.json"


# ============================================================
#  SAVE FUNCTIONS
# ============================================================
def save_to_sqlite(articles, db_path=DB_PATH):
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            content TEXT,
            link TEXT UNIQUE,
            source TEXT,
            published_at TEXT
        )
    """)

    for a in articles:
        try:
            cur.execute("""
                INSERT OR IGNORE INTO raw_articles
                (title, content, link, source, published_at)
                VALUES (?, ?, ?, ?, ?)
            """, (a["title"], a["content"], a["link"], a["source"], a["published_at"]))
        except Exception:
            pass

    conn.commit()
    conn.close()
    logger.info(f"SQLite saved → {len(articles)} articles → {db_path}")


def save_to_csv(articles, csv_file=CSV_PATH):
    df = pd.DataFrame(articles)
    df.to_csv(csv_file, index=False)
    logger.info(f"CSV saved → {csv_file}")


def save_to_json(articles, json_file=JSON_PATH):
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(articles, f, indent=4)
    logger.info(f"JSON saved → {json_file}")


# ============================================================
#  MAIN INGESTION FUNCTION (EXPORT THIS)
# ============================================================
def run_ingestion():
    logger.info("=== STARTING DATA INGESTION ===")

    rss_feeds = [
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.moneycontrol.com/rss/latestnews.xml",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=AAPL&region=US&lang=en-US",
        "https://www.business-standard.com/rss/latest.rss"
    ]

    api_key = "36b5d90eea8cd28253581f3f536871f5"
    api_urls = [
        f"https://gnews.io/api/v4/top-headlines?apikey={api_key}&topic=business&lang=en&max=10",
        f"https://gnews.io/api/v4/top-headlines?apikey={api_key}&topic=world&lang=en&max=10",
        f"https://gnews.io/api/v4/top-headlines?apikey={api_key}&topic=breaking-news&lang=en&max=10",
        f"https://gnews.io/api/v4/top-headlines?apikey={api_key}&topic=finance&lang=en&max=10"
    ]

    scraper_pages = [
        "https://www.reuters.com/markets/",
        "https://www.reuters.com/business/"
    ]

    # Fetch data
    rss_data = fetch_rss(rss_feeds)
    api_data = fetch_api(api_urls)
    scraper_data = fetch_scraper(scraper_pages)

    final_data = rss_data + api_data + scraper_data

    logger.info(f"TOTAL ARTICLES COLLECTED → {len(final_data)}")

    # Save outputs
    save_to_sqlite(final_data)
    save_to_csv(final_data)
    save_to_json(final_data)

    return final_data


# ============================================================
#  RUN DIRECTLY
# ============================================================
if __name__ == "__main__":
    data = run_ingestion()
    print(f"Total Articles Collected: {len(data)}")


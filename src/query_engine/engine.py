from sentence_transformers import SentenceTransformer
from chromadb import PersistentClient
from pathlib import Path

CHROMA_DIR = Path(r"C:\Users\thang\Desktop\hackthon_project\data\chroma_store")

# Load model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# Load vectorDB
client = PersistentClient(path=str(CHROMA_DIR))
collection = client.get_collection("financial_news")


def semantic_query(query: str, top_k: int = 5):
    query_emb = model.encode(query).tolist()

    results = collection.query(
        query_embeddings=[query_emb],
        n_results=top_k
    )

    return results


def pretty_print(results):
    for idx, doc in enumerate(results["documents"][0]):
        print(f"\n🔹 Result {idx+1}")
        print("-" * 40)
        print("ID:", results["ids"][0][idx])
        print("Distance:", results["distances"][0][idx])
        print("Source:", results["metadatas"][0][idx]["source"])
        print("Published:", results["metadatas"][0][idx]["published_at"])
        print("\nDocument:\n", doc)


if __name__ == "__main__":
    q = "What is the latest news about AAPL?"
    result = semantic_query(q)
    pretty_print(result)

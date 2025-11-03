"""
Document ingestion for RAG (Retrieval-Augmented Generation)
Loads documentation files into Qdrant vector database
"""
import os
import glob
from uuid import uuid4
from typing import List, Dict
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

# Import embeddings module
from embeddings import embed_texts


COLLECTION_NAME = "knowledge_base"


def ensure_collection(client: QdrantClient, dim: int):
    """Create collection if it doesn't exist"""
    try:
        client.get_collection(COLLECTION_NAME)
        print(f"✅ Collection '{COLLECTION_NAME}' exists")
    except:
        print(f"📦 Creating collection '{COLLECTION_NAME}' with dimension {dim}")
        client.recreate_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE)
        )


def chunk_text(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """
    Split text into overlapping chunks
    
    Args:
        text: Input text
        chunk_size: Size of each chunk in characters
        overlap: Overlap between chunks
        
    Returns:
        List of text chunks
    """
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        
        if chunk.strip():
            chunks.append(chunk)
        
        start += (chunk_size - overlap)
    
    return chunks


def iter_documents(docs_dir: str = "./docs") -> List[Dict]:
    """
    Iterate through documentation files and yield chunks
    
    Args:
        docs_dir: Directory containing documentation
        
    Returns:
        List of document chunks with metadata
    """
    documents = []
    
    # Support .md and .txt files
    patterns = [
        os.path.join(docs_dir, "**", "*.md"),
        os.path.join(docs_dir, "**", "*.txt"),
    ]
    
    for pattern in patterns:
        for file_path in glob.glob(pattern, recursive=True):
            print(f"📄 Reading: {file_path}")
            
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    text = f.read().strip()
                
                if not text:
                    continue
                
                # Chunk the document
                chunks = chunk_text(text, chunk_size=1000, overlap=200)
                
                for i, chunk in enumerate(chunks):
                    documents.append({
                        "text": chunk,
                        "source": file_path,
                        "chunk_index": i,
                        "total_chunks": len(chunks),
                    })
                    
            except Exception as e:
                print(f"❌ Error reading {file_path}: {e}")
    
    return documents


def ingest_documents(docs_dir: str = "./docs", batch_size: int = 64):
    """
    Main ingestion function
    
    Args:
        docs_dir: Directory containing documentation
        batch_size: Number of documents to process at once
    """
    # Connect to Qdrant
    qdrant_host = os.getenv("QDRANT_HOST", "localhost")
    qdrant_port = int(os.getenv("QDRANT_PORT", "6333"))
    
    print(f"🔗 Connecting to Qdrant at {qdrant_host}:{qdrant_port}")
    client = QdrantClient(host=qdrant_host, port=qdrant_port)
    
    # Load documents
    print(f"📚 Loading documents from {docs_dir}")
    documents = iter_documents(docs_dir)
    
    if not documents:
        print("⚠️  No documents found!")
        return
    
    print(f"✅ Found {len(documents)} document chunks")
    
    # Process in batches
    total_ingested = 0
    
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        texts = [doc["text"] for doc in batch]
        
        print(f"\n🔄 Processing batch {i//batch_size + 1}/{(len(documents)-1)//batch_size + 1}")
        
        # Generate embeddings
        embeddings, dim = embed_texts(texts)
        
        # Ensure collection exists
        ensure_collection(client, dim)
        
        # Create points
        points = [
            PointStruct(
                id=str(uuid4()),
                vector=embeddings[j],
                payload=batch[j]
            )
            for j in range(len(batch))
        ]
        
        # Upsert to Qdrant
        client.upsert(collection_name=COLLECTION_NAME, points=points)
        
        total_ingested += len(batch)
        print(f"✅ Ingested {total_ingested}/{len(documents)} chunks")
    
    print(f"\n🎉 Ingestion complete! Total chunks: {total_ingested}")
    
    # Show collection info
    collection_info = client.get_collection(COLLECTION_NAME)
    print(f"\n📊 Collection '{COLLECTION_NAME}':")
    print(f"   - Vectors: {collection_info.vectors_count}")
    print(f"   - Dimension: {collection_info.config.params.vectors.size}")


if __name__ == "__main__":
    import sys
    
    docs_dir = sys.argv[1] if len(sys.argv) > 1 else "../docs"
    
    print("="*60)
    print("📚 Document Ingestion for RAG")
    print("="*60)
    
    ingest_documents(docs_dir)


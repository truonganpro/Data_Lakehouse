# -*- coding: utf-8 -*-
"""
Embeddings module - supports multiple providers (Gemini, OpenAI, Sentence-Transformers)
"""
import os
from typing import List, Tuple


def embed_texts(texts: List[str]) -> Tuple[List[List[float]], int]:
    """
    Generate embeddings for texts using configured provider
    
    Args:
        texts: List of text strings to embed
        
    Returns:
        Tuple of (embeddings, dimension)
    """
    provider = os.getenv("EMBEDDING_PROVIDER", "sentence-transformers").lower()
    
    if provider == "gemini":
        return _embed_gemini(texts)
    elif provider == "openai":
        return _embed_openai(texts)
    else:
        return _embed_sbert(texts)


def _embed_gemini(texts: List[str]) -> Tuple[List[List[float]], int]:
    """
    Embed using Google Gemini text-embedding-004 (768 dims)
    """
    import google.generativeai as genai
    
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY not set for Gemini embeddings")
    
    genai.configure(api_key=api_key)
    model = "models/text-embedding-004"
    
    print(f"📦 Embedding with Gemini: {len(texts)} texts")
    
    # Gemini supports batch embedding
    result = genai.embed_content(
        model=model,
        content=texts,
        task_type="retrieval_document"  # for RAG use case
    )
    
    # Extract embeddings from result
    if "embedding" in result:
        # Single text input
        embeddings = [result["embedding"]]
    else:
        # Multiple texts
        embeddings = [emb for emb in result["embeddings"]]
    
    return embeddings, 768


def _embed_openai(texts: List[str]) -> Tuple[List[List[float]], int]:
    """
    Embed using OpenAI text-embedding-3-small (1536 dims)
    """
    import openai
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set for OpenAI embeddings")
    
    openai.api_key = api_key
    
    print(f"📦 Embedding with OpenAI: {len(texts)} texts")
    
    response = openai.embeddings.create(
        model="text-embedding-3-small",
        input=texts
    )
    
    embeddings = [item.embedding for item in response.data]
    return embeddings, 1536


def _embed_sbert(texts: List[str]) -> Tuple[List[List[float]], int]:
    """
    Embed using Sentence-Transformers (local, no API key needed, 768 dims)
    """
    from sentence_transformers import SentenceTransformer
    
    model_name = os.getenv("SBERT_MODEL", "intfloat/multilingual-e5-base")
    
    print(f"📦 Loading model: {model_name}")
    model = SentenceTransformer(model_name)
    
    print(f"📦 Embedding {len(texts)} texts")
    embeddings = model.encode(
        texts,
        normalize_embeddings=True,
        show_progress_bar=True
    )
    
    return embeddings.tolist(), 768


def embed_query(query: str) -> List[float]:
    """
    Generate embedding for a single query string
    
    Args:
        query: Query string
        
    Returns:
        Embedding vector
    """
    provider = os.getenv("EMBEDDING_PROVIDER", "sentence-transformers").lower()
    
    if provider == "gemini":
        import google.generativeai as genai
        
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY not set for Gemini embeddings")
        
        genai.configure(api_key=api_key)
        
        result = genai.embed_content(
            model="models/text-embedding-004",
            content=query,
            task_type="retrieval_query"  # for query (vs document)
        )
        
        return result["embedding"]
    
    elif provider == "openai":
        import openai
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY not set")
        
        openai.api_key = api_key
        
        response = openai.embeddings.create(
            model="text-embedding-3-small",
            input=[query]
        )
        
        return response.data[0].embedding
    
    else:
        from sentence_transformers import SentenceTransformer
        
        model_name = os.getenv("SBERT_MODEL", "intfloat/multilingual-e5-base")
        model = SentenceTransformer(model_name)
        
        return model.encode([query], normalize_embeddings=True)[0].tolist()


if __name__ == "__main__":
    # Test embeddings
    test_texts = [
        "Doanh thu theo tháng gần đây?",
        "Top 10 sản phẩm bán chạy nhất?",
        "Phương thức thanh toán nào phổ biến nhất?"
    ]
    
    print("="*60)
    print("Testing Embeddings Module")
    print("="*60)
    
    embeddings, dim = embed_texts(test_texts)
    
    print(f"\n✅ Generated {len(embeddings)} embeddings")
    print(f"   Dimension: {dim}")
    print(f"   First embedding shape: {len(embeddings[0])}")
    
    # Test single query
    query_emb = embed_query("Test query")
    print(f"\n✅ Query embedding dimension: {len(query_emb)}")


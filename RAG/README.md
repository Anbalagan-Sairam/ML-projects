# RAG (Retrieval-Augmented Generation)

This folder contains a small starter for RAG (Retrieval-Augmented Generation) projects.

What is RAG?
- RAG combines a retriever (search/index over documents) and a generator (large language model).
- The retriever finds relevant context from a knowledge base, and the generator produces answers conditioned on that context.

Typical architecture
1. Ingest documents into a vector store (e.g., FAISS, Milvus, Weaviate, Pinecone).
2. Encode documents with an embedding model (OpenAI, Hugging Face, sentence-transformers).
3. At query time, embed the query, retrieve top-k nearest documents, and pass them with the query to the LLM.

Files
- `rag_quickstart.ipynb` — a starter notebook showing ingestion, indexing, retrieval, and a simple RAG call.
- `README.md` — this overview and guidance.

Quick start (high-level)
1. Prepare documents (PDFs, text, markdown) and place them under `data/`.
2. Create embeddings and store vectors in a vector DB.
3. Implement a retriever (k-NN) and test retrieval quality.
4. Build a prompt that includes retrieved context and call your LLM (API or local) to generate answers.

Example prompt pattern
```
You are an assistant with access to the following context from documents:

<context documents>

Using only the above context, answer the following question:
Q: <user question>
A:
```

Security & best practices
- Do not include sensitive data in your public repo.
- Keep API keys out of code; use environment variables or secrets management.

References
- RAG paper: https://arxiv.org/abs/2005.11401
- ML frameworks: Haystack, LangChain, LlamaIndex

Contributions welcome — add experiments and notebooks here.

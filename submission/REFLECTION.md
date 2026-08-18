# Reflection: Lakehouse Anti-Patterns

Our team is most vulnerable to **Anti-Pattern #3: Data & Vector Lifecycle Decoupling (Stale External Index)**.

When building GenAI and RAG pipelines, our default pattern has been storing raw documents in the Lakehouse while pushing embedding vectors to a decoupled external vector database (e.g., Pinecone/Milvus). As demonstrated in Notebook 7, because the external index lives outside the Lakehouse ACID transaction boundary, data deletions (such as GDPR Right-to-Erasure requests or document updates) fail to propagate atomically. This causes dangerous "ghost retrievals" where search returns deleted or stale context.

To eliminate this vulnerability, our team must transition to:
1. **Delta Change Data Feed (CDF)** to stream transactional mutation events (`_change_type` = `delete`/`update_preimage`) to synchronize external indices in real time.
2. **In-table vector storage** via open formats (e.g., Lance/Delta vector arrays) queried directly by embedded engines (DuckDB), preserving a single source of truth with deterministic versioning.

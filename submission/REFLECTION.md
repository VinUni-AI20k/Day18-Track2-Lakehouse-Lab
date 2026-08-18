# Reflection — Day 18, Lakehouse

**The anti-pattern our data is most at risk of: treating a derived copy as if it were the system of record — specifically, an external vector index that drifts.**

Our RAG stack is exactly the shape NB7 breaks on purpose. Embeddings live in a standalone vector store, synced nightly from the warehouse. That sync is append-oriented: it upserts new documents well and never replays deletes. NB7 reproduced the consequence precisely — after erasing `user_042`, the lakehouse returned **0** of their documents and the stale index still returned **8**.

What makes this more than a data-quality annoyance is Vietnam's PDPL (Law 91/2025) and GDPR Art. 17. Deleting from the system of record does not satisfy a right-to-erasure request while a derived index keeps feeding the deleted content into prompts. We would have believed, wrongly, that we had complied.

Two defensible fixes. Propagate deletions as first-class Change Data Feed events so the index subscribes to deletes instead of guessing. Better, where latency allows, keep the embedding as a column in the governed table — then the vector and the consent flag share one row and one lifecycle, enforced by the table itself.

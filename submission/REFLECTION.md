# Reflection

**Student:** Tran Duc Manh (2A202601567)

The anti-pattern our data is most likely to fall into is treating a derived
system as the source of truth. NB7 made this concrete: after deleting the
requested documents from the lakehouse, the table returned 0 matches, but the
external vector index still returned 8. That is not just a synchronization
bug; it can become a compliance failure when a user asks for erasure. The
lakehouse should own the embeddings and emit change events, while the vector
index should be rebuildable and monitored for drift.

I also noticed how quickly operational details become real problems. The
Bronze table had 200,000 rows but only 190,052 unique request IDs, and NB6
started with 200 small files. Compaction reduced that to 11 files, while
snapshot expiry alone left the old Iceberg Avro files untouched. These results
changed my view of maintenance: it is a paired process, not a single
`VACUUM` button. Finally, pinning NB8 to Delta version 0 reproduced exactly
1,578 steps, which is the kind of evidence I would want before trusting a
training run.

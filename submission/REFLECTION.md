# Reflection — Day 18 Lakehouse Lab

The anti-pattern my team would most likely fall into is the **"Silver skip"** — 
analysts querying raw Bronze directly because "the data is already there."

This looks harmless at small scale: one-off dashboards, ad-hoc SQL, a quick 
join in Python. But it silently couples every downstream consumer to raw 
schema drift, duplicate request_ids, and untyped JSON blobs. When the upstream 
generator changes a key name or adds a nullable column, every dashboard breaks 
at the same time. Debugging becomes archeology: was this null introduced in 
Bronze, or did the dedup rule fail in Silver?

The lab made this concrete: Silver dropped 9,948 rows (4.9 %) via `rn=1` 
deduplication and `model IS NOT NULL` validation. Those rows would have 
inflated latency p95 by ~12 % and corrupted the cost_usd total in Gold. 
Without a mandatory Silver gate, teams would ship those numbers to executives.

Day 18's schema enforcement + `schema_mode="merge"` also protects against the 
"just add the column everywhere" panic. Silver is not bureaucracy; it is the 
**only** place where the team owns the data contract.

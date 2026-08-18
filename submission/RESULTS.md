# Lab Results

All eight notebooks contain successful final assertions in the executed
outputs.

| Notebook | Evidence captured |
|---|---|
| NB1 | `_delta_log` JSON; bad `age` write blocked; `tier` added with merge |
| NB2 | 200 → 55 files; speedup 10.5×; files-pruned ratio 55× |
| NB3 | MERGE 100K; RESTORE; 5 history versions including RESTORE |
| NB4 | Bronze 200,000; Silver 190,052; Gold 8 dates × 3 models = 24 rows |
| NB5 | Hidden pruning 10×; field ID 4 preserved; partition specs 1 and 2 |
| NB6 | Compaction 18×; clustering skip 90%; 3 Delta orphans removed; checkpoint written; Iceberg 20 → 3 snapshots and stranded files swept |
| NB7 | Random-read amplification 200×; int8 5.8× smaller; recall@10 0.904; lifecycle bug reproduced with 8 stale external hits |
| NB8 | Silver partitioned by `agent_version`; pinned replay exact; MCP 5 turns → 1 catalog read; 4 Art. 10 buckets; 1,666 trainable rows |

## Screenshot checklist

- [x] `00-lakehouse-tree.png`: `_lakehouse/` layout and one `_delta_log/*.json`
- [x] `01-nb1-delta-schema.png`
- [x] `02-nb2-optimize.png`
- [x] `03-nb3-time-travel.png`
- [x] `04-nb4-medallion.png`
- [x] `05-nb5-iceberg.png`
- [x] `06-nb6-maintenance.png`
- [x] `07-nb7-vectors.png`
- [x] `08-nb8-provenance.png`

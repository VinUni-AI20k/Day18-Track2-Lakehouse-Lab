# Reflection — Day 18 Lakehouse

**The anti-pattern our data is most at risk of: small files written by a
streaming job that nobody owns the cleanup for.**

We stream LLM-gateway telemetry with a ~5-second micro-batch trigger — exactly
the writer NB6 reproduces. 200 commits left 200 files averaging **51.5 KB**
against a 128–512 MB target. Storage was never the problem; requests were:
**10M GETs/day ≈ $4.00/day** for 10.1 MB that costs **$0.08/day** once
compacted to 11 files. NB5 showed the second tax — at small-file scale
metadata was **278% of table bytes**, so planning gets slower too.

It stays an anti-pattern rather than a tuning knob because "we run VACUUM"
is not ownership. NB6 measured that `deltalake`'s vacuum only reclaims
files the log **tombstoned**: the 5 files our crashed writers left behind
were invisible at every retention setting, and we found them only by
diffing the directory against the log. Iceberg was worse — `expire_snapshots`
went 20 → 3 snapshots and deleted **zero** bytes.

Our fix: lengthen the trigger interval, schedule compaction + checkpoints,
and never run an expiry job without a paired orphan sweep.

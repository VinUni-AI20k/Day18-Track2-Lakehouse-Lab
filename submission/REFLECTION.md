# Lab 18 Reflection

Based on the industrial anti-patterns discussed in Slide §5, our team's data is most at risk of **"The Small File Problem."** 

As demonstrated in Notebook 2, frequent streaming appends or small batch writes (like LLM logs coming in every minute) create hundreds of tiny Parquet files. This forces the query engine to spend more time opening file headers and metadata than actually reading data, leading to slow dashboard performance. 

By implementing a Lakehouse with Delta Lake, we mitigate this risk using the `OPTIMIZE` command to compact small files and `Z-ORDER` to co-locate related data on the disk. This ensures that even as our LLM observability data grows to billions of requests, we maintain high performance and avoid the high costs associated with inefficient data storage and "The Data Swamp."
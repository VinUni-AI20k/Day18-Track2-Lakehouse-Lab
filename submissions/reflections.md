Student name: Thai Minh Kien
Student ID: 2A202600288

# Reflection

Our team would be most at risk of anti-pattern #1: “Đổ tất cả vào S3” (raw JSON, no schema).

The reason is that when teams move fast, it is tempting to land data first and clean it later. That usually creates inconsistent formats, missing fields, and low trust in downstream dashboards or models. Over time, the storage layer becomes a data swamp because nobody is sure which files are reliable or how schemas changed.

This lab showed why Delta Lake helps avoid that problem early. Schema enforcement blocks bad writes, controlled schema evolution allows safe changes like adding a new column, and the Bronze-Silver-Gold structure keeps raw, cleaned, and business-ready data separate. If our team applied those practices from the beginning, debugging would be easier and analytics would be much more reliable.

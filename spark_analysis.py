from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CS236_Spark_Analysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Make the unified dataset available to SQL queries
spark.read.parquet("data/unified.parquet").createOrReplaceTempView("unified")

# 2.1 Weekly request volume
# Dividing the months into their respective categories based on the requirements
weekly = spark.sql("""
    SELECT source, year, month, period, COUNT(*) AS requests
    FROM (
        SELECT source, year(created) AS year, month(created) AS month,
               CASE WHEN day(created) <= 7 THEN 1
                    WHEN day(created) <= 14 THEN 2
                    WHEN day(created) <= 21 THEN 3
                    ELSE 4 END AS period
        FROM unified
    )
    GROUP BY source, year, month, period
""")
weekly.createOrReplaceTempView("weekly")

# Selecting the total requests per week
print("Requests per period, by city")
spark.sql("SELECT * FROM weekly ORDER BY source, year, month, period").show(100)

# Highest and lowest period per city
# The RANK function assigns a ranking vlaue to each row within partitios of a result set
print("Highest-volume period per city")
spark.sql("""
    SELECT source, year, month, period, requests FROM (
        SELECT *, RANK() OVER (PARTITION BY source ORDER BY requests DESC) AS rk FROM weekly
    ) WHERE rk = 1
""").show()

print("Lowest-volume period per city")
spark.sql("""
    SELECT source, year, month, period, requests FROM (
        SELECT *, RANK() OVER (PARTITION BY source ORDER BY requests ASC) AS rk FROM weekly
    ) WHERE rk = 1
""").show()

# The first and last periods of each dataset are incomplete, so they can look  low.
# We can do ita gain only with the completely full periods
spark.sql("""
    SELECT w.source, w.year, w.month, w.period, w.requests
    FROM weekly w
    JOIN (SELECT source, MIN(to_date(created)) AS first_day, MAX(to_date(created)) AS last_day
          FROM unified GROUP BY source) b
      ON w.source = b.source
    WHERE make_date(w.year, w.month, CASE w.period WHEN 1 THEN 1 WHEN 2 THEN 8 WHEN 3 THEN 15 ELSE 22 END) >= b.first_day
      AND CASE w.period
            WHEN 1 THEN make_date(w.year, w.month, 7)
            WHEN 2 THEN make_date(w.year, w.month, 14)
            WHEN 3 THEN make_date(w.year, w.month, 21)
            ELSE last_day(make_date(w.year, w.month, 1)) END <= b.last_day
""").createOrReplaceTempView("full_periods")

print("Highest-volume period per city (complete periods only)")
spark.sql("""
    SELECT source, year, month, period, requests FROM (
        SELECT *, RANK() OVER (PARTITION BY source ORDER BY requests DESC) AS rk FROM full_periods
    ) WHERE rk = 1
""").show()

print("Lowest-volume period per city (complete periods only)")
spark.sql("""
    SELECT source, year, month, period, requests FROM (
        SELECT *, RANK() OVER (PARTITION BY source ORDER BY requests ASC) AS rk FROM full_periods
    ) WHERE rk = 1
""").show()

# 2.1 Service-request hotspots
# For each city: the 10 ZIP codes with the most requests, and for each of those
# ZIPs the 3 most frequent request types with their count and share of the ZIP.
spark.sql("""
    SELECT source, zip, zip_requests, zip_rank FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY source ORDER BY zip_requests DESC, zip) AS zip_rank
        FROM (SELECT source, zip, COUNT(*) AS zip_requests
              FROM unified WHERE zip IS NOT NULL GROUP BY source, zip)
    ) WHERE zip_rank <= 10
""").createOrReplaceTempView("top_zips")

print("Top 10 ZIP codes per city")
spark.sql("SELECT * FROM top_zips ORDER BY source, zip_rank").show(20)

# Use the original request type: the standardized one puts about 95% of NYC rows in 'Other'
print("Top 3 request types in each top-10 ZIP")
spark.sql("""
    SELECT source, zip_rank, zip, zip_requests, type_rank, request_type_original, type_requests,
           ROUND(type_requests / zip_requests * 100, 2) AS pct_of_zip
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY source, zip
                                     ORDER BY type_requests DESC, request_type_original) AS type_rank
        FROM (
            SELECT u.source, u.zip, t.zip_rank, t.zip_requests, u.request_type_original,
                   COUNT(*) AS type_requests
            FROM unified u
            JOIN top_zips t ON u.source = t.source AND u.zip = t.zip
            GROUP BY u.source, u.zip, t.zip_rank, t.zip_requests, u.request_type_original
        )
    ) WHERE type_rank <= 3
    ORDER BY source, zip_rank, type_rank
""").show(60, truncate=False)

# Bonus 1: Resolution performance
# Valid completed = status 'closed' AND the Phase 1 validity flag (non-null, non-negative resolution_hours)
spark.sql("""
    SELECT * FROM unified
    WHERE status = 'closed' AND is_valid_resolution = true
""").createOrReplaceTempView("valid_resolved")

print("Average and median resolution time (hours) per city")
spark.sql("""
    SELECT source,
           COUNT(*) AS completed_requests,
           ROUND(AVG(resolution_hours), 2) AS avg_resolution_hours,
           ROUND(percentile(resolution_hours, 0.5), 2) AS median_resolution_hours
    FROM valid_resolved
    GROUP BY source
    ORDER BY source
""").show()

# request_type_original, same reasoning as the hotspot query
print("Top 5 request types by average resolution time, per city")
spark.sql("""
    SELECT source, type_rank, request_type_original, requests, avg_resolution_hours
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY source
                                     ORDER BY avg_resolution_hours DESC, request_type_original) AS type_rank
        FROM (
            SELECT source, request_type_original, COUNT(*) AS requests,
                   ROUND(AVG(resolution_hours), 2) AS avg_resolution_hours
            FROM valid_resolved
            GROUP BY source, request_type_original
        )
    ) WHERE type_rank <= 5
    ORDER BY source, type_rank
""").show(20, truncate=False)

# Bonus 2: Status and submission channels
def share_by_city(col, label):
    spark.sql(f"""
        SELECT source, {col}, COUNT(*) AS requests,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY source), 2) AS pct
        FROM unified
        GROUP BY source, {col}
    """).createOrReplaceTempView(f"{label}_dist")

    print(f"Requests per {label}, by city")
    spark.sql(f"SELECT * FROM {label}_dist ORDER BY source, requests DESC").show(50, truncate=False)

    print(f"Most common {label} per city")
    spark.sql(f"""
        SELECT source, {col}, requests, pct FROM (
            SELECT *, RANK() OVER (PARTITION BY source ORDER BY requests DESC) AS rk
            FROM {label}_dist
        ) WHERE rk = 1
    """).show(truncate=False)

share_by_city("status", "status")
share_by_city("channel", "channel")
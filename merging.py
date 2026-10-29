from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("CS236_Merging").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Cleaned datasets saved at the end of data_cleaning.ipynb
nyc = spark.read.parquet("new_data/nyc_cleaned.parquet")
la = spark.read.parquet("new_data/la_cleaned.parquet")

# Making sure the counts for each dataset are valid
assert nyc.count() == la.count() == 800000

# Keeping track of the columns we merge
unified_cols = {}

# Function add column to the unified column, takes in the two columns from each dataset
def add_column(name, nyc_expr, la_expr):
    unified_cols[name] = (nyc_expr.alias(name), la_expr.alias(name))

# Value before cleaning if the cleaning step created a _original columnm, otherwise its just the column itself
def orig(df, c):
    return F.col(c + "_original") if (c + "_original") in df.columns else F.col(c)

# Map a string column through a dict (trimmed, lower-case keys). Nulls stay null
def map_values(col, mapping, default=None):
    m = F.create_map(*[x for k, v in mapping.items() for x in (F.lit(k), F.lit(v))])
    mapped = m[F.lower(F.trim(col))]
    if default is not None:
        mapped = F.coalesce(mapped, F.lit(default))
    return F.when(col.isNull(), F.lit(None).cast("string")).otherwise(mapped)


# Create a source column to distinguisrh between cities
add_column("source", F.lit("NYC"), F.lit("LA"))

# `id` from NYC's `unique_key` and LA's `srnumber`
# We do source + id in case they have the same id
add_column("id", F.col("unique_key").cast("string"), F.col("srnumber").cast("string"))

# `created` from NYC's `created_date` and LA's `createddate`
add_column("created", F.col("created_date"), F.col("createddate"))

# `closed` from NYC's `closed_date` and LA's `closeddate`
add_column("closed", F.col("closed_date"), F.col("closeddate"))

# `updated_date` from NYC's `resolution_action_updated_date` and LA's `updated_date`
add_column("updated_date", F.col("resolution_action_updated_date"), F.col("updateddate"))

# `status` from NYC's `status` and LA's `status`
# Using the mapping outlied in the project report
status_map = {
    "closed": "closed",
    "open": "open",
    "pending": "open",
    "in progress": "open",
    "assigned": "open",
    "started": "open",
    "cancelled": "cancelled",
    "referred out": "other",
    "forward": "other",
}

# Map the values to the mapping above
add_column(
    "status",
    map_values(F.col("cleaned_status"), status_map, default="other"),
    map_values(F.col("cleaned_status"), status_map, default="other"),
)
add_column("status_detail", F.col("cleaned_status"), F.col("cleaned_status"))

# `request_type` from NYC's `complaint_type` and LA's `requesttype`
# Using mapping from the project report
nyc_type_map = {
    "graffiti": "Graffiti Removal",
    "encampment": "Homeless Encampment",
    "dead animal": "Dead Animal Removal",
    "street light condition": "Streetlight Issue",
    "illegal dumping": "Illegal Dumping Pickup",
    "water conservation": "Report Water Waste",
}
la_type_map = {
    "bulky items": "Bulky Items",
    "dead animal removal": "Dead Animal Removal",
    "electronic waste": "Electronic Waste",
    "feedback": "Feedback",
    "graffiti removal": "Graffiti Removal",
    "homeless encampment": "Homeless Encampment",
    "illegal dumping pickup": "Illegal Dumping Pickup",
    "metal/household appliances": "Metal/Household Appliances",
    "multiple streetlight issue": "Streetlight Issue",
    "single streetlight issue": "Streetlight Issue",
    "report water waste": "Report Water Waste",
    "other": "Other",
}
add_column(
    "request_type",
    map_values(F.col("complaint_type"), nyc_type_map, default="Other"),
    map_values(F.col("requesttype"), la_type_map, default="Other"),
)
add_column("request_type_original", orig(nyc, "complaint_type"), orig(la, "requesttype"))

# `channel` from NYC's `open_data_channel_type` and LA's `request_source`
nyc_channel_map = {"phone": "Phone", "online": "Online", "mobile": "Mobile", "other": "Other"}
la_channel_map = {
    "call": "Phone",
    "voicemail": "Phone",
    "queue initiated customer call": "Phone",
    "mobile app": "Mobile",
    "self service": "Online",
    "email": "Online",
    "web form": "Online",
    "social": "Online",
    "driver self report": "Other",
    "council's office": "Other",
    "fax": "Other",
    "letter": "Other",
    "walk-in": "Other",
    "city attorney": "Other",
    "radio": "Other",
}
add_column(
    "channel",
    map_values(F.col("open_data_channel_type"), nyc_channel_map, default="Other"),
    map_values(F.col("requestsource"), la_channel_map, default="Other"),
)
add_column("channel_original", orig(nyc, "open_data_channel_type"), F.col("requestsource"))

# `zip` from NYC's `incident_zip` and LA's `zipcode`
add_column("zip", F.col("incident_zip").cast("string"), F.col("zipcode").cast("string"))

# `agency` from NYC's `agency` and LA's `owner`
add_column("agency", F.col("agency"), F.col("owner"))

# `address` from NYC's `incident_address` and LA's `address`
add_column("address", F.col("incident_address"), F.col("address"))

# `street_name` from NYC's `street_name` and LA's `streetname`
add_column("street_name", F.upper(F.col("street_name")), F.upper(F.col("streetname")))

# `police_precinct` from NYC's `police_precinct` and LA's `policeprecinct`
add_column("police_precinct", F.col("police_precinct"), F.col("policeprecinct"))

# `latitude` from NYC's `latitude` and LA's `latitude`
add_column("latitude", F.col("latitude").cast("double"), F.col("latitude").cast("double"))

# `longitude` from NYC's `longitude` and LA's `longitude`
add_column("longitude", F.col("longitude").cast("double"), F.col("longitude").cast("double"))

# `location` from NYC's `location` and LA's `location`
add_column("location", F.col("location").cast("string"), F.col("location").cast("string"))

# Compare the formats before moving on
nyc.select("location").where(F.col("location").isNotNull()).show(3, truncate=False)
la.select("location").where(F.col("location").isNotNull()).show(3, truncate=False)

# Derived attributes
add_column("resolution_hours", F.col("resolution_hours"), F.col("resolution_hours"))
add_column("is_valid_resolution", F.col("is_valid_resolution"), F.col("is_valid_resolution"))
add_column(
    "is_suspected_duplicate",
    F.coalesce(F.col("is_suspected_duplicate"), F.lit(False)),
    F.lit(False),
)

# Union and unique identifier
order = [
    "source",
    "id",
    "created",
    "closed",
    "updated_date",
    "status",
    "status_detail",
    "request_type",
    "request_type_original",
    "channel",
    "channel_original",
    "zip",
    "agency",
    "address",
    "street_name",
    "police_precinct",
    "latitude",
    "longitude",
    "location",
    "resolution_hours",
    "is_valid_resolution",
    "is_suspected_duplicate",
]
assert set(order) == set(unified_cols), set(order) ^ set(unified_cols)

nyc_u = nyc.select(*[unified_cols[c][0] for c in order])
la_u = la.select(*[unified_cols[c][1] for c in order])

assert nyc_u.dtypes == la_u.dtypes, (nyc_u.dtypes, la_u.dtypes)

unified = nyc_u.unionByName(la_u)
unified = unified.withColumn("uid", F.concat_ws("_", F.col("source"), F.col("id")))
unified = unified.select("uid", *order)
unified.printSchema()

# Validate and save
unified = unified.persist(StorageLevel.DISK_ONLY)

# Record counts: 800,000 per city
counts = {r["source"]: r["count"] for r in unified.groupBy("source").count().collect()}
assert counts == {"NYC": 800000, "LA": 800000}, counts
assert unified.count() == 1600000

# Identifier uniqueness: uid overall, and id inside each city
assert unified.select("uid").distinct().count() == 1600000
assert (
    unified.groupBy("source")
    .agg(F.countDistinct("id").alias("d"))
    .filter(F.col("d") != 800000)
    .count()
    == 0
)
assert unified.filter(F.col("uid").isNull() | F.col("created").isNull()).count() == 0

# Standardized categories contain only the agreed values
assert {r[0] for r in unified.select("status").distinct().collect()} <= {
    "closed",
    "open",
    "cancelled",
    "other",
    None,
}
assert {r[0] for r in unified.select("channel").distinct().collect()} <= {
    "Phone",
    "Online",
    "Mobile",
    "Other",
    None,
}
unified.groupBy("source", "status").count().orderBy("source", F.desc("count")).show()
unified.groupBy("source", "channel").count().orderBy("source", F.desc("count")).show()
unified.groupBy("source", "request_type").count().orderBy("source", F.desc("count")).show(
    30, truncate=False
)

# Derived attributes: valid resolution times are non-negative and exist exactly when flagged valid
assert (
    unified.filter(
        (F.col("resolution_hours") < 0)
        | (F.col("is_valid_resolution") != F.col("resolution_hours").isNotNull())
    ).count()
    == 0
)
assert unified.filter(F.col("closed") < F.col("created")).count() == 0

# Missing-value summary per column
unified.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in unified.columns]
).show(vertical=True)

#  Save and verify Parquet
unified.write.mode("overwrite").parquet("new_data/unified.parquet")
saved_parquet = spark.read.parquet("new_data/unified.parquet")
assert saved_parquet.count() == 1600000 and sorted(saved_parquet.dtypes) == sorted(unified.dtypes)
print("Unified dataset successfully validated and saved to Parquet.")

# Save and verify CSV (coalesced into a single CSV file)
unified.coalesce(1).write.mode("overwrite").option("header", "true").csv("new_data/unified.csv")
saved_csv = spark.read.csv("new_data/unified.csv", header=True, inferSchema=True)
assert saved_csv.count() == 1600000
print("Unified dataset successfully validated and saved to CSV.")

# Print the head of the saved CSV
print("\n--- Head of Saved CSV (First 5 Rows) ---")
saved_csv.show(5, truncate=False)

# 4. Clear cache from RAM
unified.unpersist()
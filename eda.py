from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, to_timestamp, desc
import os

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("CS236_Data_Preparation") \
    .getOrCreate()

# Get rid of some of the warning terminal messages
spark.sparkContext.setLogLevel("WARN")


# Load the datasets
script_dir = os.path.dirname(os.path.abspath('eda.py'))

nyc_df = spark.read.csv(os.path.join(script_dir, '..', 'Data', 'nyc_311_800k.csv'), header=True, inferSchema=True)
la_df = spark.read.csv(os.path.join(script_dir, '..', 'Data', 'la_311_800k.csv'), header=True, inferSchema=True)

# Extract schemas as formatted strings using the underlying JVM object
nyc_schema = nyc_df._jdf.schema().treeString()
la_schema = la_df._jdf.schema().treeString()

# Write the schemas to a text file
# with open("dataset_schemas.txt", "w") as file:
#     file.write("=== NYC 311 Schema ===\n")
#     file.write(nyc_schema)
#     file.write("\n=== LA MyLA311 Schema ===\n")
#     file.write(la_schema)

# print("Schemas successfully written to dataset_schemas.txt")~


nyc_head = nyc_df._jdf.showString(15, 20, False)
la_head = la_df._jdf.showString(15, 20, False)

# Write the rows to a text file
# with open("head.txt", "w") as file:
#     file.write("NYC 311 First 15 Rows\n")
#     file.write(nyc_head)
#     file.write("\nLA MyLA311 First 15 Rows\n")
#     file.write(la_head)

from pyspark.sql import functions as F

# ID uniqueness: nulls, distinct count, and format
print("NYC IDs null:", nyc_df.filter(F.col("unique_key").isNull()).count())
print("NYC distinct IDs:", nyc_df.select("unique_key").distinct().count())
print("NYC IDs not 8 digits:", nyc_df.filter(~F.col("unique_key").cast("string").rlike(r"^\d{8}$")).count())

print("LA IDs null:", la_df.filter(F.col("srnumber").isNull()).count())
print("LA distinct IDs:", la_df.select("srnumber").distinct().count())
print("LA IDs not matching 1-#########:", la_df.filter(~F.col("srnumber").rlike(r"^1-\d+$")).count())

# Duplicates ignoring the ID
print("NYC distinct rows ignoring ID:", nyc_df.drop("unique_key").dropDuplicates().count())
print("LA distinct rows ignoring ID:", la_df.drop("srnumber").dropDuplicates().count())


# grouping by complaint type to spot any patterns (only doing NYC since LA has no dupes)
# Getting list of columns tha  are not the unique key
cols = [c for c in nyc_df.columns if c != "unique_key"]
# Filter for duplicates where all columns match except for unique ID
dups = nyc_df.groupBy(*cols).count().filter("count > 1")
print("groups:", dups.count())
# Grouping by agency, seems to have the most skewed distribution across all categories
dups.groupBy("agency", "complaint_type").sum("count").orderBy(F.desc("sum(count)")).show(10, truncate=False)

# Repeated key, excluding null addresses
# Lose match for created_data, complaint_type, and adress
print(nyc_df.filter(F.col("incident_address").isNotNull())
      .groupBy("created_date", "complaint_type", "incident_address").count()
      .filter("count > 1").count())
# Same as above but for LA
print(la_df.filter(F.col("address").isNotNull())
      .groupBy("createddate", "requesttype", "address").count()
      .filter("count > 1").count())


# Dimensions
print("\nNYC rows:", nyc_df.count(), "columns:", len(nyc_df.columns))
print("LA rows:", la_df.count(), "columns:", len(la_df.columns))

# Parsed dates and resolution time in hours (used by the sections below)

# Create a timestamp df for both created and closed
nyc_ts = nyc_df.withColumn("created_ts", to_timestamp("created_date")).withColumn("closed_ts", to_timestamp("closed_date"))
# Casting the timestamps as longs transforms them into seconds.
nyc_ts = nyc_ts.withColumn("resolution_hours", (nyc_ts["closed_ts"].cast("long") - nyc_ts["created_ts"].cast("long")) / 3600)

# Same process as for NYC
la_ts = la_df.withColumn("created_ts", to_timestamp("createddate")).withColumn("closed_ts", to_timestamp("closeddate"))
la_ts = la_ts.withColumn("resolution_hours", (la_ts["closed_ts"].cast("long") - la_ts["created_ts"].cast("long")) / 3600)

# Date ranges shown using selection with expressions so we can calculate min and max
# selectExpr allows you to select multiple columns from a pyspark dataset and apply functions such as min and max
print("\nNYC: Date Ranges")
nyc_ts.selectExpr("min(created_ts)", "max(created_ts)", "min(closed_ts)", "max(closed_ts)").show(truncate=False)
print("\nLA: Date Ranges")
la_ts.selectExpr("min(created_ts)", "max(created_ts)", "min(closed_ts)", "max(closed_ts)").show(truncate=False)

# Fully duplicated rows
# df.dropDuplicates().count() simply counts the number of non-duplicate rows in df
print("\nNYC: distinct rows", nyc_df.dropDuplicates().count(), "out of", nyc_df.count())
print("LA: distinct rows", la_df.dropDuplicates().count(), "out of", la_df.count())

# Date problems
# Check if any of the created dates are null
print("\nNYC: created date null after parsing:", nyc_ts.filter("created_ts IS NULL").count())
# Check if closed data is before created date
print("NYC: closed before created:", nyc_ts.filter("closed_ts < created_ts").count())
# Check if created data is after the current date
print("NYC: created in the future:", nyc_ts.filter("created_ts > current_timestamp()").count())

# Check if any of the created dates are null
print("\nLA: created date null after parsing:", la_ts.filter("created_ts IS NULL").count())
# Check if closed data is before created date
print("LA: closed before created:", la_ts.filter("closed_ts < created_ts").count())
# Check if created data is after the current date
print("LA: created in the future:", la_ts.filter("created_ts > current_timestamp()").count())

# Check for zipcodes that are not exactly 5 digits long
print("\nNYC: zips that are not 5 digits")
# RLIKE '^[0-9]{5}$') checks if the string had 5 digits from 0-9
nyc_bad_zip = nyc_df.filter("incident_zip IS NOT NULL AND NOT (CAST(incident_zip AS STRING) RLIKE '^[0-9]{5}$')")
print(nyc_bad_zip.count())
# Gets the top 10 most common wrong zipcodes
nyc_bad_zip.groupBy("incident_zip").count().orderBy(desc("count")).show(10)

print("\nLA: zips that are not 5 digits")
# RLIKE '^[0-9]{5}$') checks if the string had 5 digits from 0-9
la_bad_zip = la_df.filter("zipcode IS NOT NULL AND NOT (CAST(zipcode AS STRING) RLIKE '^[0-9]{5}$')")
print(la_bad_zip.count())
# Gets the top 10 most common wrong zipcodes
la_bad_zip.groupBy("zipcode").count().orderBy(desc("count")).show(10)

# Placeholder values that actually mean missing
# we can denote '', 'unspecified', 'n/a', 'unknown', and 'none' as values that actually just mean NA
print("\nNYC: placeholder values")
for c in ["status", "open_data_channel_type", "complaint_type", "agency"]:
    # we do trim to remove any whitespace and lower to convert to lowercase
    n = nyc_df.filter(f"lower(trim({c})) IN ('', 'unspecified', 'n/a', 'unknown', 'none')").count()
    print(f"{c}: {n}")

print("\nLA: placeholder values")
for c in ["status", "requestsource", "requesttype", "owner"]:
    # we do trim to remove any whitespace and lower to convert to lowercase
    n = la_df.filter(f"lower(trim({c})) IN ('', 'unspecified', 'n/a', 'unknown', 'none')").count()
    print(f"{c}: {n}")

# Latitude / longitude summary
# We can identify any zeroes here, or any values that are far from their repective cities
print("\nNYC: latitude/longitude")
nyc_df.select("latitude", "longitude").describe().show()
print("\nLA: latitude/longitude")
la_df.select("latitude", "longitude").describe().show()


# Print duplicate values for unique_key in NYC 311 Schema and srnumber in LA 311 Schema to check if they act as unique keys

# Check for duplicate unique_key values in NYC 311
print("NYC 311: Duplicate 'unique_key' values")
# we do count > 1 to check if there are more than one in ay single group
nyc_duplicates = nyc_df.groupBy("unique_key").count().filter("count > 1")
nyc_duplicates.show()

# Check for duplicate srnumber values in LA MyLA311
print("LA MyLA311: Duplicate 'srnumber' values")
# we do count > 1 to check if there are more than one in ay single group
la_duplicates = la_df.groupBy("srnumber").count().filter("count > 1")
la_duplicates.show()

# Create a % value of NA in each column for both datasets, and a vizualization for it

nyc_count = nyc_df.count()
print("NYC 311 Missing Values")
# percentage of nulls in all columns
for col in nyc_df.columns:
    nulls = nyc_df.filter(nyc_df[col].isNull()).count()
    pct = (nulls / nyc_count) * 100
    # Star vizualization of percentage
    print(f"{col}: {pct:.1f}% | {'*' * int(pct / 5)}")

la_count = la_df.count()
print("\nLA 311 Missing Values")
for col in la_df.columns:
    nulls = la_df.filter(la_df[col].isNull()).count()
    pct = (nulls / la_count) * 100
    # Star vizualization of percentage
    print(f"{col}: {pct:.1f}% | {'*' * int(pct / 5)}")


# CHeck for any inconsistencies across the columns in both datasets

nyc_dict = dict(nyc_df.dtypes)
la_dict = dict(la_df.dtypes)

# Use set operators for unique columns (doesnt consider columns that are
# theoretically the same but have different names)
print("\nColumns ONLY in NYC")
print(set(nyc_dict.keys()) - set(la_dict.keys()))

print("\nColumns ONLY in LA")
print(set(la_dict.keys()) - set(nyc_dict.keys()))



# Basic ascii bar chart function
def ascii_bar_chart(df, column_name, top_n=15):
    print(f"\n--- {column_name} ---")
    data = df.groupBy(column_name).count().orderBy(desc("count")).limit(top_n).collect()
    if not data:
        return
    max_count = data[0]["count"]
    for row in data:
        name = str(row[column_name])[:25]
        count = row["count"]
        bar = "█" * int((count / max_count) * 40) if max_count > 0 else ""
        print(f"{name.ljust(26)} | {count:<7} | {bar}")


# Distributions and visualisations

# NYC and LA
# Distribution of closed date - open date
# Instead of doing it the first way with cast(long), you can also do datediff
nyc_time_df = nyc_df.withColumn("resolution_days", datediff(to_timestamp("closed_date"), to_timestamp("created_date")))
la_time_df = la_df.withColumn("resolution_days", datediff(to_timestamp("closeddate"), to_timestamp("createddate")))

# Get the count for each resolution day, order by the count desc, and show the top 10.
print("\nNYC: Resolution Time (Days)")
nyc_time_df.groupBy("resolution_days").count().orderBy(desc("count")).show(10)

print("\nLA: Resolution Time (Days)")
la_time_df.groupBy("resolution_days").count().orderBy(desc("count")).show(10)


# NYC
# Bar Graph of borough
print("\nNYC: Bar Graph of Borough")
ascii_bar_chart(nyc_df, "borough")


# LA
# Bar graph of Neighborhood name
print("\nLA: Bar Graph of Neighborhood name")
ascii_bar_chart(la_df, "ncname")


# NYC: complaint_type, status, open_data_channel_type
print("\nNYC: complaint_type")
ascii_bar_chart(nyc_df, "complaint_type", 10)
print("\nNYC: status")
ascii_bar_chart(nyc_df, "status", 10)
print("\nNYC: open_data_channel_type")
ascii_bar_chart(nyc_df, "open_data_channel_type", 10)


# LA: requesttype, status, requestsource
print("\nLA: requesttype")
ascii_bar_chart(la_df, "requesttype", 10)
print("\nLA: status")
ascii_bar_chart(la_df, "status", 10)
print("\nLA: requestsource")
ascii_bar_chart(la_df, "requestsource", 10)

# All values of the categorical columns, to compare vocabularies
print("\nNYC: status (all)")
ascii_bar_chart(nyc_df, "status", 30)
print("\nLA: status (all)")
ascii_bar_chart(la_df, "status", 30)

print("\nNYC: channel (all)")
ascii_bar_chart(nyc_df, "open_data_channel_type", 30)
print("\nLA: channel (all)")
ascii_bar_chart(la_df, "requestsource", 30)

print("\nNYC: agency")
ascii_bar_chart(nyc_df, "agency", 15)
print("\nLA: agency (owner)")
ascii_bar_chart(la_df, "owner", 15)

# NYC and LA: Zip code
print("\nNYC: Top Zip Codes")
ascii_bar_chart(nyc_df, "incident_zip", 10)

print("\nLA: Top Zip Codes")
ascii_bar_chart(la_df, "zipcode", 10)

# Requests per year, month, hour
print("\nNYC: Requests per Year")
nyc_ts.selectExpr("year(created_ts) AS year").groupBy("year").count().orderBy("year").show(20)
print("\nNYC: Requests per Month")
nyc_ts.selectExpr("month(created_ts) AS month").groupBy("month").count().orderBy("month").show(12)
print("\nNYC: Requests per Hour")
nyc_ts.selectExpr("hour(created_ts) AS hour").groupBy("hour").count().orderBy("hour").show(24)

print("\nLA: Requests per Year")
la_ts.selectExpr("year(created_ts) AS year").groupBy("year").count().orderBy("year").show(20)
print("\nLA: Requests per Month")
la_ts.selectExpr("month(created_ts) AS month").groupBy("month").count().orderBy("month").show(12)
print("\nLA: Requests per Hour")
la_ts.selectExpr("hour(created_ts) AS hour").groupBy("hour").count().orderBy("hour").show(24)

# Resolution time in hours (only rows where closed >= created)
print("\nNYC: Resolution Time (Hours)")
print("Negative resolution times:", nyc_ts.filter("resolution_hours < 0").count())
nyc_valid = nyc_ts.filter("resolution_hours >= 0")
nyc_valid.select("resolution_hours").describe().show()
print("Percentiles 25/50/75/95/99:", nyc_valid.approxQuantile("resolution_hours", [0.25, 0.5, 0.75, 0.95, 0.99], 0.01))

print("\nLA: Resolution Time (Hours)")
print("Negative resolution times:", la_ts.filter("resolution_hours < 0").count())
la_valid = la_ts.filter("resolution_hours >= 0")
la_valid.select("resolution_hours").describe().show()
print("Percentiles 25/50/75/95/99:", la_valid.approxQuantile("resolution_hours", [0.25, 0.5, 0.75, 0.95, 0.99], 0.01))


# Manual mapping of matching concepts (column names differ between the datasets)
# Matching was done through logical pairings.
print("\nConcept mapping")
print(f"{'concept':<14}{'NYC column':<32}{'NYC type':<12}{'LA column':<20}{'LA type':<12}")
pairs = [("id", "unique_key", "srnumber"),
         ("created", "created_date", "createddate"),
         ("closed", "closed_date", "closeddate"),
         ("updateddate", "resolution_action_updated_date", "updateddate"),
         ("status", "status", "status"),
         ("request type", "complaint_type", "requesttype"),
         ("channel", "open_data_channel_type", "requestsource"),
         ("zip", "incident_zip", "zipcode"),
         ("agency", "agency", "owner"),
         ("adress", "incident_address", "address"),
         ("streetname", "street_name", "streetname"),
         ("policeprecint", "police_precinct", "policeprecinct"),
         ("requestedby", "open_data_channel_type", "requestsource"),
         ("latitude", "latitude", "latitude"),
         ("longitude", "longitude", "longitude"),
         ("location", "location", "location")]
for concept, n, l in pairs:
    print(f"{concept:<14}{n:<32}{nyc_dict.get(n, '?'):<12}{l:<20}{la_dict.get(l, '?'):<12}")

# Columns not in the mapping above
print("\nNYC columns not mapped:", sorted(set(nyc_df.columns) - set(p[1] for p in pairs)))
print("LA columns not mapped:", sorted(set(la_df.columns) - set(p[2] for p in pairs)))


la_cat_cols = [
    "actiontaken", "owner", "requesttype", "status", "requestsource",
    "createdbyuserorganization", "mobileos", "anonymous", "assignto",
    "addressverified", "approximateaddress", "direction", "suffix",
    "tbmcolumn", "tbmrow", "apc", "cd", "cdmember", "nc", "ncname",
    "policeprecinct"
]

nyc_cat_cols = [
    "agency", "agency_name", "status", "borough", "open_data_channel_type",
    "address_type", "facility_type", "park_facility_name", "park_borough",
    "vehicle_type", "taxi_company_borough", "council_district",
    "police_precinct", "community_board", "bridge_highway_name", "complaint_type", "city", "location_type"
]

# Get all the possible values for categorical variables in both
print("LA MyLA311")
for c in la_cat_cols:
    # Get distinct values in the categorical variables above
    vals = sorted([r[0] for r in la_df.select(c).distinct().collect()],
                  key=lambda x: (x is None, str(x)))
    print(f"\n{c} ({len(vals)} distinct):\n{vals}")

print("\nNYC 311")
for c in nyc_cat_cols:
    # Get distinct values in the categorical variables above
    vals = sorted([r[0] for r in nyc_df.select(c).distinct().collect()],
                  key=lambda x: (x is None, str(x)))
    print(f"\n{c} ({len(vals)} distinct):\n{vals}")




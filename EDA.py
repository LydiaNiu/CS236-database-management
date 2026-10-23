from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, to_timestamp, desc

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("CS236_Data_Preparation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Load the datasets
nyc_df = spark.read.csv("nyc_311_800k.csv", header=True, inferSchema=True)
la_df = spark.read.csv("la_311_800k.csv", header=True, inferSchema=True)

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

# Show the first few rows in the console
# nyc_df.show(5)
# la_df.show(5)

nyc_head = nyc_df._jdf.showString(15, 20, False)
la_head = la_df._jdf.showString(15, 20, False)

# Write the rows to a text file
with open("head.txt", "w") as file:
    file.write("=== NYC 311 First 15 Rows ===\n")
    file.write(nyc_head)
    file.write("\n=== LA MyLA311 First 15 Rows ===\n")
    file.write(la_head)

# Print duplicate values for unique_key in NYC 311 Schema and srnumber in LA 311 Schema to check if they act as unique keys

# Check for duplicate unique_key values in NYC 311
print("=== NYC 311: Duplicate 'unique_key' values ===")
nyc_duplicates = nyc_df.groupBy("unique_key").count().filter("count > 1")
nyc_duplicates.show()

# Check for duplicate srnumber values in LA MyLA311
print("=== LA MyLA311: Duplicate 'srnumber' values ===")
la_duplicates = la_df.groupBy("srnumber").count().filter("count > 1")
la_duplicates.show()

# Create a % value of NA in each column for both datasets, and some sort of vizualization for it

nyc_count = nyc_df.count()
print("--- NYC 311 Missing Values ---")
# percemtage of nulls in all columns
for col in nyc_df.columns:
    nulls = nyc_df.filter(nyc_df[col].isNull()).count()
    pct = (nulls / nyc_count) * 100
    # Star vizualization of percentage
    print(f"{col}: {pct:.1f}% | {'*' * int(pct / 5)}")

la_count = la_df.count()
print("\n--- LA 311 Missing Values ---")
for col in la_df.columns:
    nulls = la_df.filter(la_df[col].isNull()).count()
    pct = (nulls / la_count) * 100
    # Star vizualization of percentage
    print(f"{col}: {pct:.1f}% | {'*' * int(pct / 5)}")


# CHeck for any inconsistencies across the columns in both datasets

nyc_dict = dict(nyc_df.dtypes)
la_dict = dict(la_df.dtypes)

print("\n--- Columns ONLY in NYC ---")
print(set(nyc_dict.keys()) - set(la_dict.keys()))

print("\n--- Columns ONLY in LA ---")
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


# Distributions and vizualizations

# NYC and LA
# Distribution of closed date - open date
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



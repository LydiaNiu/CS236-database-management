


import os

# Set JAVA_HOME using your exact Microsoft OpenJDK 11 path BEFORE importing PySpark
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/microsoft-11.jdk/Contents/Home"

# Import PySpark tools
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F

# Start Spark
spark = SparkSession.builder.appName("CS236_Data_Cleaning").getOrCreate()

# Reduce warning messages
spark.sparkContext.setLogLevel("ERROR")

script_dir = os.path.dirname(os.path.abspath('data_cleaning.ipynb'))

nyc_df = spark.read.csv(os.path.join(script_dir, '..', 'Data', 'nyc_311_800k.csv'), header=True, inferSchema=True)
la_df = spark.read.csv(os.path.join(script_dir, '..', 'Data', 'la_311_800k.csv'), header=True, inferSchema=True)

# Convert NYC dates to timestamps
nyc_clean = nyc_df.withColumn("created_ts", F.to_timestamp("created_date")).withColumn("closed_date", F.to_timestamp("closed_date"))

# Convert LA dates to timestamps
la_clean = la_df.withColumn("created_ts", F.to_timestamp("createddate")).withColumn("closeddate", F.to_timestamp("closeddate"))

# Keep source status unchanged and create status columns
nyc_clean = nyc_clean.withColumn("cleaned_status", F.col("status"))
la_clean = la_clean.withColumn("cleaned_status", F.col("status"))


# ## 1. NYC and LA: closed dates, status, and resolution time
# 
# From EDA, we found out that there exists conflicts between the closed_date to start_date and status. Following are the checks of the count in each condition.
# 
# `status` preserves the source value; `cleaned_status` contains the final value after cleaning. All status changes and placeholder replacements apply only to `cleaned_status`.

# In[25]:


# Flag NYC conditions and exclude cancellation, referral, and forwarding statuses
nyc_clean = (nyc_clean
    .withColumn("closed_without_date", F.coalesce((F.col("status") == "Closed") & F.col("closed_date").isNull(), F.lit(False)))
    .withColumn("closed_before_created", F.coalesce(F.col("closed_date") < F.col("created_ts"), F.lit(False)))
    .withColumn("status_changed_to_closed", F.col("closed_date").isNotNull() & ~F.col("closed_before_created") & ~F.coalesce(F.col("status").isin("Closed", "Cancelled", "Referred Out", "Forward"), F.lit(False))))

# Flag LA conditions and exclude cancellation, referral, and forwarding statuses
la_clean = (la_clean
    .withColumn("closed_without_date", F.coalesce((F.col("status") == "Closed") & F.col("closeddate").isNull(), F.lit(False)))
    .withColumn("closed_before_created", F.coalesce(F.col("closeddate") < F.col("created_ts"), F.lit(False)))
    .withColumn("status_changed_to_closed", F.col("closeddate").isNotNull() & ~F.col("closed_before_created") & ~F.coalesce(F.col("status").isin("Closed", "Cancelled", "Referred Out", "Forward"), F.lit(False))))

# Count each condition before cleaning
for city, df, closed_field in [("NYC", nyc_clean, "closed_date"), ("LA", la_clean, "closeddate")]:
    print(city)
    df.selectExpr(
        "sum(CAST(closed_without_date AS INT)) AS closed_without_date",
        "sum(CAST(closed_before_created AS INT)) AS reversed_dates",
        f"sum(CASE WHEN {closed_field} IS NOT NULL AND NOT COALESCE(status = 'Closed', FALSE) THEN 1 ELSE 0 END) AS date_without_closed_status",
        "sum(CAST(status_changed_to_closed AS INT)) AS eligible_status_changes",
        f"sum(CASE WHEN {closed_field} IS NOT NULL AND status IN ('Cancelled', 'Referred Out', 'Forward') THEN 1 ELSE 0 END) AS excluded_status_changes"
    ).show()

# Show which statuses have a closed date without Closed status
for city, df, closed_field in [("NYC", nyc_clean, "closed_date"), ("LA", la_clean, "closeddate")]:
    print(city)
    df.filter(F.col(closed_field).isNotNull() & ~F.coalesce(F.col("status") == "Closed", F.lit(False))).groupBy("status").count().orderBy(F.desc("count")).show()


# ### Checked conditions and cleaning decisions
# 
# | Condition in the original data | NYC | LA | Decision |
# |---|---:|---:|---|
# | Closed status, but no closed date | 4 | 1,991 | Keep the missing closed date as null; do not estimate it. |
# | Closed date before creation, any status | 626 | 0 | Set closed date and duration to null. |
# | Closed date exists, but status is not Closed | 11,109 | 35,643 | Exclude Cancelled, Referred Out, and Forward; change only remaining eligible statuses to Closed. |
# | Eligible for status change after exclusions | 11,109 | 0 | Set status to Closed. |
# | Excluded from status change despite a closed date | 0 | 35,643 | Keep the existing status. |
# 
# 
# NYC's third condition includes Open (7,020), In Progress (1,535), Assigned (2,490), Pending (18), and Unspecified (46). LA's includes Cancelled (34,711) and Referred Out (932). NYC statuses listed above remain eligible for conversion to Closed. LA cancellations and referrals are excluded and keep their existing labels; all 164 Forward requests also keep their status and have no closed date. Status changes are written to cleaned_status, while status retains the source label; the source datasets remain unchanged.

# ### 1.1 Closed status, but no closed date
# 
# **Reason:** A Closed status does not tell us when closure happened, so we keep the missing date as null instead of inventing a closure time.
# 
# The 4 NYC and 1,991 LA requests remain Closed with null closed dates. No mean resolution time is calculated, and no closure dates are imputed.

# In[26]:


# Leave missing closed dates as null
nyc_clean = nyc_clean.withColumn(
    "closed_date", F.when(F.col("closed_without_date"), F.lit(None).cast("timestamp")).otherwise(F.col("closed_date"))
)
la_clean = la_clean.withColumn(
    "closeddate", F.when(F.col("closed_without_date"), F.lit(None).cast("timestamp")).otherwise(F.col("closeddate"))
)


# ### 1.2 Closed date before creation, any status
# 
# **Reason:** A request cannot close before it is created, so we rewrite the invalid closure date to null instead of guessing a replacement.

# In[27]:


# Set invalid closed timestamps to null
nyc_clean = nyc_clean.withColumn("closed_date", F.when(~F.col("closed_before_created"), F.col("closed_date")))
la_clean = la_clean.withColumn("closeddate", F.when(~F.col("closed_before_created"), F.col("closeddate")))


# ### 1.3 Closed date exists, but status is not Closed
# 
# **Reason:** A closure date can coexist with a cancellation, referral, or forwarding outcome, so we preserve those labels and apply the date-priority rule only to the remaining statuses.
# 
# Exclude `Cancelled`, `Referred Out`, and `Forward` before flagging requests for conversion to Closed. This leaves 11,109 NYC requests and 0 LA requests eligible; LA keeps all 34,725 Cancelled, 932 Referred Out, and 164 Forward requests. The rule for active or unspecified statuses remains a project assumption that a recorded closure date takes priority.
# 
# Write the result to `cleaned_status`; do not overwrite `status`.

# In[28]:


# Set cleaned_status to Closed for the originally flagged requests
nyc_clean = nyc_clean.withColumn("cleaned_status", F.when(F.col("status_changed_to_closed"), "Closed").otherwise(F.col("cleaned_status")))
la_clean = la_clean.withColumn("cleaned_status", F.when(F.col("status_changed_to_closed"), "Closed").otherwise(F.col("cleaned_status")))


# In[30]:


# Verify the three rules and row preservation in both cities
for city, df, closed_field, expected in [("NYC", nyc_clean, "closed_date", (4, 626, 11092)), ("LA", la_clean, "closeddate", (1991, 0, 0))]:
    counts = df.selectExpr(
        "count(*) AS rows",
        "sum(CAST(closed_without_date AS INT)) AS missing_closed",
        "sum(CAST(closed_before_created AS INT)) AS invalid",
        "sum(CAST(status_changed_to_closed AS INT)) AS changed",
        f"sum(CASE WHEN closed_without_date AND {closed_field} IS NOT NULL THEN 1 ELSE 0 END) AS failed_null_preservation",
        f"sum(CASE WHEN closed_before_created AND {closed_field} IS NOT NULL THEN 1 ELSE 0 END) AS failed_date_cleaning",
        "sum(CASE WHEN status_changed_to_closed AND cleaned_status != 'Closed' THEN 1 ELSE 0 END) AS failed_status_cleaning",
        f"sum(CASE WHEN {closed_field} IS NOT NULL AND NOT COALESCE(cleaned_status IN ('Closed', 'Cancelled', 'Referred Out', 'Forward'), FALSE) THEN 1 ELSE 0 END) AS remaining_status_conflicts",
    ).first()
    assert counts.rows == 800000
    assert (counts.missing_closed, counts.invalid, counts.changed) == expected, f"{city}: got {(counts.missing_closed, counts.invalid, counts.changed)}, expected {expected}"
    # Check that missing dates stay null and invalid dates were cleared
    assert counts.failed_null_preservation == 0, f"{city}: missing closed dates were filled unexpectedly"
    assert counts.failed_date_cleaning == 0, f"{city}: invalid closed dates remain"

    # Check that only excluded statuses can retain a closed date without Closed cleaned_status
    assert counts.failed_status_cleaning == 0, f"{city}: flagged statuses were not updated"
    assert counts.remaining_status_conflicts == 0, f"{city}: eligible cleaned_status conflicts remain"
    print(city, "checks passed")


# Show the final NYC cleaned_status and date combinations
nyc_clean.groupBy("cleaned_status").agg(F.count("*").alias("requests"), F.count("closed_date").alias("with_closed_date"), F.sum(F.when(F.col("closed_date").isNull(), 1).otherwise(0)).alias("without_closed_date")).orderBy(F.desc("requests")).show()

# Show the final LA cleaned_status and date combinations
la_clean.groupBy("cleaned_status").agg(F.count("*").alias("requests"), F.count("closeddate").alias("with_closed_date"), F.sum(F.when(F.col("closeddate").isNull(), 1).otherwise(0)).alias("without_closed_date")).orderBy(F.desc("requests")).show()

# Check that excluded LA statuses were preserved
la_preserved_statuses = {row["cleaned_status"]: row["count"] for row in la_clean.filter(F.col("cleaned_status").isin("Cancelled", "Referred Out", "Forward")).groupBy("cleaned_status").count().collect()}
assert la_preserved_statuses == {"Cancelled": 34725, "Referred Out": 932, "Forward": 164}
print("LA cancellation, referral, and forwarding statuses preserved.")

# Check the final number of Closed requests without a closed date
assert nyc_clean.filter((F.col("cleaned_status") == "Closed") & F.col("closed_date").isNull()).count() == 613
assert la_clean.filter((F.col("cleaned_status") == "Closed") & F.col("closeddate").isNull()).count() == 1991
print("Closed requests with missing dates: NYC 613; LA 1,991.")


# ### Final report justification
#
# First, 4 NYC and 1,991 LA requests were marked Closed but had no closed date, so we left their dates null because the status does not tell us when closure happened. Second, 626 NYC requests had closed dates before creation, so we set those invalid dates to null rather than guess a correction. Third, 11,109 NYC and 35,643 LA requests had closed dates without Closed status, but we excluded Cancelled, Referred Out, and Forward because those labels describe distinct outcomes that can legitimately have a closure date. After this filter, we changed 11,109 NYC statuses and no LA statuses to Closed, preserving all cancellation, referral, and forwarding labels. All requests were retained; the final data contains 630 Closed NYC requests and 1,991 Closed LA requests with null closed dates, including the 17 NYC requests affected by both the invalid-date and status rules.
#
# The source status is preserved in `status`, and the final status after these rules and placeholder cleaning is stored in `cleaned_status`.

# ## 2. NYC and LA: ZIP codes that are not five digits

# In[31]:


# Show invalid NYC ZIP values and their counts
nyc_clean.filter(
    F.col("incident_zip").isNotNull() & ~F.col("incident_zip").rlike("^[0-9]{5}$")
).groupBy("incident_zip").count().show()

# Show invalid LA ZIP values and their counts
la_clean.filter(
    F.col("zipcode").isNotNull() & ~F.col("zipcode").rlike("^[0-9]{5}$")
).groupBy("zipcode").count().show()


# In[32]:


# Replace invalid values with null, keep valid ones as strings
nyc_clean = nyc_clean.withColumn(
    "incident_zip",
    F.when(F.col("incident_zip").cast("string").rlike("^[0-9]{5}$"),
           F.col("incident_zip").cast("string"))
)
la_clean = la_clean.withColumn(
    "zipcode",
    F.when(F.col("zipcode").cast("string").rlike("^[0-9]{5}$"),
           F.col("zipcode").cast("string"))
)

# Check that no invalid non-missing zip remained
assert nyc_clean.filter(F.col("incident_zip").isNotNull() & ~F.col("incident_zip").rlike("^[0-9]{5}$")).count() == 0
assert la_clean.filter(F.col("zipcode").isNotNull() & ~F.col("zipcode").rlike("^[0-9]{5}$")).count() == 0


# ### Final report explanation
#
# We found one invalid NYC ZIP value (`No`) and 39 invalid LA ZIP values (38 of `0` and 1 of `VE, 0`). We replaced these values with null. All requests were retained, and existing missing ZIP values were left missing.

# ## 3. NYC: placeholder values in 'cleaned_status' and 'open_data_channel_type'

# In[33]:


# Get unique cleaned_status values and their counts
nyc_clean.groupBy("cleaned_status").count().orderBy(F.desc("count")).show()

# Get unique submission channel values and their counts
nyc_clean.groupBy("open_data_channel_type").count().orderBy(F.desc("count")).show()


# The source had 53 Unspecified statuses; section 1 changed 46 with recorded closure dates to Closed, leaving 7 Unspecified statuses. The submission channel still has 65,687 UNKNOWN values, with no existing nulls in either checked column. Replace these placeholders with null in cleaned_status and open_data_channel_type and keep all other values, including `OTHER`.

# In[34]:


# Replace Unspecified cleaned_status with null
nyc_clean = nyc_clean.withColumn(
    "cleaned_status", F.when(F.col("cleaned_status") != "Unspecified", F.col("cleaned_status"))
)

# Replace UNKNOWN submission channel with null
nyc_clean = nyc_clean.withColumn(
    "open_data_channel_type",
    F.when(F.col("open_data_channel_type") != "UNKNOWN", F.col("open_data_channel_type"))
)

# Check that the placeholders are gone
assert nyc_clean.filter((F.col("cleaned_status") == "Unspecified") | (F.col("open_data_channel_type") == "UNKNOWN")).count() == 0


# ### Final report explanation
#
# After section 1 reclassified 46 Unspecified statuses with closure dates, 7 Unspecified statuses remained, along with 65,687 UNKNOWN submission channel values. We replaced these placeholders with null in cleaned_status and open_data_channel_type because they do not identify a known status or channel. All requests and other category values, including `OTHER`, were retained.

# ## Final verification
#
# Check the final row counts, request IDs, and missing-value totals. Status standardization is deferred until merging.

# In[35]:


# Check final NYC row count, unique request IDs, and missing values
nyc_final = nyc_clean.selectExpr(
    "count(*) AS rows", "count(DISTINCT unique_key) AS unique_ids",
    "sum(CASE WHEN incident_zip IS NULL THEN 1 ELSE 0 END) AS missing_zip",
    "sum(CASE WHEN cleaned_status IS NULL THEN 1 ELSE 0 END) AS missing_status",
    "sum(CASE WHEN open_data_channel_type IS NULL THEN 1 ELSE 0 END) AS missing_channel"
).first()
assert nyc_final.rows == nyc_final.unique_ids == 800000
# Unspecified statuses that were not changed to Closed become null
expected_missing_status = nyc_clean.filter((F.col("status") == "Unspecified") & ~F.col("status_changed_to_closed")).count()
assert (nyc_final.missing_zip, nyc_final.missing_status, nyc_final.missing_channel) == (6882, expected_missing_status, 65687)

# Check final LA row count, unique request IDs, and missing ZIPs
la_final = la_clean.selectExpr(
    "count(*) AS rows", "count(DISTINCT srnumber) AS unique_ids",
    "sum(CASE WHEN zipcode IS NULL THEN 1 ELSE 0 END) AS missing_zip"
).first()
assert la_final.rows == la_final.unique_ids == 800000
assert la_final.missing_zip == 926

# Show final verification results
print("NYC: 800,000 requests retained, unique IDs preserved, missing-value totals verified.")
print("LA: 800,000 requests retained, unique IDs preserved, missing ZIP total verified.")

# Check that the source status values are preserved in both cleaned datasets
for city, df, source, id_field in [
    ("NYC", nyc_clean, nyc_df, "unique_key"),
    ("LA", la_clean, la_df, "srnumber")
]:
    assert df.alias("clean").join(source.alias("source"), id_field).filter(
        ~F.col("clean.status").eqNullSafe(F.col("source.status"))
    ).count() == 0
    print(city, "source status unchanged; cleaned_status contains the cleaning results.")


# # universal cleaning

# ## 1. Trim whitespace
#
# Remove leading and trailing spaces from text columns while keeping source status unchanged.
#

# In[36]:


# Trim text fields; status already preserves the original source label.
nyc_clean = nyc_clean.select(*[F.trim(F.col(c)).alias(c) if t == "string" and c != "status" else F.col(c) for c, t in nyc_clean.dtypes])
la_clean = la_clean.select(*[F.trim(F.col(c)).alias(c) if t == "string" and c != "status" else F.col(c) for c, t in la_clean.dtypes])


# ## 2. Date and ZIP types
#
# Parse all listed date columns as timestamps and keep ZIP codes as strings.
#

# In[37]:


for c in ["created_date", "closed_date", "due_date", "resolution_action_updated_date"]:
    nyc_clean = nyc_clean.withColumn(c, F.to_timestamp(c))
for c in ["createddate", "updateddate", "servicedate", "closeddate"]:
    la_clean = la_clean.withColumn(c, F.to_timestamp(c))

# ZIP codes are labels, not numbers
nyc_clean = nyc_clean.withColumn("incident_zip", F.col("incident_zip").cast("string"))
la_clean = la_clean.withColumn("zipcode", F.col("zipcode").cast("string"))


# ## 3. Placeholder values
#
# Replace the remaining documented placeholders with null without filling missing values.
#

# In[38]:


for c, values in {
    "city": ["Not provided"],
    "borough": ["Unspecified"],
    "police_precinct": ["Unspecified"],
    "descriptor": ["N/A", "Unknown"],
    "descriptor_2": ["N/A", "Unknown"],
    "location_type": ["N/A", "Unknown"]
}.items():
    nyc_clean = nyc_clean.withColumn(c, F.when(~F.col(c).isin(values), F.col(c)))

# Include both numbered and borough-specific unspecified community boards.
nyc_clean = nyc_clean.withColumn("community_board", F.when(~F.col("community_board").rlike("(?i)unspecified"), F.col("community_board")))

# A zero house number does not identify a building.
la_clean = la_clean.withColumn("housenumber", F.when(F.col("housenumber").cast("string") != "0", F.col("housenumber")))


# ## 4. Coordinates and map pages
#
# Null zero or globally invalid coordinates and the three documented LA map-page outliers.
#

# In[39]:


# Use global bounds; the notes do not define city-specific boundaries.
for c, limit in [("latitude", 90), ("longitude", 180)]:
    nyc_clean = nyc_clean.withColumn(c, F.when((F.abs(F.col(c)) <= limit) & (F.col(c) != 0), F.col(c)))
    la_clean = la_clean.withColumn(c, F.when((F.abs(F.col(c)) <= limit) & (F.col(c) != 0), F.col(c)))

# Remove only the map-page values identified in EDA.
la_clean = la_clean.withColumn("tbmpage", F.when(~F.col("tbmpage").isin(4641, 4723, 4724), F.col("tbmpage")))


# ## 5. Spelling and casing
#
# Normalize NYC city and complaint labels and the four listed LA street-suffix variants.
#

# In[40]:


# Standardize casing and correct the observed NYC city aliases.
nyc_clean = nyc_clean.withColumn("city", F.upper("city"))
nyc_clean = nyc_clean.replace({"MANHATTEN": "MANHATTAN", "NY": "NEW YORK", "NEW YORK CITY": "NEW YORK"}, subset=["city"])

# Clear observed junk and non-NYC places; keep valid neighborhood names.
non_nyc = ["DOCTORS", "ROSLYN", "ELMONT", "ORIENT", "LYNBROOK", "KENMORE", "MOUNTAIN DALE", "DUANESBURG", "PELHAM", "SOUTH HAMPTON", "WILLINSTON PK", "HAMILTON TOWNSHIP", "DEER PARK", "MELVILLE", "BELLPORT", "CHERRY HILL", "BRONXVILLE", "YONKERS", "WEST HEMPSTEAD", "HARRISBURG", "FRANKLIN SQUARE", "WEST ISLIP", "ALBANY", "SOUTH RIVER", "MORAVIA", "BRENTWOOD", "MT VERNON", "TRUMBULL", "CHESTER", "IRVINE"]
nyc_clean = nyc_clean.withColumn("city", F.when(~F.col("city").isin(non_nyc), F.col("city")))
nyc_clean = nyc_clean.withColumn("complaint_type", F.upper("complaint_type"))

# Use the longer existing spelling for each documented suffix pair.
la_clean = la_clean.replace({"AV": "AVE", "RD": "ROAD", "LN": "LANE", "WY": "WAY"}, subset=["suffix"])
# addressverified = M stays unchanged because its meaning is unknown.


# ## 6. Suspected NYC duplicates
#
# Flag surplus NYC records that match on all source fields except request ID, and retain every row.
#

# In[41]:


# Compare source values so cleaning does not create artificial duplicates.
duplicate_columns = [c for c in nyc_df.columns if c != "unique_key"]
duplicate_window = Window.partitionBy(*duplicate_columns).orderBy("unique_key")
nyc_duplicate_flags = nyc_df.select("unique_key", (F.row_number().over(duplicate_window) > 1).alias("is_suspected_duplicate"))
nyc_clean = nyc_clean.join(nyc_duplicate_flags, "unique_key", "left")

# Report the measured count rather than assuming the PDF's count is current.
nyc_clean.groupBy("is_suspected_duplicate").count().show()


# ## 7. Resolution time
#
# Calculate elapsed hours only when both timestamps exist and closure is not before creation.
#

# In[42]:


# Missing or reversed dates cannot produce a valid duration.
nyc_clean = nyc_clean.withColumn("is_valid_resolution", F.coalesce(F.col("closed_date") >= F.col("created_date"), F.lit(False)))
la_clean = la_clean.withColumn("is_valid_resolution", F.coalesce(F.col("closeddate") >= F.col("createddate"), F.lit(False)))

# Timestamp subtraction gives seconds; divide by 3,600 for hours.
nyc_clean = nyc_clean.withColumn("resolution_hours", F.when(F.col("is_valid_resolution"), (F.col("closed_date").cast("long") - F.col("created_date").cast("long")) / 3600))
la_clean = la_clean.withColumn("resolution_hours", F.when(F.col("is_valid_resolution"), (F.col("closeddate").cast("long") - F.col("createddate").cast("long")) / 3600))


# ## 8. Original values and cleaning log
#
# Keep changed source values beside their cleaned columns and count affected rows for each column.
#

# In[43]:


# Compare against the untouched source and keep originals only for changed columns.
cleaning_log = []
for city, clean, source, key in [("NYC", nyc_clean, nyc_df, "unique_key"), ("LA", la_clean, la_df, "srnumber")]:
    compared = clean.alias("clean").join(source.alias("source"), key)

    # Exclude the join key from comparison since clean.key and source.key were merged by PySpark
    compared_cols = [c for c in source.columns if c != key]

    counts = compared.select(*[
        F.sum((~F.col(f"clean.{c}").eqNullSafe(F.col(f"source.{c}"))).cast("int")).alias(c)
        for c in compared_cols
    ]).first().asDict()

    changed = [c for c, count in counts.items() if count > 0]
    cleaning_log.extend((city, c, counts[c]) for c in changed)

    # status is already the original; give it the shared name requested in the notes.
    clean = clean.withColumn("status_original", F.col("status"))
    originals = source.select(key, *[F.col(c).alias(c + "_original") for c in changed])
    clean = clean.join(originals, key)

    ordered = []
    for c in clean.columns:
        if not c.endswith("_original"):
            ordered.append(c)
            if c + "_original" in clean.columns:
                ordered.append(c + "_original")
    clean = clean.select(*ordered)

    if city == "NYC":
        nyc_clean = clean
    else:
        la_clean = clean

# Counts are per column, so one request can appear in several log rows.
for city, column, count in cleaning_log:
    print(city, column, "rows changed:", count)


# ## 9. Verify and save
#
# Check rows, unique IDs, dates, coordinates, and durations before saving and re-reading Parquet.
#

# In[45]:


# Keep optional column drops out; all source columns remain available.
for city, df, key, created, closed in [
    ("NYC", nyc_clean, "unique_key", "created_date", "closed_date"),
    ("LA", la_clean, "srnumber", "createddate", "closeddate")
]:
    # Reuse a disk copy instead of rebuilding the full cleaning for every check.
    df = df.persist(StorageLevel.DISK_ONLY)
    assert df.count() == df.select(key).distinct().count() == 800000
    assert df.filter(F.col(closed) < F.col(created)).count() == 0
    assert df.filter((F.col("resolution_hours") < 0) | (F.col("is_valid_resolution") != F.col("resolution_hours").isNotNull())).count() == 0
    for c, limit in [("latitude", 90), ("longitude", 180)]:
        assert df.filter((F.col(c) == 0) | (F.abs(F.col(c)) > limit) | F.isnan(c)).count() == 0

    # Error if output exists so a rerun cannot silently replace saved data.
    path = f"clean_data/{city.lower()}_cleaned.parquet"
    df.write.mode("errorifexists").parquet(path)
    saved = spark.read.parquet(path)
    assert saved.count() == 800000
    assert saved.select(key).distinct().count() == 800000
    assert sorted(saved.dtypes) == sorted(df.dtypes)

    # Also save a CSV copy as a single file; escape='"' keeps commas, quotes, and line breaks inside text fields intact.
    csv_path = f"clean_data/{city.lower()}_cleaned.csv"
    df.coalesce(1).write.mode("errorifexists").option("header", True).option("escape", '"').csv(csv_path)
    # multiLine is needed to read back text fields that contain line breaks.
    saved_csv = spark.read.option("header", True).option("escape", '"').option("multiLine", True).csv(csv_path)
    assert saved_csv.count() == 800000
    assert saved_csv.columns == df.columns

    df.unpersist()
    print(city, "cleaning, Parquet, and CSV checks passed")

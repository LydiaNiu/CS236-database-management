from getpass import getpass

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CS236 Load Datasets").getOrCreate()
password = getpass("PostgreSQL password: ")
spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
# define jbdc url and jbdc properties for read and write postgres
jdbc_url = "jdbc:postgresql://localhost:8080/cs236db"
jdbc_properties = {
    "user": "admin",
    "password": password,
    "driver": "org.postgresql.Driver",
    "preferTimestampNTZ": "true",
}

# load the parquet datasets
datasets = [
    ("../Phase1/src/clean_data/la_cleaned.parquet", "public.la"),
    ("../Phase1/src/clean_data/nyc_cleaned.parquet", "public.nyc"),
    ("../Phase1/src/clean_data/unified.parquet", "public.unified"),
]

for path, table in datasets:
    df = spark.read.parquet(path) # also read the dtype
    
    # cast columns that differ from the postgresSQL schema
    if table == "public.la":
        for column in ("anonymous", "addressverified"):
            df = df.withColumn(column, df[column].cast("boolean"))

        for column in ("housenumber", "tbmpage", "tbmpage_original", "tbmrow", "cd", "nc"):
            df = df.withColumn(column, df[column].cast("string"))

    elif table == "public.nyc":
        for column in ("council_district", "bbl"):
            df = df.withColumn(column, df[column].cast("string"))

        df = df.withColumn("unique_key", df["unique_key"].cast("long"))

        for column in ("x_coordinate_state_plane", "y_coordinate_state_plane"):
            df = df.withColumn(column, df[column].cast("double"))

    # Recover the Pacific clock readings used in both Phase 1 exports.
    for column, dtype in df.dtypes:
        if dtype == "timestamp":
            df = df.withColumn(column, df[column].cast("timestamp_ntz"))

    # Match PostgreSQL NUMERIC(10, 4) for resolution_hours.
    df = df.withColumn("resolution_hours", df["resolution_hours"].cast("decimal(10,4)"))

    # display the path, table, and schema after casting
    print(f"Loading {path} into {table}")
    df.printSchema()

    # read the destination schema from the postgres
    target_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"(SELECT * FROM {table} WHERE 1 = 0) AS target_schema",
        properties=jdbc_properties,
    )
    
    # mapping the the column name to the spark data type
    spark_types = {field.name: field.dataType for field in df.schema}
    target_types = {field.name: field.dataType for field in target_df.schema}

    problems = []

    # check and report any problems from the schema check between the spark dataset and the postgres 
    for column in sorted(target_types.keys() - spark_types.keys()):
        problems.append(f"{column}: missing from the dataset")

    for column in sorted(spark_types.keys() - target_types.keys()):
        problems.append(f"{column}: missing from the PostgreSQL table")

    for column in sorted(spark_types.keys() & target_types.keys()):
        if spark_types[column] != target_types[column]:
            problems.append(
                f"{column}: Spark has {spark_types[column].simpleString()}, "
                f"PostgreSQL expects {target_types[column].simpleString()}"
            )

    if problems:
        raise ValueError(f"Schema check failed for {table}\n" + "\n".join(problems))

    print(f"Schema check passed for {table} ({len(spark_types)} columns).")
    
    # write data into the table
    df.write.jdbc(
        url=jdbc_url,
        table=table,
        mode="append",
        properties=jdbc_properties,
    )

    print(f"Loaded {table}")

spark.stop()

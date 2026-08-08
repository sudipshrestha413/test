# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Hands-On Lab v2 — EXTERNAL table on OVHcloud Object Storage
# MAGIC
# MAGIC ## What changed vs. v1, and why
# MAGIC In v1 the table was a Unity Catalog **managed** table: Databricks owns both the metadata *and* the storage,
# MAGIC and UC deliberately hides the physical path (that's why `DESCRIBE DETAIL` gave you an empty `location`
# MAGIC and the file peek failed with `INVALID_PATH_STRING`).
# MAGIC
# MAGIC This version uses an **external** table:
# MAGIC
# MAGIC | | Managed table | External table (this lab) |
# MAGIC | --- | --- | --- |
# MAGIC | Who owns the files | Databricks / UC | **You** — an OVHcloud S3 bucket you control |
# MAGIC | `LOCATION` clause | none | `LOCATION 's3a://<bucket>/...'` |
# MAGIC | `DROP TABLE` | deletes data | **keeps** data — only the catalog pointer is removed |
# MAGIC | Inspecting `_delta_log` | blocked on UC | fully visible (great for learning!) |
# MAGIC
# MAGIC ## Prerequisites (one-time)
# MAGIC **OVHcloud side**
# MAGIC 1. Public Cloud → Object Storage → create a **bucket** (note its **region**, e.g. `gra`, `rbx`, `sbg`, `de`, `waw`).
# MAGIC 2. Object Storage → **S3 users** → create a user → copy the **access key** and **secret key**.
# MAGIC 3. Endpoint pattern: `https://s3.<region>.io.cloud.ovh.net` (one endpoint serves Standard + High Performance).
# MAGIC
# MAGIC **Databricks side**
# MAGIC 1. Use a **classic cluster** in **Dedicated (single user)** access mode — *not* serverless.
# MAGIC    (Unity Catalog external locations only support the native clouds / Cloudflare R2, so custom
# MAGIC    S3-compatible endpoints like OVH must go through classic S3A configuration + `hive_metastore`.)
# MAGIC 2. Recommended: store keys in a secret scope:
# MAGIC    `databricks secrets create-scope ovh` → `databricks secrets put-secret ovh s3_access_key` (+ `s3_secret_key`).
# MAGIC
# MAGIC > Alternative to the config cell below — set once at **cluster level** (Advanced options → Spark config):
# MAGIC > ```
# MAGIC > spark.hadoop.fs.s3a.bucket.<bucket>.endpoint https://s3.<region>.io.cloud.ovh.net
# MAGIC > spark.hadoop.fs.s3a.bucket.<bucket>.access.key {{secrets/ovh/s3_access_key}}
# MAGIC > spark.hadoop.fs.s3a.bucket.<bucket>.secret.key {{secrets/ovh/s3_secret_key}}
# MAGIC > spark.hadoop.fs.s3a.bucket.<bucket>.path.style.access true
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0a. Configuration — EDIT THESE THREE VALUES

# COMMAND ----------

BUCKET = "msc"        # <-- your OVH bucket name
REGION = "eu-west-par"                        # <-- your OVH region (lowercase)
SECRET_SCOPE = "ovh"                  # <-- your Databricks secret scope (or leave and use fallback below)

ENDPOINT   = f"https://s3.{REGION}.io.cloud.ovh.net"
TABLE_PATH = f"s3a://{BUCKET}/delta_demo/employees"

def get_secret(scope, key, fallback):
    try:
        return dbutils.secrets.get(scope, key)
    except Exception:
        print(f"⚠ Secret {scope}/{key} not found — using inline fallback. "
              f"Fine for a quick test; do NOT keep real keys in a notebook.")
        return fallback

ACCESS_KEY = get_secret(SECRET_SCOPE, "s3_access_key", "b959f6f8e3584820ac11089174f2b5e9")
SECRET_KEY = get_secret(SECRET_SCOPE, "s3_secret_key", "7a82748c48224d94b121664b57ebe33d")

print("Endpoint  :", ENDPOINT)
print("Table path:", TABLE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0b. Wire the OVH endpoint into S3A — *per bucket*
# MAGIC We scope every option to `fs.s3a.bucket.<your-bucket>.*` so **only this bucket** is redirected to OVH.
# MAGIC A global `fs.s3a.endpoint` override could break other S3 access on the cluster — never do that.

# COMMAND ----------

hconf = spark._jsc.hadoopConfiguration()          # requires Dedicated/single-user access mode
prefix = f"fs.s3a.bucket.{BUCKET}."

hconf.set(prefix + "endpoint",          ENDPOINT)
hconf.set(prefix + "access.key",        ACCESS_KEY)
hconf.set(prefix + "secret.key",        SECRET_KEY)
hconf.set(prefix + "path.style.access", "true")   # safest addressing style for S3-compatible providers
hconf.set(prefix + "endpoint.region",   REGION)   # helps SigV4 signing against custom endpoints
hconf.set(prefix + "aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

print(f"S3A configured for bucket '{BUCKET}' → {ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connectivity smoke test
# MAGIC Fail fast here (bad keys / wrong region / typo'd bucket) rather than mid-lab.

# COMMAND ----------

probe = f"s3a://{BUCKET}/_connectivity_check.txt"
dbutils.fs.put(probe, "hello from Databricks → OVHcloud", True)
print("Read back:", dbutils.fs.head(probe))
dbutils.fs.rm(probe)
print("\nBucket root listing:")
for f in dbutils.fs.ls(f"s3a://{BUCKET}/"):
    print("  ", f.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0c. Create the EXTERNAL Delta table
# MAGIC We register it in **`hive_metastore`** (UC catalogs would demand a UC External Location for the path,
# MAGIC which doesn't exist for OVH). The `LOCATION` clause is what makes it external.

# COMMAND ----------

try:
    spark.sql("USE CATALOG hive_metastore")     # no-op / may not exist on non-UC workspaces
except Exception:
    pass

spark.sql("CREATE SCHEMA IF NOT EXISTS delta_demo_ext")
spark.sql("USE delta_demo_ext")
spark.sql("DROP TABLE IF EXISTS employees")

spark.sql(f"""
    CREATE TABLE employees (
      id     INT,
      name   STRING,
      dept   STRING,
      salary DOUBLE
    )
    USING DELTA
    LOCATION '{TABLE_PATH}'
""")
print("External Delta table created at:", TABLE_PATH)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES
# MAGIC   (1, 'Asha',   'Engineering', 95000),
# MAGIC   (2, 'Bikash', 'Engineering', 88000),
# MAGIC   (3, 'Clara',  'Sales',       67000),
# MAGIC   (4, 'Dawa',   'Sales',       71000),
# MAGIC   (5, 'Elena',  'Finance',     83000);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Under the hood — this time it works 🎉
# MAGIC `DESCRIBE DETAIL` now shows a real `location` (your bucket), and we can list the raw layout:
# MAGIC Parquet data files + the `_delta_log/` folder. You can verify the same files in the
# MAGIC **OVHcloud Control Panel → your bucket** — it's just object storage.

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL employees")
        .select("format", "location", "numFiles", "sizeInBytes"))

# COMMAND ----------

print("TABLE DIRECTORY:")
for f in dbutils.fs.ls(TABLE_PATH):
    print("  ", f.name)

print("\nTRANSACTION LOG (_delta_log):")
for f in dbutils.fs.ls(TABLE_PATH + "/_delta_log"):
    print("  ", f.name)

# COMMAND ----------

# The very first commit — raw, human-readable JSON. THE LOG IS THE TABLE.
print(dbutils.fs.head(TABLE_PATH + "/_delta_log/00000000000000000000.json", 1200))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ACID — proving each letter
# MAGIC
# MAGIC ### A — Atomicity: a write that dies halfway leaves *zero* trace
# MAGIC A 3-row insert, rigged so row `103` throws a runtime error via `raise_error()` — simulating a crash
# MAGIC mid-write. Rows `101`/`102` are valid… yet **none** survive, because no commit reached the log.

# COMMAND ----------

print("Row count BEFORE the failed write:", spark.table("employees").count())

try:
    spark.sql("""
        INSERT INTO employees
        SELECT id, name, dept,
               CASE WHEN id = 103 THEN raise_error('Simulated crash halfway through the batch!')
                    ELSE salary END AS salary
        FROM VALUES
          (101, 'Farid', 'Marketing', 59000.0),
          (102, 'Gita',  'Marketing', 61000.0),
          (103, 'Hari',  'Marketing', 63000.0)
        AS t(id, name, dept, salary)
    """)
except Exception as e:
    print("\nWrite failed (by design):", str(e).splitlines()[0][:120])

print("\nRow count AFTER the failed write:", spark.table("employees").count())

# COMMAND ----------

# Bonus (only possible with an inspectable external table): the aborted job may have left an
# ORPHANED Parquet file in the bucket — yet the table ignores it, because it was never committed.
data_files_on_disk = [f.name for f in dbutils.fs.ls(TABLE_PATH) if f.name.endswith(".parquet")]
committed_files    = spark.table("employees").inputFiles()
print(f"Parquet files physically in the bucket : {len(data_files_on_disk)}")
print(f"Files the transaction log says exist   : {len(committed_files)}")
print("→ Readers trust the LOG, not the directory listing. That's atomicity on object storage.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### C — Consistency: constraints guard every transaction

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE employees ADD CONSTRAINT salary_positive CHECK (salary > 0);

# COMMAND ----------

try:
    spark.sql("""
        INSERT INTO employees VALUES
          (6, 'Farid', 'Marketing',  59000),
          (7, 'Gita',  'Marketing', -12345)   -- violates salary_positive
    """)
except Exception as e:
    print("Batch rejected:", str(e).splitlines()[0][:160])

print("\nRow count still:", spark.table("employees").count(),
      "→ valid row 6 rolled back together with bad row 7")

# COMMAND ----------

# MAGIC %md
# MAGIC ### I — Isolation • D — Durability
# MAGIC * **Isolation:** optimistic concurrency. Readers are pinned to the latest *committed* version when their
# MAGIC   query starts — never a half-finished write. Concurrent writers commit serially; a conflicting second
# MAGIC   writer retries or fails (`ConcurrentModificationException`, default level `WriteSerializable`).
# MAGIC * **Durability:** every commit below is a JSON file **already persisted in your OVH bucket**. Kill this
# MAGIC   cluster, come back tomorrow, attach a different cluster — the history is intact, because the table
# MAGIC   lives in the bucket, not in Databricks.
# MAGIC
# MAGIC Note in the history that our two *failed* writes appear **nowhere** — they never became versions.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema enforcement — bad data bounces off
# MAGIC Delta validates every incoming write against the table schema stored in the log.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Attempt 1: an EXTRA column ('bonus') the table doesn't have
schema_with_bonus = StructType([
    StructField("id",     IntegerType()),
    StructField("name",   StringType()),
    StructField("dept",   StringType()),
    StructField("salary", DoubleType()),
    StructField("bonus",  DoubleType()),
])
extra_col_df = spark.createDataFrame([(8, "Imran", "Finance", 90000.0, 8000.0)], schema_with_bonus)

try:
    extra_col_df.write.format("delta").mode("append").saveAsTable("employees")
except Exception as e:
    print("REJECTED — schema mismatch (extra column):\n")
    print(str(e)[:600])

# COMMAND ----------

# Attempt 2: a WRONG TYPE (id arrives as a string)
wrong_type_schema = StructType([
    StructField("id",     StringType()),   # table expects INT
    StructField("name",   StringType()),
    StructField("dept",   StringType()),
    StructField("salary", DoubleType()),
])
wrong_type_df = spark.createDataFrame([("nine", "Jia", "Finance", 76000.0)], wrong_type_schema)

try:
    wrong_type_df.write.format("delta").mode("append").saveAsTable("employees")
except Exception as e:
    print("REJECTED — schema mismatch (incompatible type):\n")
    print(str(e)[:600])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema *evolution* — drift as a deliberate choice
# MAGIC Enforcement is the default; evolution is **opt-in** via `mergeSchema` (or `ALTER TABLE ... ADD COLUMN`).
# MAGIC The new column lands, historical rows read it as `NULL`, and the schema change is itself a new commit.

# COMMAND ----------

extra_col_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("employees")

display(spark.table("employees").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time travel — every commit is a queryable version
# MAGIC One more meaningful change first, so we have something to diff: Engineering gets a 10% raise.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees SET salary = salary * 1.10 WHERE dept = 'Engineering';

# COMMAND ----------

# MAGIC %md
# MAGIC > Exact version numbers depend on how many times you've re-run cells, so we *compute* them from history.

# COMMAND ----------

current_v = spark.sql("DESCRIBE HISTORY employees").selectExpr("max(version)").first()[0]
prev_v = current_v - 1
print(f"Current version = {current_v}. Reading version {prev_v} (pre-raise):\n")

spark.sql(f"SELECT * FROM employees VERSION AS OF {prev_v} ORDER BY id").show()

# COMMAND ----------

# Audit: join the present against the past to see exactly what the UPDATE changed
spark.sql(f"""
    SELECT cur.id, cur.name, cur.dept,
           old.salary AS salary_before,
           cur.salary AS salary_now
    FROM employees cur
    JOIN employees VERSION AS OF {prev_v} old USING (id)
    WHERE cur.salary <> old.salary
    ORDER BY id
""").show()

# COMMAND ----------

# Travel by TIMESTAMP too (any moment resolves to the last commit at or before it)
ts = (spark.sql("DESCRIBE HISTORY employees")
          .where(f"version = {prev_v}")
          .select("timestamp").first()[0])

spark.sql(f"SELECT COUNT(*) AS rows_at_that_moment FROM employees TIMESTAMP AS OF '{ts}'").show()

# Equivalent syntaxes worth knowing:
#   SQL shorthand:  SELECT * FROM employees@v3
#   PySpark:        spark.read.option("versionAsOf", 3).table("employees")
#   By path:        spark.read.format("delta").load(TABLE_PATH)   ← works even with NO catalog entry

# COMMAND ----------

# MAGIC %md
# MAGIC ### "Someone deleted production" → full recovery in one command

# COMMAND ----------

last_good = spark.sql("DESCRIBE HISTORY employees").selectExpr("max(version)").first()[0]

spark.sql("DELETE FROM employees")   # ...the intern strikes
print("Rows after the 'accident':", spark.table("employees").count())

# COMMAND ----------

spark.sql(f"RESTORE TABLE employees TO VERSION AS OF {last_good}")
print("Rows after RESTORE:", spark.table("employees").count())
display(spark.table("employees").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The external-table finale: drop it, then resurrect it from the bucket
# MAGIC For an external table, `DROP TABLE` removes only the catalog entry — every Parquet file and the whole
# MAGIC `_delta_log` stay in OVHcloud. Since **the log is the table**, we can re-register it with a one-liner —
# MAGIC no schema needed, and the *entire version history* comes back with it.

# COMMAND ----------

spark.sql("DROP TABLE employees")
print("Table dropped from the metastore. But in the OVH bucket:")
for f in dbutils.fs.ls(TABLE_PATH):
    print("  ", f.name)

# The data is even queryable right now, catalog or no catalog:
print("\nRows readable directly by path:",
      spark.read.format("delta").load(TABLE_PATH).count())

# COMMAND ----------

# Resurrection: no column list required — schema, constraints, and history all live in the log
spark.sql(f"CREATE TABLE employees USING DELTA LOCATION '{TABLE_PATH}'")
print("Re-registered. Full history preserved:\n")
display(spark.sql("DESCRIBE HISTORY employees")
        .select("version", "timestamp", "operation"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## How far back can you travel?
# MAGIC Time travel is an **operational window, not an archive**:
# MAGIC * Old *data files* stay readable until `VACUUM` removes files unreferenced for longer than
# MAGIC   `delta.deletedFileRetentionDuration` (default **7 days**), e.g. `VACUUM employees RETAIN 168 HOURS`.
# MAGIC * Old *log entries* are kept per `delta.logRetentionDuration` (default **30 days**).
# MAGIC
# MAGIC For long-term snapshots, `CLONE` the table or raise those properties deliberately.
# MAGIC
# MAGIC ---
# MAGIC ## Recap
# MAGIC | You proved | How |
# MAGIC | --- | --- |
# MAGIC | **Atomicity** | Mid-batch crash → zero rows committed; orphaned file ignored by the log |
# MAGIC | **Consistency** | `CHECK` constraint rolled back an entire bad batch |
# MAGIC | **Isolation / Durability** | Serialized commits, persisted as JSON in *your* OVH bucket |
# MAGIC | **Schema enforcement** | Extra column + wrong type rejected; `mergeSchema` evolved on purpose |
# MAGIC | **Time travel** | `VERSION AS OF`, `TIMESTAMP AS OF`, version diff, `RESTORE` after full delete |
# MAGIC | **External tables** | `DROP` kept the data in OVH; re-registered from the path with full history |

# COMMAND ----------

# OPTIONAL cleanup — uncomment to remove everything this lab created
# spark.sql("DROP TABLE IF EXISTS employees")                 # metadata only (external table)
# dbutils.fs.rm(f"s3a://{BUCKET}/delta_demo", recurse=True)   # actually deletes the data in OVH
# spark.sql("DROP SCHEMA IF EXISTS delta_demo_ext CASCADE")
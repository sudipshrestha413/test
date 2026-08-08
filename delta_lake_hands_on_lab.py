# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake on Databricks — Concept + Hands-On Lab
# MAGIC
# MAGIC ## The concept in 60 seconds
# MAGIC A **Delta table** is nothing exotic. It is two things living together in cloud storage:
# MAGIC
# MAGIC 1. **Data files** → plain, open **Parquet** files
# MAGIC 2. **A transaction log** → the `_delta_log/` folder: an ordered sequence of JSON *commit* files (plus periodic Parquet checkpoints)
# MAGIC
# MAGIC The log — not the folder listing — is the **single source of truth**. Every reader replays the log to learn
# MAGIC exactly which Parquet files constitute the table *at this instant*, and every write
# MAGIC (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, schema change) becomes **one atomic commit** appended to the log.
# MAGIC
# MAGIC That single design decision unlocks everything we test below:
# MAGIC
# MAGIC | Capability | Why the log makes it possible |
# MAGIC | --- | --- |
# MAGIC | **ACID transactions** | A commit either lands in the log or it never happened — nothing in between |
# MAGIC | **Schema enforcement / evolution** | The schema is versioned metadata in the log, validated on every write |
# MAGIC | **Time travel** | Old commits (and the files they reference) stick around, so any past version stays queryable |
# MAGIC
# MAGIC Delta is the **default table format on Databricks** — a bare `CREATE TABLE` already gives you Delta.
# MAGIC
# MAGIC > **How to run:** attach any recent Databricks Runtime cluster or serverless compute and use *Run all*, top to bottom.
# MAGIC > Cells that are *supposed* to fail are wrapped in `try/except`, so the full run completes cleanly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup — a schema and one Delta table

# COMMAND ----------

# Unity Catalog users: optionally run  spark.sql("USE CATALOG <your_catalog>")  first.
spark.sql("CREATE SCHEMA IF NOT EXISTS delta_demo")
spark.sql("USE delta_demo")
spark.sql("DROP TABLE IF EXISTS employees")
print("Using schema:", spark.sql("SELECT current_schema()").first()[0])

# COMMAND ----------

spark.sql("use sudip")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 'USING DELTA' is optional on Databricks: Delta is already the default format
# MAGIC CREATE TABLE employees (
# MAGIC   id     INT,
# MAGIC   name   STRING,
# MAGIC   dept   STRING,
# MAGIC   salary DOUBLE
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql 
# MAGIC show create table employees;

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
# MAGIC ### Peek under the hood
# MAGIC `DESCRIBE DETAIL` confirms the format and where the table lives. The optional cell after it lists the raw
# MAGIC storage layout: Parquet files next to a `_delta_log/` folder — that's the entire "magic".

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL employees").select("format", "location", "numFiles", "sizeInBytes"))

# COMMAND ----------

# OPTIONAL peek at raw files. On Unity Catalog *managed* tables, direct path access is
# intentionally blocked by governance — if this cell prints a permission message, that's expected.
loc = spark.sql("DESCRIBE DETAIL employees").select("location").first()[0]
try:
    print("TABLE DIRECTORY:")
    for f in dbutils.fs.ls(loc):
        print("  ", f.name)
    print("\nTRANSACTION LOG (_delta_log):")
    for f in dbutils.fs.ls(loc + "/_delta_log"):
        print("  ", f.name)
except Exception as e:
    print("Direct file access not permitted here (normal for UC managed tables).")
    print(str(e)[:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ACID — proving each letter
# MAGIC
# MAGIC ### A — Atomicity: a write that dies halfway leaves *zero* trace
# MAGIC We insert a 3-row batch, but rig row `103` to throw a runtime error with `raise_error()` — simulating a job
# MAGIC that crashes mid-write. Rows `101` and `102` are perfectly valid… yet **none** of them should survive,
# MAGIC because the commit never reaches the transaction log.

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

# MAGIC %md
# MAGIC The count is unchanged — **all-or-nothing**. Even if the aborted job left a stray Parquet file in the
# MAGIC directory, readers never see it: a file that isn't referenced by a committed log entry simply is not
# MAGIC part of the table. Compare this with plain Parquet directories, where a half-finished job leaves
# MAGIC partial files that silently corrupt every downstream query.
# MAGIC
# MAGIC ### C — Consistency: constraints guard every transaction
# MAGIC Delta lets you declare rules the data must always satisfy. A write that would break a rule is rejected
# MAGIC **in its entirety**, so the table can never transition into an invalid state.

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

print("\nRow count still:", spark.table("employees").count(), "→ the valid row 6 was rolled back with the bad row 7")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Constraints are stored as versioned table metadata (in the log, like everything else)
# MAGIC SHOW TBLPROPERTIES employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### I — Isolation: optimistic concurrency, snapshot reads
# MAGIC Delta uses **optimistic concurrency control**:
# MAGIC
# MAGIC * **Readers never block and are never blocked.** A query is pinned to the latest *committed* version at the
# MAGIC   moment it starts — a long-running report can never observe a half-finished write.
# MAGIC * **Writers commit serially.** Two concurrent writers both prepare their changes; the first to append its
# MAGIC   commit file wins, the second detects the conflict and retries or fails with a `ConcurrentModificationException`
# MAGIC   (default level: `WriteSerializable`).
# MAGIC
# MAGIC The evidence is the history below: every transaction is exactly one serialized version, and our two
# MAGIC failed writes appear **nowhere** — they were never part of the table's timeline.
# MAGIC
# MAGIC ### D — Durability: the log *is* the table
# MAGIC Each commit is a JSON file persisted to cloud object storage (S3 / ADLS / GCS). Once written, it survives
# MAGIC cluster crashes and restarts — detach this cluster, come back tomorrow, and the history below is intact.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# OPTIONAL: read the very first commit file — raw, human-readable JSON describing commit 0.
try:
    print(dbutils.fs.head(loc + "/_delta_log/00000000000000000000.json", 1200))
except Exception:
    print("(Path access blocked on UC managed tables — skip this peek; DESCRIBE HISTORY above shows the same story.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema enforcement — bad data bounces off
# MAGIC
# MAGIC On every write, Delta validates the incoming schema against the table's schema. Extra columns or
# MAGIC incompatible types → the whole write is rejected. This kills the classic data-lake disease of
# MAGIC *silent schema drift*, where one rogue job quietly poisons a dataset for everyone downstream.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Attempt 1: an EXTRA column ('bonus') that the table doesn't have
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
# MAGIC Enforcement is the default; evolution is **opt-in**. Passing `mergeSchema=true` tells Delta:
# MAGIC "I really do want this new column." The column is added, and all historical rows read it as `NULL`.
# MAGIC (You can also evolve explicitly with `ALTER TABLE employees ADD COLUMN ...`.)

# COMMAND ----------

extra_col_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("employees")

display(spark.table("employees").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC The `bonus` column now exists, old rows show `NULL`, and — because a schema change is just another
# MAGIC commit — this created a **new table version**. Which is the perfect segue…

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time travel — every commit is a queryable version
# MAGIC
# MAGIC First, one more meaningful change so we have something to compare: Engineering gets a 10% raise.
# MAGIC (`UPDATE` on a data lake — itself something plain Parquet cannot do.)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees SET salary = salary * 1.10 WHERE dept = 'Engineering';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md
# MAGIC > Exact version numbers depend on how many times you've re-run cells, so below we *compute* them from
# MAGIC > the history instead of hard-coding.

# COMMAND ----------

# Query the table AS IT WAS one version ago (before the raise)
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

# You can also travel by TIMESTAMP (any moment resolves to the last commit at or before it)
ts = (spark.sql("DESCRIBE HISTORY employees")
          .where(f"version = {prev_v}")
          .select("timestamp").first()[0])

spark.sql(f"SELECT COUNT(*) AS rows_at_that_moment FROM employees TIMESTAMP AS OF '{ts}'").show()

# Equivalent syntaxes worth knowing:
#   SQL shorthand:  SELECT * FROM employees@v3
#   PySpark:        spark.read.option("versionAsOf", 3).table("employees")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The killer demo: "someone deleted production" → full recovery in one command

# COMMAND ----------

last_good = spark.sql("DESCRIBE HISTORY employees").selectExpr("max(version)").first()[0]

spark.sql("DELETE FROM employees")   # ...the intern strikes
print("Rows after the 'accident':", spark.table("employees").count())

# COMMAND ----------

spark.sql(f"RESTORE TABLE employees TO VERSION AS OF {last_good}")
print("Rows after RESTORE:", spark.table("employees").count())
display(spark.table("employees").orderBy("id"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: RESTORE didn't erase anything — it is itself a new commit.
# MAGIC -- The DELETE and the RESTORE both remain in the audit trail forever (well, until log retention).
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### How far back can you travel?
# MAGIC Time travel is an **operational window, not an archive**:
# MAGIC
# MAGIC * Old *data files* stay readable until `VACUUM` physically removes files unreferenced for longer than the
# MAGIC   retention threshold (`delta.deletedFileRetentionDuration`, default **7 days**).
# MAGIC * Old *log entries* are kept per `delta.logRetentionDuration` (default **30 days**).
# MAGIC
# MAGIC After `VACUUM employees RETAIN 168 HOURS`, versions older than 7 days become unreadable. For long-term
# MAGIC snapshots, clone the table or raise the retention properties deliberately.
# MAGIC
# MAGIC ---
# MAGIC ## Recap + where to go next
# MAGIC
# MAGIC | You proved | How |
# MAGIC | --- | --- |
# MAGIC | **Atomicity** | A mid-batch crash left zero rows behind |
# MAGIC | **Consistency** | A `CHECK` constraint rolled back an entire bad batch |
# MAGIC | **Isolation / Durability** | Serialized, crash-proof commits in `DESCRIBE HISTORY` |
# MAGIC | **Schema enforcement** | Extra column + wrong type both rejected; `mergeSchema` evolved on purpose |
# MAGIC | **Time travel** | `VERSION AS OF`, `TIMESTAMP AS OF`, version-diff audit, `RESTORE` after a full delete |
# MAGIC
# MAGIC Natural next steps: `MERGE INTO` (upserts / CDC), `OPTIMIZE` + liquid clustering or `ZORDER`,
# MAGIC Change Data Feed, and using one Delta table as both a **streaming** source/sink and a **batch** table.

# COMMAND ----------

# OPTIONAL cleanup — uncomment to remove everything this lab created
# spark.sql("DROP SCHEMA delta_demo CASCADE")
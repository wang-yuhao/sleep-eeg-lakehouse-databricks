# Databricks Data Engineer Associate – 14-Day Study Plan (Project-Based)

This guide turns the **Sleep EEG Lakehouse** project into a *complete hands-on lab* for the Databricks Certified Data Engineer Associate exam.

- **Repo:** `sleep-eeg-lakehouse-databricks`
- **Audience:** You (BCI) preparing for the exam + building interview stories.
- **Goal:** In ~14 days (2–3 hours/day), you:
  - Cover all exam domains using *actual code* in this repo.
  - Extend the project with small, exam-aligned features.
  - Produce clear explanations you can re-use in interviews.

Use this file as your **daily checklist**. For each day, there are:
- **Objectives** (what you should be able to explain).
- **Repo paths** to open.
- **Exercises** (code / config changes).
- **Reflection** prompts to prepare interview answers.

> Tip: Commit small changes per exercise (e.g. `feat(exam): add auto loader example for JSON`). This also improves your Git + CI story for interviews.

---

## Day 1 – Exam Orientation & Project Mapping

### Objectives

- Understand the **exam domains** and weights.
- Map each domain to components in this repo.
- Draft a 1–2 sentence elevator pitch linking this project to the exam.

### Steps

1. **Read the official exam guide**
   - Go to the Databricks Certified Data Engineer Associate page and download/read the latest Exam Guide PDF.
   - Write down the domains (names + %), e.g. (example only, check current guide):
     - Databricks Lakehouse Platform & Architecture
     - Data Ingestion & ELT
     - Data Processing & Transformations
     - Production Pipelines (DLT, Jobs)
     - Governance & Unity Catalog
     - CI/CD, Git, CLI (newer domains)

2. **Open this repo’s main documentation**
   - File: `README.md`
   - Sections to skim:
     - Project Overview
     - Architecture
     - Databricks Certified Data Engineer Associate Coverage
     - Skillset Demonstrated

3. **Create a mapping table**

   Add a section to `README.md` (or a new file `exam-prep/domain-mapping.md`) like:

   ```markdown
   ## Exam Domain → Repo Mapping

   | Exam Domain | Repo Components |
   | ----------- | ----------------|
   | Lakehouse & Platform | `notebooks/01_setup_unity_catalog.py`, `README.md` architecture diagram |
   | Ingestion & ELT | `src/bronze/ingest_sleep_edf.py`, `src/bronze/schema_sleep_edf.json` |
   | Data Processing | `src/silver/*.py`, `src/gold/*.py`, `notebooks/03_analyze_tda_features.py` |
   | Production Pipelines | `resources/sleep_eeg_dlt_pipeline.yml`, `notebooks/02_run_dlt_pipeline.py` |
   | Governance & UC | `notebooks/01_setup_unity_catalog.py`, `src/utils/config.py` |
   | CI/CD & Git | `.github/workflows/ci.yml`, `tests/*.py`, `databricks.yml` |
   ```

4. **Reflection (write in a personal notes file)**

   - “In 2–3 sentences, how does this project prove I meet the exam requirements?”
   - Save this in `exam-prep/notes.md` so you can reuse wording for LinkedIn and interviews.

---

## Day 2 – Lakehouse & Unity Catalog Basics

### Objectives

- Explain the **medallion architecture** (Bronze/Silver/Gold) with this project.
- Understand Unity Catalog objects: catalog, schema, tables, volumes.

### Steps

1. **Study Unity Catalog setup notebook**

   - File: `notebooks/01_setup_unity_catalog.py`
   - Walk through each SQL block:
     - `CREATE CATALOG IF NOT EXISTS sleep_eeg_lakehouse`
     - `CREATE SCHEMA bronze/silver/gold`
     - `CREATE VOLUME bronze.sleep_edf_raw`
   - For each statement, answer:
     - What object is being created?
     - Where does it live (catalog.schema)?
     - Who would you grant access to in a real company?

2. **Draw (or re-draw) the architecture**

   - In `exam-prep/architecture-notes.md`, sketch:
     - PhysioNet → Volume → Bronze table → Silver → Gold → ML notebooks.
   - Re-use the mermaid diagram from `README.md` but annotate with exam language (e.g., “Delta table”, “Unity Catalog volume”).

3. **Exercise: add a comments section about governance**

   - In `notebooks/01_setup_unity_catalog.py`, add a markdown cell:

   ```python
   # MAGIC %md
   # MAGIC ### Governance Considerations
   # MAGIC - Use GRANT statements to restrict access to `gold` schema.
   # MAGIC - Use row-level security for sensitive subjects.
   # MAGIC - Audit access via Unity Catalog audit logs.
   ```

4. **Reflection**

   - Practice answering: “How would you explain the Databricks Lakehouse to a hiring manager?”

---

## Day 3 – Ingestion & Auto Loader (Bronze)

### Objectives

- Understand Auto Loader (`cloudFiles`) options and behavior.
- Be able to describe **incremental ingestion** and schema management.

### Steps

1. **Read Bronze ingestion code**

   - File: `src/bronze/ingest_sleep_edf.py`
   - Focus on:
     - `.format("cloudFiles")`
     - `.option("cloudFiles.format", "binaryFile")`
     - `.option("cloudFiles.schemaLocation", "/mcp/bronze_schema_checkpoint")`
     - `.option("cloudFiles.useNotifications", "false")`
   - For each option, add a **one-line comment** explaining its purpose.

2. **Exercise: add a second ingestion table**

   Goal: show that you can ingest both EDF and metadata.

   - Create a new DLT table in the same file:

   ```python
   @dlt.table(
       name="bronze_sleep_metadata",
       comment="Subject-level metadata ingested from JSON files.",
       table_properties={"quality": "bronze"}
   )
   def bronze_sleep_metadata():
       return (
           spark.readStream
               .format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", "/mcp/bronze_metadata_schema")
               .option("recursiveFileLookup", "true")
               .load("/Volumes/sleep_eeg_lakehouse/bronze/sleep_edf_metadata")
       )
   ```

   - This mirrors common exam questions where you ingest multiple file types.

3. **Delta fundamentals checkpoint**

   - In `notebooks/02_run_dlt_pipeline.py`, add a small cell:

   ```python
   # Inspect Bronze table
   spark.sql("SELECT * FROM bronze.bronze_sleep_edf_raw LIMIT 5").display()

   # Show table details
   spark.sql("DESCRIBE HISTORY bronze.bronze_sleep_edf_raw").display()
   ```

4. **Reflection**

   - Write a short answer: “What are the advantages of Auto Loader vs classic `spark.read` for this use case?”

---

## Day 4 – Delta Lake Modeling & Time Travel

### Objectives

- Explain Delta features: ACID, schema enforcement/evolution, time travel.
- Show where you use `OPTIMIZE` and `ZORDER`.

### Steps

1. **Study existing usage**

   - File: `notebooks/03_analyze_tda_features.py`
   - Find the `OPTIMIZE gold.gold_ml_features ZORDER BY (...)` command.

2. **Exercise: add time travel examples**

   In `03_analyze_tda_features.py`, add:

   ```python
   # Time travel example: view previous version of gold_ml_features
   spark.sql("SELECT VERSION, TIMESTAMP FROM DESCRIBE HISTORY gold.gold_ml_features").display()

   # Replace <N> with a real version number from history
   spark.sql("SELECT * FROM gold.gold_ml_features VERSION AS OF <N> LIMIT 10").display()
   ```

3. **Exercise: schema evolution thought experiment**

   - Add a markdown cell describing how you would:
     - Add a new column `artifact_version` to `gold_ml_features` using `ALTER TABLE ADD COLUMNS`.
     - Backfill values via an `UPDATE` statement.

4. **Reflection**

   - Prepare a 2–3 sentence explanation: “When would you use ZORDER instead of partitioning?” using `subject_id`, `betti_1`, and `spindle_density_per_min` as examples.

---

## Day 5 – Data Processing & Transformations (Silver)

### Objectives

- Understand complex PySpark transformations and UDFs.
- Be comfortable reading and explaining nested schemas.

### Steps

1. **Study `silver` preprocessing code**

   - Files:
     - `src/silver/preprocess_eeg.py`
     - `src/silver/detect_sleep_events.py`
   - Identify:
     - UDF definitions (`preprocess_eeg_signal`, `detect_spindles_yasa`, `detect_slow_oscillations_yasa`).
     - Return schemas (`preprocessed_schema`, `spindle_event_schema`, `so_event_schema`).

2. **Exercise: write your own small UDF**

   In `preprocess_eeg.py`, add a simple UDF that computes **signal energy** for a channel:

   ```python
   from pyspark.sql.types import DoubleType

   def compute_signal_energy(eeg_data: list) -> float:
       import numpy as np
       if not eeg_data:
           return 0.0
       arr = np.array(eeg_data[0])  # first channel
       return float(np.sum(arr ** 2))

   signal_energy_udf = F.udf(compute_signal_energy, DoubleType())
   ```

   Then in `silver_eeg_preprocessed()`:

   ```python
   .withColumn("signal_energy", signal_energy_udf(F.col("data")))
   ```

   This mimics the kind of transformation logic you might be tested on.

3. **Exercise: simple window aggregation in a notebook**

   In a new or existing notebook, after loading `silver_eeg_preprocessed`, compute average `signal_energy` per `subject_id`:

   ```python
   df = spark.table("silver.silver_eeg_preprocessed")
   avg_energy = df.groupBy("subject_id").agg(F.avg("signal_energy").alias("avg_signal_energy"))
   avg_energy.display()
   ```

4. **Reflection**

   - Answer: “When would you use a Python UDF vs pure Spark SQL functions? What are the trade-offs?”

---

## Day 6 – Delta Live Tables: Semantics & Data Quality

### Objectives

- Understand DLT semantics: `@dlt.table`, `dlt.read`, `dlt.read_stream`.
- Explain expectations and the event log.

### Steps

1. **Review all DLT decorators**

   - Bronze: `bronze_sleep_edf_raw` and `bronze_edf_metadata`.
   - Silver: `silver_eeg_preprocessed`, `silver_sleep_spindles`, `silver_slow_oscillations`, `silver_pac`.
   - Gold: `gold_tda_features`, `gold_ml_features`.

2. **Exercise: add a materialized view / derived table**

   In `src/gold/create_ml_features.py`, add a simple derived DLT table:

   ```python
   @dlt.table(
       name="gold_tmci_view",
       comment="View of key TMCI features per subject.",
       table_properties={"quality": "gold"},
       partition_cols=["subject_id"]
   )
   def gold_tmci_view():
       return dlt.read("gold_ml_features").select(
           "subject_id",
           "betti_1",
           "spindle_density_per_min",
           "pac_modulation_index",
           "tmci"
       )
   ```

3. **Exercise: event log query**

   In `notebooks/02_run_dlt_pipeline.py`, extend the event log query:

   ```python
   spark.sql("""
   SELECT
     timestamp,
     details:flow_progress.data_quality.expectations.name AS expectation_name,
     details:flow_progress.data_quality.expectations.passed_records,
     details:flow_progress.data_quality.expectations.failed_records
   FROM event_log(TABLE(bronze.bronze_sleep_edf_raw))
   WHERE details:flow_progress.data_quality IS NOT NULL
   ORDER BY timestamp DESC
   LIMIT 50
   """).display()
   ```

4. **Reflection**

   - Prepare to answer: “What is the difference between `dlt.read` and `dlt.read_stream`? When would you use each?”

---

## Day 7 – Testing, CI/CD, and Bundles

### Objectives

- Understand how tests and CI/CD tie into the exam’s CI/CD & Git domain.
- Be able to talk about how you validate pipelines before deployment.

### Steps

1. **Study tests**

   - Files: `tests/test_bronze.py`, `tests/test_silver.py`, `tests/test_gold.py`.
   - Understand how these tests validate:
     - Subject ID extraction
     - File type detection
     - Spindle density calculation
     - TDA feature validity

2. **Exercise: add one more test**

   - Example in `test_silver.py`:

   ```python
   def test_signal_energy_non_negative(self):
       energy_values = [0.0, 10.5, 100.0]
       for e in energy_values:
           assert e >= 0
   ```

3. **Study CI workflow**

   - File: `.github/workflows/ci.yml`
   - Identify steps:
     - Install deps
     - Lint (`flake8`, `black`)
     - Run tests (`pytest`)
     - Validate bundle (`databricks bundle validate`)

4. **Reflection**

   - Practice: “How do you ensure only valid changes are deployed to Databricks?” → Walk through CI steps + bundle validation.

---

## Day 8 – Governance & Security with Unity Catalog

### Objectives

- Be able to explain how you secure and govern data in this project.

### Steps

1. **Revisit UC setup**

   - `notebooks/01_setup_unity_catalog.py` and `src/utils/config.py`.

2. **Exercise: add pseudo-GRANT statements**

   Add a markdown cell to the notebook with sample GRANTs:

   ```sql
   GRANT SELECT ON TABLE gold.gold_ml_features TO `ml_team`;
   GRANT SELECT ON SCHEMA silver TO `data_scientists`;
   REVOKE ALL PRIVILEGES ON SCHEMA bronze FROM `analysts`;
   ```

3. **Exercise: extend `config.py`**

   - Add comments or a small method describing how you would implement row-level security (e.g., via views or attribute-based filters).

4. **Reflection**

   - Prepare: “How does Unity Catalog improve governance compared to legacy workspace-local tables?”

---

## Day 9 – Performance & Cost Optimization

### Objectives

- Understand how your project uses Spark/Delta features to optimize performance.

### Steps

1. **Review performance-related configs**

   - File: `src/utils/config.py`, class `PipelineConfig`.
   - Understand:
     - `spark.databricks.delta.optimizeWrite.enabled`
     - `spark.databricks.delta.autoCompact.enabled`
     - `spark.sql.adaptive.enabled`

2. **Exercise: add broadcast hint (for learning)**

   In `gold/create_ml_features.py`, annotate where broadcast joins might help:

   ```python
   tda = dlt.read("gold_tda_features").alias("tda")
   spindles = dlt.read("silver_sleep_spindles").alias("sp")

   # In a real workload, if spindles is small, we could hint broadcast:
   # joined = tda.join(F.broadcast(spindles), "subject_id", "inner")
   ```

3. **Exercise: document optimization choices**

   - In `README.md`, update the performance section listing AQE, OPTIMIZE/ZORDER, and auto-compact as deliberate choices.

4. **Reflection**

   - Be ready to answer: “How would you reduce cost and improve performance if this pipeline scaled to 10x data?”

---

## Day 10 – End-to-End Story & Whiteboard Practice

### Objectives

- Be able to explain the full pipeline end-to-end in ≤5 minutes.

### Steps

1. **Write an end-to-end narrative**

   - Create `exam-prep/end-to-end-story.md` with sections:
     - Problem & data
     - Architecture (Bronze/Silver/Gold)
     - DLT pipeline and data quality
     - Governance & security
     - Performance considerations

2. **Whiteboard (sketch on paper or tablet)**

   - Draw:
     - Source → Volume → Bronze (Auto Loader) → Silver (processing) → Gold (features) → ML.
     - Label where DLT, Delta, UC, and CI/CD appear.

3. **Speak practice**

   - Talk through the diagram as if in an interview; refine text in `end-to-end-story.md` based on what feels natural.

---

## Day 11 – Full Practice Exam + Gap List

### Objectives

- Simulate the real exam and identify weak domains.

### Steps

1. **Take a full practice exam (outside this repo)**

   - Use Databricks practice exam or a reputable provider.

2. **Log mistakes**

   - In `exam-prep/gap-list.md`, create a table:

   ```markdown
   | Topic | Question Type | My Mistake | Fix in Repo |
   |-------|---------------|-----------|-------------|
   | Auto Loader notifications | Conceptual | Misunderstood when to use file notifications | Add comments in `ingest_sleep_edf.py` |
   ```

3. **Link gaps to repo changes**

   - For each gap, decide how to reinforce it:
     - Add comments
     - Add a small example
     - Add a short notebook cell

---

## Day 12 – Patch Weak Areas via Repo Extensions

### Objectives

- Close the gaps identified on Day 11 using concrete repo changes.

### Steps

1. **Work through each gap**

   - Example: If you missed a DLT question → add a new expectation or a derived table.
   - Example: If you missed a Unity Catalog question → extend `01_setup_unity_catalog.py` with another schema or external location.

2. **Commit and document**

   - Make one commit per theme, e.g. `fix(exam): clarify Auto Loader schemaLocation`.

3. **Update `exam-prep/gap-list.md`**

   - Mark items as “patched” once you have added a concrete example.

---

## Day 13 – Second Practice Exam & Final Adjustments

### Objectives

- Confirm consistent passing performance.

### Steps

1. **Second full practice exam**

   - Again, log gaps and update `gap-list.md`.

2. **Quick repo updates**

   - Only make small clarifications now; avoid major refactors.

3. **Review key code paths**

   - Bronze ingestion
   - One Silver table
   - One Gold table
   - CI workflow
   - UC setup notebook

---

## Day 14 – Light Review & Exam Scheduling

### Objectives

- Consolidate understanding, reduce cognitive load before exam.

### Steps

1. **Light skim**

   - Exam Guide
   - `README.md` sections on exam coverage and STAR story
   - `exam-prep/end-to-end-story.md`

2. **Schedule the exam**

   - Pick a slot within 3–7 days while your memory is fresh.

3. **Optional: interview prep**

   - Turn `end-to-end-story.md` into 3–4 bullet-point answers to common interview questions:
     - “Tell me about a complex pipeline you built.”
     - “How do you ensure data quality?”
     - “How do you productionize Databricks workloads?”

---

## How to Use This Folder

- Treat `exam-prep/` as your **learning journal**.
- Commit updates as you go; this shows progression.
- Before interviews, re-read:
  - `end-to-end-story.md`
  - `gap-list.md` (to avoid old mistakes)
  - The mapping table you created on Day 1.

This way, the **Sleep EEG Lakehouse** repo becomes both your **Databricks exam lab** *and* your **flagship portfolio project** for senior data engineering roles.

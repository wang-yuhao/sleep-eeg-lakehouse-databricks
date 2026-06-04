# Databricks notebook source
# MAGIC %md
# MAGIC # Analyze TDA Features and Memory Consolidation
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Explores topological features (Betti numbers, persistence)
# MAGIC 2. Correlates features with sleep oscillations and PAC
# MAGIC 3. Computes novel TMCI (Topological Memory Consolidation Index)
# MAGIC 4. Prepares data for machine learning
# MAGIC 
# MAGIC **Research Question:**
# MAGIC Does topological complexity (Betti numbers, persistence entropy) predict
# MAGIC memory consolidation success better than traditional metrics alone?
# MAGIC 
# MAGIC **Exam Coverage:**
# MAGIC - Data Processing (30%): Aggregations, window functions, joins
# MAGIC - Data Modeling (20%): OPTIMIZE, ZORDER, caching
# MAGIC - Databricks Tooling (20%): MLflow, feature tables

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

# Set plot style
sns.set_theme(style="whitegrid")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Gold Feature Table

# COMMAND ----------

df = spark.table("gold.gold_ml_features").toPandas()
print(f"Total subjects: {len(df)}")
df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Descriptive Statistics

# COMMAND ----------

df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Distributions

# COMMAND ----------

fig, axes = plt.subplots(2, 3, figsize=(15, 10))

# Betti numbers
axes[0, 0].hist(df['betti_1'], bins=20, edgecolor='black')
axes[0, 0].set_title('Betti 1 (Loops)')
axes[0, 0].set_xlabel('Count')

axes[0, 1].hist(df['betti_2'], bins=20, edgecolor='black')
axes[0, 1].set_title('Betti 2 (Voids)')
axes[0, 1].set_xlabel('Count')

# Persistence metrics
axes[0, 2].hist(df['persistence_entropy'], bins=20, edgecolor='black')
axes[0, 2].set_title('Persistence Entropy')
axes[0, 2].set_xlabel('Shannon Entropy')

# Sleep oscillations
axes[1, 0].hist(df['spindle_density_per_min'], bins=20, edgecolor='black')
axes[1, 0].set_title('Spindle Density')
axes[1, 0].set_xlabel('Spindles/min')

axes[1, 1].hist(df['pac_modulation_index'], bins=20, edgecolor='black')
axes[1, 1].set_title('PAC Modulation Index')
axes[1, 1].set_xlabel('MI')

# TMCI
axes[1, 2].hist(df['tmci'], bins=20, edgecolor='black')
axes[1, 2].set_title('TMCI (Topological Memory Index)')
axes[1, 2].set_xlabel('Composite Score')

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correlation Analysis

# COMMAND ----------

# Compute correlation matrix
feature_cols = [
    'betti_1', 'betti_2', 'persistence_entropy',
    'spindle_density_per_min', 'pac_modulation_index', 'tmci'
]
corr_matrix = df[feature_cols].corr()

# Plot heatmap
plt.figure(figsize=(10, 8))
sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, 
            square=True, linewidths=1, cbar_kws={"shrink": 0.8})
plt.title('Feature Correlation Matrix')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Hypothesis: Betti 1 vs Memory Proxy
# MAGIC 
# MAGIC **Hypothesis:** Higher Betti 1 (more topological loops) correlates with
# MAGIC stronger memory consolidation (higher spindle density × PAC).

# COMMAND ----------

# Create memory consolidation proxy
df['memory_proxy'] = df['spindle_density_per_min'] * df['pac_modulation_index']

# Scatter plot
plt.figure(figsize=(10, 6))
plt.scatter(df['betti_1'], df['memory_proxy'], alpha=0.6, s=100)
plt.xlabel('Betti 1 (Topological Loops)', fontsize=12)
plt.ylabel('Memory Proxy (Spindle Density × PAC)', fontsize=12)
plt.title('Topological Complexity vs Memory Consolidation Proxy', fontsize=14)
plt.grid(alpha=0.3)

# Add regression line
from scipy.stats import linregress
slope, intercept, r_value, p_value, std_err = linregress(df['betti_1'], df['memory_proxy'])
line = slope * df['betti_1'] + intercept
plt.plot(df['betti_1'], line, 'r--', linewidth=2, 
         label=f'R²={r_value**2:.3f}, p={p_value:.4f}')
plt.legend()
plt.show()

print(f"Correlation: r={r_value:.3f}, p={p_value:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Table for ML Queries

# COMMAND ----------

# Run OPTIMIZE with ZORDER for common filter/join columns
spark.sql("""
OPTIMIZE gold.gold_ml_features
ZORDER BY (betti_1, spindle_density_per_min, pac_modulation_index)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export for MLflow

# COMMAND ----------

# Save as Delta table with feature store metadata
spark.createDataFrame(df).write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.ml_features_v1")

print("✅ Feature table ready for ML training!")
print("Next: Train memory prediction model using MLflow")

# COMMAND ----------
Act as a Senior Alteryx Migration Architect, Databricks Solution Architect, and PySpark Optimization Expert.

## OBJECTIVE

Migrate an Alteryx workflow to Databricks. You are provided with:
1. The original Alteryx .yxmd XML workflow.
2. A partially converted Databricks Python/PySpark notebook.
3. Legacy-to-cloud source and target mapping information, which may be supplied separately.

The existing notebook contains converted transformations, but the source and target table references have NOT been migrated.

Your task is to REVIEW, CORRECT,MODIFY, OPTIMIZE, AND COMPLETE the existing notebook.

## PHASE 1 — ANALYZE THE EXISTING ARTIFACTS

Read the complete YXMD XML and existing Python notebook.
Reconstruct the workflow using tool IDs, connections, and input/output anchors. Do not assume numerical tool ID order represents execution order.
Compare every relevant Alteryx tool against its corresponding Python implementation.
Classify each section as KEEP, OPTIMIZE, FIX, ADD, or BLOCKED.
Preserve correctly converted logic and identify missing transformations, duplicated processing, and unresolved dependencies.
Produce a tool-ID-to-Python traceability matrix.

## PHASE 2 — MIGRATE SOURCE AND TARGET REFERENCES

Identify every legacy input table, view, SQL query, lookup, stored procedure dependency, and output table.
Replace legacy references using ONLY approved cloud mappings and verified schemas.
Support:
- One legacy table mapped to multiple cloud tables.
- Multiple legacy tables mapped to one cloud table.
- Renamed columns and changed data types.
- Split, consolidated, or denormalized cloud datasets.
- Changed keys, filters, and historical-data structures.

For one-to-many mappings, reconstruct the legacy-equivalent dataset using the documented join, union, or transformation. Never assume a join is correct without mapping evidence.
For many-to-one mappings, derive the appropriate logical representation from the consolidated cloud table.
Validate keys, join cardinality, duplicates, and data types.
Use approved Unity Catalog catalog.schema.table names.

Replace legacy target references while preserving output schema, write mode, overwrite scope, INSERT/UPDATE/DELETE/MERGE behavior, incremental logic, and rerun requirements.
Do not invent cloud table names, column mappings, or target write semantics.
Flag unresolved mappings.

## PHASE 3 — ANALYZE NESTED STORED PROCEDURES

Some Alteryx tool IDs execute stored procedures that internally execute other stored procedures.
For every procedure:
1. Identify its originating Alteryx tool ID.
2. Obtain the complete procedure definition.
3. Recursively inspect nested procedures, functions, dynamic SQL, and dependent views.
4. Identify temporary tables, transformations, parameters, output values, transactions, and persistent side effects.
5. Compare the COMPLETE procedure execution chain with the existing notebook.
6. Add or correct only missing or incorrectly converted logic.

Example:
Tool 145 → sp_customer_extract → sp_customer_transform → sp_customer_validate.

Do not assume converting the outer procedure is sufficient.
Do not fabricate unavailable procedure definitions. Mark unresolved dependencies as BLOCKED.

## PHASE 4 — OPTIMIZE DATAFRAME USAGE

HIGH PRIORITY: Do not create a separate named DataFrame for every Alteryx tool.

Consolidate compatible operations such as column renaming, selection, casting, trimming, simple formulas, and filtering into meaningful PySpark stages.

### BAD:

```python
df1 = spark.table("legacy.customer")
df2 = df1.withColumnRenamed("cust_id", "customer_id")
df3 = df2.withColumn("customer_name", F.trim("name"))
df4 = df3.drop("unused_column")
```

### BETTER:

```python
customer_df = (
    spark.table("cloud_catalog.schema.customer")
    .select(
        F.col("cust_id").alias("customer_id"),
        F.trim(F.col("name")).alias("customer_name"),
        F.col("status")
    )
)
```

The example assumes verified cloud mappings and equivalent transformation semantics.

Optimization rules:
- Combine compatible transformations using select(), selectExpr(), or suitable column expressions.
- Remove unused DataFrames and redundant transformations.
- Avoid repeated source reads and duplicate joins.
- Reuse shared logical transformations where beneficial.
- Preserve meaningful boundaries for joins, aggregations, branches, and reusable business logic.
- Avoid unnecessary intermediate Delta tables, temporary views, cache(), persist(), collect(), and toPandas().
- Prefer native PySpark functions over Python UDFs.
- Preserve sequential expression dependencies and transformation order wherever required.

Remember that Spark DataFrames are lazy. A named DataFrame does not automatically materialize data. Optimize redundant computation and the execution plan rather than blindly minimizing variable count.

Never sacrifice correctness for fewer DataFrames.

## PHASE 5 — VERIFY BUSINESS LOGIC

Use the original YXMD as the authoritative workflow reference.
Verify:
- Join matched and unmatched output anchors.
- Union alignment by name or position.
- Filter and Formula expressions.
- Multi-Row Formula ordering, grouping, and boundary handling.
- Unique and deduplication behavior.
- NULL, empty-string, and case-sensitive comparisons.
- Date/time and decimal precision.
- Dynamic Input queries and macros.
- Stored procedure outputs and side effects.

Do not assume the existing notebook is correct simply because it executes.
Do not rewrite verified business logic without a specific reason.

## PHASE 6 — MODIFY THE EXISTING NOTEBOOK

Make targeted changes in this order:
1. Identify missing dependencies and ambiguous mappings.
2. Replace legacy source references.
3. Reconstruct cloud sources where structures differ.
4. Fix missing or incorrect transformations.
5. Complete nested stored procedure logic.
6. Consolidate unnecessary DataFrames.
7. Replace legacy target references.
8. Add validation and reconciliation.

Preserve existing validated code, useful functions, parameters, comments, and notebook organization.
Use native PySpark and Spark SQL.
Do not introduce unnecessary frameworks or external libraries.

## PHASE 7 — VALIDATION

Generate checks for:
- Input and output record counts.
- Schema and data types.
- Null and duplicate counts.
- Join cardinality.
- Aggregate totals.
- Column-level values.
- Stored procedure outputs and side effects.
- Final target reconciliation.
- Incremental processing and rerun behavior.

Use business keys where available and duplicate-aware comparisons otherwise.
Do not claim equivalence unless tests have actually executed against verified reference outputs.
If execution is unavailable, provide validation code and mark tests NOT EXECUTED.

## REQUIRED DELIVERABLES

- A. Migration assessment identifying KEEP, OPTIMIZE, FIX, ADD, and BLOCKED sections.
- B. Legacy-to-cloud source and target mapping report.
- C. Recursive stored procedure dependency report.
- D. Updated and optimized Databricks Python notebook.
- E. Alteryx tool-ID-to-Python traceability matrix.
- F. Summary of changes made to the existing notebook.
- G. Validation code and actual results where available.
- H. Unresolved dependencies, assumptions, and migration blockers.

## NON-NEGOTIABLE RULES

- Do not regenerate the notebook from scratch.
- Do not redo correctly converted and validated transformations.
- Do not create one DataFrame per Alteryx tool.
- Do not assume one-to-one source mappings.
- Do not overlook nested stored procedures.
- Do not invent missing mappings or business logic.
- Do not change target write semantics without verification.
- Do not claim successful validation without executing tests.
- Do not silently remove existing functionality.

## FINAL OBJECTIVE

Deliver a complete, optimized, cloud-native Databricks notebook by modifying and improving the existing partially converted notebook, preserving all verified business logic, applying correct cloud source and target mappings, resolving nested procedure dependencies, and eliminating redundant Spark processing.

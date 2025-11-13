{% docs suggest_clustering_keys %}
## **Macro:** suggest_clustering_keys

This document provides a guide to using and understanding the `suggest_clustering_keys` macro. This tool is designed to help you make data-driven decisions when choosing a clustering key for your Snowflake tables by analyzing column actual query patterns.

### **How to Run:**

Input your model name into the command below and run either in the terminal or in the Studio IDE:

`dbt run-operation suggest_clustering_keys --args '{model_name: your_model_name}'`


### **1. What it Does & How it Works**

The macro performs a two-step analysis and combines the results into a recommendation score to suggest the best clustering key candidates for a given model.

#### Step 1: Cardinality Analysis

The first step is to analyze the physical structure of your data to find columns that are good candidates for clustering.

**What it does:** The macro runs a query that calculates the APPROX_COUNT_DISTINCT for every column in your model. It uses this to determine the cardinality (the number of unique values) and the average number of rows per unique value.

**Why it matters:** A good clustering key should have a cardinality that is not too high (like a unique ID) and not too low (like a boolean flag). This analysis finds columns in that "sweet spot." 

#### Step 2: Query Usage Analysis

The second step is to analyze how your model is actually being used in practice. A clustering key is effective when it aligns with your query patterns. 

**What it does:** The macro queries Snowflake's `snowflake.account_usage.query_history` view. It scans the raw SQL text of queries run in the last 30 days to count how many times each candidate column has appeared in a WHERE clause or a JOIN ... ON clause for queries that reference your model.

**Why it matters:** This identifies the columns that your team is actually filtering and joining on, which are the columns that will benefit most from clustering.

#### The Output

The macro combines these two analyses into a recommendation score and prints the top 3 candidates directly to your terminal logs. The output will look like this:

> --- Top 3 Clustering Key Candidates for YOUR_TABLE ---
>
> Sorted by a score combining cardinality and actual query usage from the last 30 days.
>
>YOUR_TABLE has a total row count of X
>  - Candidate 1: COL_1 (Distinct Values: X, Recent JOIN/WHERE Uses: X)
>  - Candidate 2: COL_2 (Distinct Values: X, Recent JOIN/WHERE Uses: X)
>  - Candidate 3: COL_3 (Distinct Values: X, Recent JOIN/WHERE Uses: X)


### **2. How to Customize the Macro**

You can easily tweak the macro's logic to fit your specific needs.

**Lookback Period:** To analyze more or less than 30 days of query history, change the -30 value in line 69:

`where start_time >= dateadd('day', -30, current_timestamp())`


**Number of Recommendations:** To see more than the top 3 candidates, change the 3 in line 104 at the end of the macro:

`{% if loop.index <= 3 %}`


**Cardinality Filtering:** You can adjust the rules for what the macro considers a "good" candidate by editing the WHERE clause in the cardinality_sql block.

- To include columns with very low cardinality (like booleans or status flags), you can lower or remove this filter: `and cs.distinct_values > 10` in line 48.

### 3. How to Interpret the Results

The results provide a data-driven starting point, but *the final decision should always incorporate your business knowledge* and be tested on a sample table before applying to production tables.

**Selecting a Single Clustering Key**

The top candidate (Candidate 1), especially if it has the highest number of "Recent JOIN/WHERE Uses", is a strong candidate for your primary clustering key, if selecting more than one. This indicates that clustering on this key will directly speed up your most common query patterns.

**Selecting Multiple Clustering Keys**

Snowflake supports multi-column (compound) clustering keys. This is useful when your queries often filter on more than one column.

**When to use:** If your top 2-3 candidates are all frequently used together in WHERE clauses or JOINS (e.g., WHERE col_1 ... AND col_2 ...), a compound key can be very effective.

**How to order them:** The order of the columns matters. As a general rule, you should place the column with the *lowest cardinality* (the highest number of rows per value) first. 

### 4. Implementation and Testing Workflow

It is not best practice to apply a clustering key directly to a large production table without testing. Here is a safe, recommended workflow:

**Step 1: Create and Cluster a Sample Test Table**

First, create a smaller, representative sample of your production table. Using Bernoulli sampling is an efficient way to do this.

[Further Reading: Snowflake Sampling Documentation](https://docs.snowflake.com/en/sql-reference/constructs/sample)

Example: Create a new dbt model to sample your fct_order_items table.

> -- models/sandbox/fct_order_items_sample.sql
```
-- Creates a 10% sample
{{
    config(
        materialized="table",
        cluster_by=['order_date'] -- Your chosen clustering key
    )
}}

select * from {{ ref('model_name') }} tablesample bernoulli (10); 
```


Run 
`dbt run --select model_name` to build this clustered sample table.

**Step 2: Check the Clustering Health**

After the table is built, query Snowflake's SYSTEM$CLUSTERING_INFORMATION function to see how well-organized the data is.

Query:
```
-- 
select system$clustering_information('FCT_ORDER_ITEMS_SAMPLE');
```

[Further Reading: SYSTEM$CLUSTERING_INFORMATION Docs](https://docs.snowflake.com/en/sql-reference/functions/system_clustering_information)


What to look for:
- A healthy clustered table will have a low **clustering_depth**. A depth of 1.0 is perfect, meaning the data is perfectly sorted with no overlapping micropartitions.
- The **total_constant_partition_count** indicates how many of the micro-partitions would not benefit from reclustering. You want this number to be close to the **total_partition_count**. 

**Step 3: Apply to Production**

Once you have confirmed that your chosen key results in healthy clustering on your sample table, you can confidently add the cluster_by configuration to your main production model.

### 5. General Clustering Recommendations

- Which tables should I cluster? 
    - Focus on your largest tables that are frequently queried.

- How can I select the best key? 
    - Balance the data-driven suggestions from this macro with your business and usage knowledge. 

Always test first. As shown in the workflow above, always validate your chosen key on a smaller sample table before applying it to production to understand its impact and cost.

[For Deeper Reading: Snowflake's Official Clustering Documentation](https://docs.snowflake.com/en/user-guide/tables-clustering-keys)
{% enddocs %}
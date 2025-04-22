# Databricks-PySpark-Customer-Data-Pipeline


## Platform and Technical Details
- **Platform**: Databricks
- **Language**: PySpark (Spark 3.3.2)
- **Environment**: Databricks Shell with local[8] master
- **Data Flow**: 
  1. Uploaded local data files to Databricks
  2. Processed using PySpark DataFrames
  3. Generated and downloaded results as a zip file

## Overview
This project analyzes customer data using PySpark on Databricks to extract valuable insights about customer behavior, order patterns, and spending habits. The analysis focuses on identifying customer segments based on order frequency and spending patterns.

## Workflow

### 1. Data Loading & Exploration
I loaded customer data into a Spark DataFrame containing fields like customer_id, name, city, state, country, registration_date, and is_active status.

Sample data:
```
|customer_id| name      | city     | state      |country|registration_date|is_active|
|-----------+----------+----------+------------+-------+-----------------+---------|
| 0         |Customer_0 | Pune     |Maharashtra | India | 2023-06-29      | False   |
| 1         |Customer_1 |Bangalore | Tamil Nadu | India | 2023-12-07      | True    |
| 2         |Customer_2 |Hyderabad | Gujarat    | India | 2023-10-27      | True    |
| 3         |Customer_3 |Bangalore | Karnataka  | India | 2023-10-17      | False   |
| 4         |Customer_4 |Ahmedabad | Karnataka  | India | 2023-03-14      | False   |
```

### 2. Data Enrichment
Added temporal dimensions to the data for time-based analysis:
- Extracted registration year and month from registration dates
- Created additional metrics for analysis using:
  ```python
  df1 = df1.withColumn('registration_year', year(col('registration_date'))) \
           .withColumn('registration_month', month(col('registration_date')))
  ```

### 3. Geographic Analysis
Performed aggregation by geographic regions to understand customer distribution:
```python
df1.groupBy('state', 'country').count().orderBy(col('count').desc()).show()
```

### 4. Customer Ranking
Applied window functions to rank customers by registration date within each state:
```python
window_spec = Window.partitionBy('state').orderBy(col('registration_date').desc())

df1 = df1.withColumn('rank', rank().over(window_spec)) \
         .withColumn('dense_rank', dense_rank().over(window_spec)) \
         .withColumn('row_number', row_number().over(window_spec))
```

### 5. Order Analysis
Analyzed customer ordering patterns:
- Counted total orders per customer
- Identified top customers by order count
```python
customers_orders_count = customers_orders_df.groupBy('customer_id').count() \
                                          .orderBy(col('count').desc())
```

### 6. Spending Pattern Analysis
Identified interesting customer segments:
- Customers with high order frequency but low total spend
- Monthly spending patterns

### 7. Data Export and Download
The output files were downloaded to my local machine, and the zip file is available at: 
https://github.com/Bhawnadhaka/Databricks-PySpark-Customer-Data-Pipeline/blob/main/final_output%20(1).zip

## Key Findings
- Identified geographic distribution of customers across Indian states
- Ranked customers by registration date within each state
- Discovered customer segments with high order frequency but low spending
- Generated temporal patterns in customer registration and ordering behavior


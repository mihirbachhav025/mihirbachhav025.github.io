# <p align="center">SQL Window Functions</p> #  

Sql window functions are a set of powerful analytical functions that help us to do row level computations.  
They differ from group by function by maintaining the row level granularity while performing calculations like aggregation at same time.As a result window functions are helpful in advanced data analysis  

Simple Example of Granularity Preservation:  

Here is a sample dataset:

| id | Day     | Product | Sales |
|----|---------|---------|-------|
| 1  | Monday  | Milk    | 20    |
| 2  | Monday  | Milk    | 30    |
| 3  | Tuesday | Bread   | 5     |
| 4  | Tuesday | Bread   | 6     |

If we implement a `GROUP BY` function to check total sales of each product till Tuesday:

```sql
SELECT Product, SUM(Sales) AS Total_Sales
FROM sales_table
GROUP BY Product;
```

**Output:**

| Product | Total_Sales |
|---------|-------------|
| Milk    | 50          |
| Bread   | 11          |

Notice that the result aggregates the sales per product and does not preserve the original row granularity.We are getting 2 rows instead of the original 4 rows  

But if we use the `Window function` instead:

```sql
SELECT Day, Product, Sales, SUM(Sales) OVER (PARTITION BY Product) AS TotalSales
FROM sales_table;
```

**Output:**

| Day     | Product | Sales | TotalSales |
|---------|---------|-------|------------|
| Monday  | Milk    | 20    | 50         |
| Monday  | Milk    | 30    | 50         |
| Tuesday | Bread   | 5     | 11         |
| Tuesday | Bread   | 6     | 11         |

Notice how the window function preserves the original row granularity while still providing the aggregated total for each product.  

*Note: We cannot use Day in the select part of group by function as it will change granularity*

<hr>  

#### 1. Aggregate Functions

These functions perform calculations across a set of table rows that are somehow related to the current row. Common aggregate window functions include:

- `SUM(EXPR)`, `AVG(EXPR)`, `COUNT(EXPR)`, `MIN(EXPR)`, `MAX(EXPR)`

#### 2. Rank Functions

Rank functions assign a rank or row number to each row within a partition of a result set. They are useful for ordering and ranking data.

- `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `CUME_DIST()`,  `PERCENT_RANK()`, `NTILE(n)`.  

#### 3. Value Functions

Value functions return a value from another row in the window frame relative to the current row. These are useful for comparing values between rows.

- `LAG(EXPR, offset, default)`, `LEAD(EXPR, offset, default)`,  `FIRST_VALUE(EXPR)`, `LAST_VALUE(EXPR)`, `NTH_VALUE(EXPR, n)`

*Note:*  
1. Window Fucntion are allowed only in the select or order by part of sql query  

2. Window Fucntions cannot be nested  

3. Window Functions are executed after the where clause (if present )  

4. Window Function can be used together with group by only if both have same columns(Since group by has a fixed granularity)  

<hr>
  
Let's first understand the basic structure of a window function query with this example
```sql
AVG(Sales) OVER (PARTITION BY Product ORDER BY Day ROWS UNBOUNDED PRECEDING)
```
- `AVG(Sales)`: Window Function Action(Expression) - Calculation performed on the window.The expression can contain column name,be empty,contain integer value,conditional logic or multiple arguments depending upon the window function being used.  
  

- `OVER`: Indicates the use of a window function and defines the window.  

- `PARTITION BY `: Divides the data into partitions based upon the passed columns(we can pass multiple columns),Calculation then takes place on this partitions individually.It is an optional clause,if we dont pass it over will consider the whole dataset as one window
  

- `ORDER BY `: Orders rows within each partition.This clause is optional in aggregation but complusory in rank and value functions
  

- `ROWS UNBOUNDED PRECEDING`: This is known as the frame clause and it defines a subset od rows within each window that is to be considered for the calculation.I'll be explaining the frame clause in detail below
<hr>

### Deep Dive into the Frame Clause ###  

Frame clause allows us to define a scope inside the window, so calculations are performed only on a specific subset of rows within each partition.

Below, the highlighted cells show how the running total is calculated for each row using:

```
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
```

| Day        | Product | Sales | RunningTotal Calculation           | RunningTotal |
|------------|---------|-------|------------------------------------|--------------|
| `Monday`    | `Milk`    | `20`    | 20                                 | 20           |
| `Tuesday`   | `Milk`    | `15`    | 20 + 15                            | 35           |
| `Wednesday` | `Milk`    | `25`    | 20 + 15 + 25                       | 60           |
| `Monday`    | `Bread`   | `5`     | 5                                  | 5            |
| `Tuesday`   | `Bread`   | `6`     | 5 + 6                              | 11           |
| `Wednesday` | `Bread`   | `8`     | 5 + 6 + 8                          | 19           |

- The **RunningTotal** for each row is the sum of all `Sales` values for the same `Product` from the start of the partition up to the current row.
- The highlighted rows show which data is included in the calculation for each step.

> **Tip:** You can visualize the frame as a moving window that expands from the first row of each partition to the current row.

Parts of a Frame Clause:

```
ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING 
```
- Rows : Defines the Frame Type  
    - Rows
    - Range
- Current Row : Frame Boundary(Lower Value)
    - Current Row
    - N Preceding
    - Unbounded Preceding
- UnBounded Preceding : Frame Boundary(Higher Value)
    - Current Row
    - N Following
    - Unbounded Following

**Important Rules:**

1. The frame clause can only be used together with the `ORDER BY` clause.
2. The lower boundary must come before the higher boundary in the frame specification.

<hr>  

Now Let's take a look at each window function in Detail.We will be using sparksql and pyspark to query the data

## Aggregate Window Functions: ##

- `SUM(EXPR)`: Calculates the sum of values.
- `AVG(EXPR)`: Calculates the average value.
- `COUNT(EXPR)`: Counts the number of rows.
- `MIN(EXPR)`: Finds the minimum value.
- `MAX(EXPR)`: Finds the maximum value.  

1. `PARTITION BY`, `ORDER BY`, and the `Frame Clause` are all optional for aggregate window functions. If omitted, the function operates over the entire result set.
2. The data type of the expression should be numeric for `SUM` and `AVG`, and any comparable type (numeric, date, etc.) for `MIN` and `MAX`.
3. The Frame Clause can only be used if `ORDER BY` is specified.If `ORDER BY` is specified a default frame clause of `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is applied(also called as running Total)

[Data](https://github.com/mihirbachhav025/mihirbachhav025.github.io/blob/7d667919c683316e188c59a8991c7e72519812b2/codes/Generate%20Sample%20Data.py) :

| id | Date       | Product | City        | Sales |
|----|------------|---------|-------------|-------|
| 1  | 2025-06-01 | Laptop  | New York    | 1200  |
| 2  | 2025-06-01 | Phone   | Los Angeles | 800   |
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  |
| 5  | 2025-06-03 | Phone   | New York    | 850   |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   |  

```
from pyspark.sql.window import Window
from pyspark.sql.functions import count

#Pyspark
sales_df.withColumn('Count', count('*').over(Window.partitionBy())).display()
# Note: We cannot leave over empty in pyspark
#Instead of * we can specify individual column names but null values wont be counted

#Sql
spark.sql(f'''select *,count(*) over() as Count from sales_Df''').display()
```
Output:
| id | Date       | Product | City        | Sales | Count |
|----|------------|---------|-------------|-------|-------|
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 8     |
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 8     |
| 5  | 2025-06-03 | Phone   | New York    | 850   | 8     |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 8     |
| 2  | 2025-06-01 | Phone   | Los Angeles | 800   | 8     |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 8     |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 8     |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 8     |

```
#Pyspark
sales_df.withColumn('Avg_Overall_Sales', avg('Sales').over(Window.partitionBy()))\
    .withColumn('Avg_City_Sales', avg('Sales').over(Window.partitionBy('City')))\
    .withColumn('Max_City_Sales', max('Sales').over(Window.partitionBy('City')))\
    .withColumn('Min_City_Sales',min('Sales').over(Window.partitionBy('City')))\
    .withColumn('Sum_product_sales',sum('Sales').over(Window.partitionBy('Product')))\
    .withColumn('Product_Datewise_sales',sum('Sales').over(Window.partitionBy('Product').orderBy('Date').rowsBetween(Window.unboundedPreceding,Window.currentRow)))\
    .orderBy('Product','Date')\
    .display()


#Sql
spark.sql(f'''select *,
          avg(Sales) over () as Avg_Overall_Sales,
          avg(Sales) over (partition by City) as Avg_City_Sales,
          max(Sales) over (partition by City) as Max_City_Sales,
          min(Sales) over (partition by City) as Min_City_Sales,
          sum(Sales) over (partition by Product) as Sum_product_sales,
          sum(Sales) over (partition by Product order by Date rows between unbounded preceding and current row) as Product_Datewise_sales
          from sales_df order by Product,Date''').display()
```
Output:
| id | Date       | Product | City        | Sales | Avg_Overall_Sales | Avg_City_Sales      | Max_City_Sales | Min_City_Sales | Sum_product_sales | Product_Datewise_sales |
|----|------------|---------|-------------|-------|-------------------|---------------------|----------------|----------------|-------------------|-----------------------|
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 850               | 1025                | 1200           | 850            | 3550              | 1200                  |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 850               | 760                 | 1100           | 400            | 3550              | 2300                  |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 850               | 823.33              | 1250           | 420            | 3550              | 3550                  |
| 2  | 2025-06-01 | Phone   | Los Angeles | 800   | 850               | 823.33              | 1250           | 420            | 2430              | 800                   |
| 5  | 2025-06-03 | Phone   | New York    | 850   | 850               | 1025                | 1200           | 850            | 2430              | 1650                  |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 850               | 760                 | 1100           | 400            | 2430              | 2430                  |
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 850               | 760                 | 1100           | 400            | 820               | 400                   |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 850               | 823.33              | 1250           | 420            | 820               | 820                   |

*We can use the frame clause to calculate running and rolling statistics*  

## RANK Window Functions: ##

In Rank window Functions the Expression part is empty( except NTILE(N) ) ,Partition By is optional and Order By is required and Frame Clause is not allowed

Following Functions generate Integer ranking and are used for Top/Bottom N Analysis:

- `ROW_NUMBER()`: Assigns a unique sequential integer to each row within a partition, starting at 1.

```
#Pyspark
sales_df.withColumn('RowNumber', row_number().over(Window.orderBy('Sales'))).display()
sales_df.withColumn('RowNumber_partitioned', row_number().over(Window.partitionBy('City').orderBy('Sales'))).display()

#Sql
spark.sql(f''' select *,row_number() over (order by Sales) as RowNumber from sales_df''').display()
spark.sql(f''' select *,row_number() over (partition by City order by Sales) as RowNumber_partitioned from sales_df''').display()

```
Output:
| id | Date       | Product | City        | Sales | RowNumber |
|----|------------|---------|-------------|-------|-----------|
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 1         |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 2         |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 3         |
| 2  | 2025-06-01 | Phone   | Los Angeles | 800   | 4         |
| 5  | 2025-06-03 | Phone   | New York    | 850   | 5         |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 6         |
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 7         |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 8         |  
 
<br>
<br>

| id | Date       | Product | City        | Sales | RowNumber_partitioned |
|----|------------|---------|-------------|-------|-----------------------|
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 1                     |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 2                     |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 3                     |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 1                     |
| 2  | 2025-06-01 | Phone   | Los Angeles | 800   | 2                     |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 3                     |
| 5  | 2025-06-03 | Phone   | New York    | 850   | 1                     |
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 2                     |



- `RANK()`: Assigns a rank to each row within a partition, with gaps in the ranking for tied values.

```
#Pyspark
sales_df.withColumn('rank', rank().over(Window.orderBy('Sales'))).display()
sales_df.withColumn('rank_partitioned', rank().over(Window.partitionBy('City').orderBy('Sales'))).display()

#Sql
spark.sql(f''' select *,rank() over (order by Sales) as rank from sales_df''').display()
spark.sql(f''' select *,rank() over (partition by City order by Sales) as rank_partitioned from sales_df''').display()
```

Output:
| id | Date       | Product | City        | Sales | rank |
|----|------------|---------|-------------|-------|-----------|
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 1         |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 2         |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 3         |
| 2  | 2025-06-01 | Phone   | Los Angeles | 850   | `4`         |
| 5  | 2025-06-03 | Phone   | New York    | 850   | `4`          |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 6         |
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 7         |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 8         |

***Notice that it tied for 4<sup>th</sup> place,hence rank 5 was skipped and 6 followed 4***

| id | Date       | Product | City        | Sales | rank_partitioned |
|----|------------|---------|-------------|-------|-----------------------|
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 1                     |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 2                     |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 3                     |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 1                     |
| 2  | 2025-06-01 | Phone   | Los Angeles | 850   | 2                     |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 3                     |
| 5  | 2025-06-03 | Phone   | New York    | 850   | 1                     |
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 2                     |

- `DENSE_RANK()`: Similar to `RANK()`, but does not leave gaps in the ranking for ties.

```
#Pyspark
sales_df.withColumn('dense_rank', dense_rank().over(Window.orderBy('Sales'))).display()
sales_df.withColumn('dense_rank_partitioned', dense_rank().over(Window.partitionBy('City').orderBy('Sales'))).display()

#Sql
spark.sql(f''' select *,dense_rank() over (order by Sales) as dense_rank from sales_df''').display()
spark.sql(f''' select *,dense_rank() over (partition by City order by Sales) as dense_rank_partitioned from sales_df''').display()
```

Output:

| id | Date       | Product | City        | Sales | dense_rank |
|----|------------|---------|-------------|-------|------------|
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 1          |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 2          |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 3          |
| 5  | 2025-06-03 | Phone   | New York    | 850   | `4`          |
| 2  | 2025-06-01 | Phone   | Los Angeles | 850   | `4`        |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | `5`         |
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 6          |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 7          |

***Notice that it tied for 4<sup>th</sup> place,then next rank was 5 hence no gap was left***  

| id | Date       | Product | City        | Sales | dense_rank_partitioned |
|----|------------|---------|-------------|-------|-----------------------|
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 1                     |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 2                     |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 3                     |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 1                     |
| 2  | 2025-06-01 | Phone   | Los Angeles | 850   | 2                     |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 3                     |
| 5  | 2025-06-03 | Phone   | New York    | 850   | 1                     |
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 2                     |

<br>

- `NTILE(n)`: Divides the result set into `n` approximately equal groups (tiles) and assigns a group number to each row.If division is not perfect the larger group will come first

```
#Pyspark
sales_df.withColumn('ntilebucket_1', ntile(1).over(Window.orderBy('Sales'))).\
    withColumn('ntilebucket_3', ntile(3).over(Window.orderBy('Sales'))).\
    withColumn('ntilebucketpartitioned_2', ntile(2).over(Window.partitionBy('Product').orderBy('Sales'))).\
    display()

#Sql
spark.sql(f''' select *,ntile(1) over (order by Sales) as ntilebucket_1,ntile(3) over (order by Sales) as ntilebucket_3,ntile(2) over (partition by Product order by Sales) as ntilebucketpartitioned_2 from sales_df''').display()
```

| id | Date       | Product | City        | Sales | ntilebucket_1 | ntilebucket_3 | ntilebucketpartitioned_2 |
|----|------------|---------|-------------|-------|---------------|---------------|--------------------------|
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 1             | 2             | 1                        |
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 1             | 3             | 1                        |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 1             | 3             | 2                        |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 1             | 1             | 1                        |
| 2  | 2025-06-01 | Phone   | Los Angeles | 850   | 1             | 2             | 1                        |
| 5  | 2025-06-03 | Phone   | New York    | 850   | 1             | 2             | 2                        |
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 1             | 1             | 1                        |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 1             | 1             | 2                        |

<br>


Following functions generate percentage based ranking and are used for distribution analysis:


- `CUME_DIST()`: Calculates the cumulative distribution of a value within a partition, returning a value between 0 and 1 that represents the proportion of rows with values less than or equal to the current row.
In simple terms, `CUME_DIST()` is calculated as:

    CUME_DIST = (Position Number) / (Total number of rows)

    During a tie for position number last occurence row number value is considered


- `PERCENT_RANK()`: Computes the relative rank of a row within a partition as a percentage, ranging from 0 to 1.
In simple terms, `PERCENT_RANK()` is calculated as:

    PERCENT_RANK = (Position Number)-1 / (Total number of rows)-1

    During a tie for position number first occurence row number value is considered

```
#Pyspark
sales_df.withColumn('cume_dist', cume_dist().over(Window.orderBy('Sales'))).\
    withColumn('percent_rank', round(percent_rank().over(Window.orderBy('Sales')), 2)).\
    display()

#Sql
spark.sql(f''' select *,cume_dist() over (order by Sales) as cume_dist,round(percent_rank() over (order by Sales),2) as percent_rank from sales_df''').display()
```

| id | Date       | Product | City        | Sales | cume_dist | percent_rank |
|----|------------|---------|-------------|-------|-----------|--------------|
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 0.125     | 0            |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 0.25      | 0.14         |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 0.375     | 0.29         |
| 2  | 2025-06-01 | Phone   | Los Angeles | 850   | 0.625     | 0.43         |
| 5  | 2025-06-03 | Phone   | New York    | 850   | 0.625     | 0.43         |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 0.75      | 0.71         |
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 0.875     | 0.86         |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 1         | 1            |  
  

***Important Point : If focus is on distribution use cume_dist(),if focus is on relative distance between data points use percent_rank(),cume_dist() is called inclusive function while the other one is known as exclusive function***  
<br>
## VALUE Window Functions: ##

This function help us to access value from another row
EXPR is required(All Datatypes supported).  
Offset is optional(number of rows to lookup forward or backward,default value = 1).  
Default Value is option(Return the default value if next or previous row is not available ,defaults to NULL)



- `LAG(EXPR, offset, default)`: Returns the value of `EXPR` from a previous row at the specified `offset`. If there is no such row, returns `default`.Frame Clause is not allowed

- `LEAD(EXPR, offset, default)`: Returns the value of `EXPR` from a following row at the specified `offset`. If there is no such row, returns `default`.Frame Clause is not allowed

```
window_spec = Window.orderBy(expr("to_date(Date)"))

sales_df.withColumn('lead_1', lead('Sales', 1).over(window_spec)) \
        .withColumn('lead_2', lead('Sales', 2).over(window_spec)) \
        .withColumn('lag_1', lag('Sales', 1).over(window_spec)) \
        .withColumn('lag_2', lag('Sales', 2).over(window_spec))\
        .display()

# Sql

# Spark SQL
spark.sql('''
    SELECT *,
           LEAD(Sales, 1) OVER (ORDER BY to_date(Date)) AS lead_1,
           LEAD(Sales, 2) OVER (ORDER BY to_date(Date)) AS lead_2,
           LAG(Sales, 1) OVER (ORDER BY to_date(Date)) AS lag_1,
           LAG(Sales, 2) OVER (ORDER BY to_date(Date)) AS lag_2
    FROM sales_df
''').display()
```

Output:  

| id | Date       | Product | City        | Sales | lead_1 | lead_2 | lag_1 | lag_2 |
|----|------------|---------|-------------|-------|--------|--------|-------|-------|
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 850    | 400    | null  | null  |
| 2  | 2025-06-01 | Phone   | Los Angeles | 850   | 400    | 1100   | 1200  | null  |
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 1100   | 850    | 850   | 1200  |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 850    | 420    | 400   | 850   |
| 5  | 2025-06-03 | Phone   | New York    | 850   | 420    | 1250   | 1100  | 400   |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 1250   | 780    | 850   | 1100  |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 780    | null   | 420   | 850   |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | null   | null   | 1250  | 420   |

- `FIRST_VALUE(EXPR)`: Returns the first value in the window frame.Frame clause is optional

- `LAST_VALUE(EXPR)`: Returns the last value in the window frame.Frame clause though not mandatory is good to have

- `NTH_VALUE(EXPR, n)`: Returns the value of `EXPR` from the nth row in the window frame. 

```
sales_df.withColumn('first_value', first('Sales').over(window_spec)) \
        .withColumn('last_value', last('Sales').over(window_spec)) \
        .withColumn('last_value_frame', last('Sales').over(window_spec.rowsBetween(Window.currentRow, Window.unboundedFollowing))) \
        .withColumn('nth_value', nth_value('Sales', 3).over(window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
        .display()

spark.sql('''
    SELECT *,
           FIRST_VALUE(Sales) OVER (ORDER BY to_date(Date)) AS first_value,
           LAST_VALUE(Sales) OVER (ORDER BY to_date(Date) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_value,
           LAST_VALUE(Sales) OVER (ORDER BY to_date(Date) ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS last_value_frame,
           NTH_VALUE(Sales, 3) OVER (ORDER BY to_date(Date) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth_value
    FROM sales_df
''').display()
```

Output:
| id | Date       | Product | City        | Sales | first_value | last_value | last_value_frame | nth_value |
|----|------------|---------|-------------|-------|-------------|------------|------------------|-----------|
| 1  | 2025-06-01 | Laptop  | New York    | 1200  | 1200        | 850        | 780              | 400       |
| 2  | 2025-06-01 | Phone   | Los Angeles | 850   | 1200        | 850        | 780              | 400       |
| 3  | 2025-06-02 | Tablet  | Chicago     | 400   | 1200        | 1100       | 780              | 400       |
| 4  | 2025-06-02 | Laptop  | Chicago     | 1100  | 1200        | 1100       | 780              | 400       |
| 5  | 2025-06-03 | Phone   | New York    | 850   | 1200        | 420        | 780              | 400       |
| 6  | 2025-06-03 | Tablet  | Los Angeles | 420   | 1200        | 420        | 780              | 400       |
| 7  | 2025-06-04 | Laptop  | Los Angeles | 1250  | 1200        | 780        | 780              | 400       |
| 8  | 2025-06-04 | Phone   | Chicago     | 780   | 1200        | 780        | 780              | 400       |

<br>
If you see in the result without frame clause the last value is the last value of the default frame,ie undbounded preceding to current row and results in always value of the current row


<br><hr>
## Conclusion ##

SQL window functions are essential tools for advanced analytics, enabling you to perform complex calculations while preserving row-level detail. Practicing these examples with your own data will help reinforce the concepts and unlock new possibilities in your data analysis workflows.

Thank you for reading!

Links for further reading:  
1.[Pyspark Window Function Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)  
2.[TSql Window Function Documentation](https://learn.microsoft.com/en-us/sql/t-sql/queries/select-window-transact-sql?view=sql-server-ver17)
















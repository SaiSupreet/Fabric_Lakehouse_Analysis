Fabric Lakehouse Analysis with PySpark and Spark SQL

Overview

This project provides a comprehensive data analysis solution using Microsoft Fabric Lakehouse, integrating with PySpark, Spark SQL, and visualization libraries such as Matplotlib and Seaborn. The key focus is to demonstrate how data engineers and data scientists can work with big data in a Lakehouse environment, covering data ingestion, processing, and visualization.

Key Features

Data Ingestion: Load CSV files into a Spark DataFrame using predefined schemas for structured data.

Data Transformation: Filter, group, and modify data using Spark DataFrame APIs. Add new columns such as Year, Month, FirstName, and LastName for enriched analysis.

Data Aggregation: Aggregate sales data by product and year to uncover trends, with simple transformations for meaningful insights.

Data Persistence: Save transformed data into partitioned Parquet files for optimized storage and access.

Table Management: Create Spark tables using the Delta format for SQL-based data exploration, supporting versioning and transactions.

SQL Queries: Use Spark SQL for querying and analyzing data in a familiar syntax, enabling flexible and powerful analysis workflows.

Data Visualization: Utilize Matplotlib and Seaborn to visualize aggregated results, providing meaningful insights through intuitive charts.

Project Structure

The project is divided into several steps:

Create a DataFrame from CSV Files: Load CSV data using a schema to create an initial Spark DataFrame.

Filter and Aggregate Data: Utilize DataFrame transformations and filters to get relevant information, e.g., customer counts and product sales.

Transform the DataFrame: Add and modify columns, such as deriving Year and Month from OrderDate and splitting CustomerName.

Save Data as Parquet: Store the transformed data in an efficient, columnar storage format.

Partition Data by Year and Month: Save partitioned data for improved query performance and organization.

Create and Query Spark Tables: Save data as a Delta table and execute SQL queries to get summarized results.

Visualize Data: Plot the data using Matplotlib and Seaborn for quick visual insights.

Prerequisites

Microsoft Fabric Trial: A free trial of Microsoft Fabric is required to run the notebook and access the Spark pools.

Python Libraries: You need to install the following libraries: matplotlib, seaborn, and pandas for data visualization.

Apache Spark: Integrated with Microsoft Fabric to provide a scalable framework for big data analysis.

How to Run

Create a Workspace: Start by creating a new workspace in Microsoft Fabric to manage all your data and notebooks.

Upload Data Files: Add the CSV files (2019.csv, 2020.csv, and 2021.csv) to your Lakehouse.

Execute Notebook Cells: Run the cells in the provided order to load data, transform it, create tables, and visualize the results.

Data Ingestion Example

The following PySpark code loads all CSV files into a DataFrame:

orders_df = spark.read.format("csv").schema(orderSchema).load("Files/orders/*.csv")
display(orders_df)

Visualization Example

To visualize revenue by year:

from matplotlib import pyplot as plt
df_sales = df_spark.toPandas()
plt.clf()
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')
plt.title('Revenue by Year')
plt.xlabel('Year')
plt.ylabel('Revenue')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=45)
plt.show()

Benefits of Using Fabric Lakehouse

Scalability: Built on Azure, Microsoft Fabric offers serverless scalability with Spark pools.

Flexibility: Combines the schema-on-read flexibility of data lakes with SQL-based data processing.

Centralized Data Management: A single location for data storage, management, and access for different analytics roles.

Getting Started

Clone this repository and import it to your Microsoft Fabric environment.

Run through the steps in the notebook to understand how to load, transform, and visualize data.

License

This project is licensed under the MIT License.

Contribution

Feel free to submit pull requests or report issues. Collaboration is highly encouraged!

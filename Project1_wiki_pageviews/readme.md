PAGEVIEW COUNT BASED ON DOMAINS DATA WAREHOUSE

INTRODUCTION
The aim of this project is to develop a data warehouse for analytical purposes, specifically targeting skills at the fresher level. The project focuses on improving batch data collection techniques and the application of ETL/ELT processes using Python and Apache Spark.

PROJECT IDEA:
• Data Source: Pageview count data collected from Wikimedia. https://dumps.wikimedia.org/other/pageviews/
• Data Structure: The dataset spans multiple years, months, and days, with data collected at 1-hour intervals. Files are stored as compressed .gz archives.
• Scope: This project extracts data on pageviews for specific domains and page names, focusing on the following companies: "Google", "Facebook", "Amazon", "Microsoft", "Apple", and "Walmart".
• Objective: The processed data is transformed and loaded into a data warehouse (OLAP) for analysis and insights.

IMPLEMENTATION DETAILS
Script Descriptions
1. create_tables.py:

- Creates the following tables in PostgreSQL:
    dim_table: Stores data for specific domains and page names.
    dim_time: Contains time-related information for easy aggregation and analysis.
    fact_pageviews: A fact table that consolidates the processed pageview data.

2. main.py:

- Runs all related tasks:
- Extracts data from the .gz archives.
- Processes and transforms the data using Python and Apache Spark.
- Loads the data into the data warehouse.

Reference Diagram
Refer to the diagram in "project1_diagram.pdf" for an overview of the data warehouse schema and ETL flow.

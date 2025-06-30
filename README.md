# Amazon Books Data Pipeline with Apache Airflow and PostgreSQL

## Project Overview

This project demonstrates a fully automated **ETL (Extract, Transform, Load)** data pipeline built using **Apache Airflow** for orchestration and **PostgreSQL** as the data warehouse. The pipeline extracts book data from Amazon, processes it, and stores it in a Postgres database for analysis.

While the original scraper targets Amazon, it uses mock data for stable execution due to scraping limitations. The design and implementation follow industry best practices for scalable and maintainable data pipelines.

---

## Key Learnings

- **Apache Airflow:**  
  - Created and scheduled DAGs with clear task dependencies  
  - Utilized `PythonOperator` and `PostgresOperator` for flexible task execution  
  - Managed task retries, failures, and XComs for inter-task communication  

- **Data Extraction & Transformation:**  
  - Implemented data extraction logic (scraping / mock data) in Python  
  - Applied data cleaning and duplicate removal with Pandas  
  - Handled complex data types and casting challenges for database insertion  

- **PostgreSQL:**  
  - Designed normalized table schema to store book metadata  
  - Used Postgres hooks and SQL queries to insert and query data  
  - Performed type casting and error handling for inconsistent input data  

- **Docker & Containerization:**  
  - Containerized the entire stack including Airflow, Postgres, and Redis  
  - Managed container orchestration using `docker-compose` for easy setup  

- **ETL Pipeline Best Practices:**  
  - Structured DAG with Extract, Transform, Load stages clearly separated  
  - Ensured idempotency with table creation checks and data deduplication  
  - Configured Airflow to trigger daily runs and handle retries  

---

## Project Architecture & ETL Workflow

### 1. Extract  
The pipeline fetches book data from Amazon using a Python scraper (currently using mock data to simulate extraction). It collects key attributes such as book title, author, price, and rating.

### 2. Transform  
The raw data is transformed into a clean pandas DataFrame, removing duplicates and ensuring consistent formatting for fields.

### 3. Load  
Cleaned data is loaded into a PostgreSQL database via Airflow's PostgresHook and PostgresOperator. The `books` table stores each book record with schema:

## Data Analysis with PostgreSQL

As part of the project, I performed detailed data analysis using PostgreSQL to gain actionable insights from the dataset. The key analytical questions addressed include:

- **Find Books Whose Rating Differs Significantly from Author Average (Outliers):**  
  Identified books whose ratings deviate substantially from the author’s average rating, helping spot outliers or inconsistencies.

- **Price Distribution: Bucket Books into Price Ranges:**  
  Grouped books into price brackets to understand the pricing landscape and distribution.

- **Top 5 Highest Priced Books with Rating Above 4.5:**  
  Extracted premium books that are highly rated, useful for highlighting premium market offerings.

These analyses utilized advanced SQL techniques such as window functions, aggregations, conditional grouping, and type casting to work with imperfect data formats.

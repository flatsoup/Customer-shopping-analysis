# Customer Shopping Analysis

End-to-end data analytics project demonstrating a full analytics workflow using Python, PostgreSQL, SQL, and Power BI.

## Project Overview

This project analyzes customer shopping behavior to uncover insights related to:
- Revenue drivers
- Subscription impact
- Customer segmentation
- Product performance
- Discount effectiveness

The project covers the complete analytics pipeline:
data ingestion → transformation → database storage → SQL analysis → visualization.

## Tech Stack

- **Python** (Pandas, SQLAlchemy)
- **PostgreSQL**
- **SQL**
- **Power BI**
- **Git & GitHub**

## Project Structure

│
├── code/
│ ├── main.py # Python ETL pipeline
│ └── init.py
│
├── data/
│ └── customer_shopping_behavior.csv
│
├── sql/
│ └── analysis.sql # SQL analytical queries
│
├── Dashboard/
│ ├── customer_analysis.pbix
│ └── dashboard_overview.png
│
├── .gitignore
├── README.md


## ETL Pipeline

The Python ETL process performs:
- Column standardization
- Missing value imputation (review ratings by category)
- Feature engineering (age groups, purchase frequency in days)
- Data loading into PostgreSQL

Run ETL:
```bash
python code/main.py
SQL Analysis

SQL queries answer business questions such as:

Revenue by gender

Subscription vs non-subscription spending

Top-rated products

Discount effectiveness

Customer segmentation (New / Returning / Loyal)

All queries are available in:

sql/analysis.sql

Power BI Dashboard

The Power BI dashboard visualizes:

Revenue distribution

Subscription impact

Category performance

Age group analysis

Dashboard file:

Dashboard/customer_analysis.pbix

Key Skills Demonstrated

Data cleaning and transformation

SQL analytics

Database integration

Business-oriented data visualization

End-to-end analytics project design

Author

Bogdan

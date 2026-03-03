# SQL Datawarehouse Project

This project demonstrates the design and implementation of a modern Data Warehouse using PostgreSQL, integrating CRM and ERP data into a unified analytical environment.

The solution follows the Medallion Architecture (Bronze, Silver, Gold) approach and applies industry best practices in Data Engineering, ETL design, data modeling, and analytics.

![Data Architecture](docs/data_architecture.png)
---

## 🚀 Project Requirements

### Postgres Database
Create the database manually:

```sql
CREATE DATABASE DataWarehouse;
```

and execute using Postgres the SQL scripts in scripts folder in numerical order to generate the Layers.
Order: 001_create_layers_schemas.sql -> 002_create_bronze_tables.sql -> 003_bulk_insert_bronze_layer.sql -> 004_create_silver_tables.sql -> 005_insert_silver_layer.sql -> 006_create_gold_layer_views.sql

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using Postgres to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/daniel-oliveira-30785b1ba)
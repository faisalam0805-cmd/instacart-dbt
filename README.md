🛒 Instacart Market Basket Analytics
End-to-End Modern Data Stack Project

dbt + Airflow + Docker + CI/CD + Power BI

📖 Project Overview

This project implements a production-style analytics engineering pipeline using the Instacart Market Basket dataset.

It simulates a real-world modern data stack:

PostgreSQL as analytical warehouse

dbt for layered transformations

Apache Airflow for orchestration

Docker for containerized infrastructure

GitHub Actions for CI/CD automation

Power BI for executive intelligence dashboards

The objective is to transform raw transactional grocery data into decision-ready business insights across customer, product, department, and time dimensions.

🏗️ Architecture

Raw Data (CSV)
→ PostgreSQL
→ dbt (Staging → Core → Mart)
→ Airflow Orchestration
→ Power BI Dashboards

The pipeline follows modern analytics engineering best practices with clear separation of concerns and automated validation.

🔄 Data Modeling Architecture

The project uses a three-layer dbt architecture.

1️⃣ Staging Layer (stg_)

Purpose:

Source-aligned transformations

Column standardization

Type corrections

Data cleaning

Minimal business logic

Examples:

stg_orders

stg_products

stg_order_products

2️⃣ Core Layer (fct_, dim_)

Purpose:

Define grain explicitly

Implement business logic

Create fact and dimension models

Maintain referential integrity

Fact Tables:

fct_orders

fct_order_products

Dimension Tables:

dim_customers

dim_products

dim_departments

dim_time

This layer forms the analytical foundation.

3️⃣ Mart Layer (mart_)

Purpose:

Business-ready aggregates

KPI-focused datasets

Optimized for BI consumption

Examples:

mart_customer_metrics

mart_product_performance

mart_department_summary

mart_time_analysis

Power BI connects directly to this layer.

📚 dbt Documentation (Live)

Comprehensive dbt documentation is publicly available and includes:

Full model lineage (DAG visualization)

Source-to-mart dependency tracking

Column-level documentation

Test coverage visibility

Fact & dimension relationships

🔗 Live dbt Documentation

👉 https://faisalam0805-cmd.github.io/instacart-dbt/

Documentation is automatically generated and published via the CI/CD pipeline using:

dbt docs generate

This ensures alignment between transformation logic and published lineage.

📊 Executive-Level Metrics

206K Total Customers

3M Total Orders

34M Total Products Sold

37% Overall Reorder Rate

The high reorder rate indicates strong habitual purchasing behavior.

👥 Customer Intelligence Insights

Key Metrics:

16.23 Average Orders per Customer

9.98 Average Basket Size

44% Average Customer Reorder Rate

Insights:

Customers demonstrate strong repeat engagement (~16 lifetime orders per user).

Average basket size (~10 items) indicates bulk purchasing behavior.

Majority of customers fall into Medium Loyalty segment.

A smaller high-value segment contributes disproportionately to order volume.

Positive correlation observed between basket size and reorder frequency.

Business Implications:

Loyalty optimization strategies can significantly increase revenue.

Reorder-based recommendation systems would perform effectively.

Medium-loyalty segment offers high conversion opportunity.

🥑 Product Intelligence Insights

Key Metrics:

34M Product Orders

20M Reorders

59% Product-Level Reorder Rate

50K Distinct Products

Top Products:

Banana

Bag of Organic Bananas

Organic Strawberries

Organic Baby Spinach

Organic Hass Avocado

Observations:

Produce dominates reorder behavior.

Some niche products exhibit very high reorder rates (>85%).

Product quadrant analysis highlights staple vs niche loyalty items.

Business Implications:

Core staples should anchor promotions.

Bundle complementary produce items.

Identify high-volume low-reorder products for improvement.

🏬 Department Intelligence Insights

Key Metrics:

16M Department Orders

20M Department Reorders

21 Departments

Produce is top-performing department.

Top Departments by Order Volume:

Produce

Dairy & Eggs

Beverages

Snacks

Frozen

Insights:

Produce drives both volume and loyalty.

Dairy & Eggs show strong repeat purchase behavior.

Department performance quadrant reveals optimization opportunities.

Business Implications:

Inventory prioritization required for high-volume departments.

Targeted promotions for underperforming categories.

⏳ Time Intelligence Insights

Key Metrics:

9.14K Average Daily Orders

Peak Order Hour: 10 AM

Insights:

Orders peak between 9 AM and 3 PM.

Midweek (Wednesday, Friday) shows highest order volume.

Gradual monthly decline suggests seasonal or dataset sampling effects.

Business Implications:

Optimize warehouse staffing for late morning.

Adjust delivery capacity during peak hours.

Midweek promotions can amplify sales.

🔄 CI/CD Pipeline

GitHub Actions automatically runs on:

Push to main branch

Pull requests

Pipeline executes:

dbt deps

dbt run

dbt test

dbt docs generate

This ensures:

Transformation integrity

Automated validation

Documentation consistency

Deployment readiness

🛠️ Technology Stack

PostgreSQL – Analytical warehouse

dbt – Transformation & modeling

Apache Airflow – Orchestration

Docker – Containerization

GitHub Actions – CI/CD

Power BI – Business Intelligence

🚀 Running the Project Locally

Clone Repository:

git clone https://github.com/faisalam0805-cmd/instacart-dbt.git
cd instacart-dbt

Start Docker Environment:

docker-compose up -d

Run dbt:

docker exec -it airflow-webserver dbt run
docker exec -it airflow-webserver dbt test

Access Airflow:

http://localhost:8080
🎯 What This Project Demonstrates

End-to-end modern data stack implementation

Layered dbt modeling best practices

Fact & dimension modeling discipline

Production-style Airflow orchestration

Containerized infrastructure

CI/CD for analytics engineering

BI-driven decision intelligence

📈 Future Enhancements

Incremental model implementation

Snapshot-based SCD modeling

Data quality monitoring framework

ML-powered product recommendations

Automated BI refresh integration

💼 Portfolio Value

This project demonstrates competency in:

Data Engineering

Analytics Engineering

BI Architecture

Data Modeling

DevOps for Data

Business Insight Generation

It reflects full ownership from raw ingestion to executive dashboard delivery.

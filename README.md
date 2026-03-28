# OLIST Order-to-Delivery Fulfillment Analytics

## 📌 Project Overview
This project builds an end-to-end analytics engineering pipeline to model the full order-to-delivery lifecycle using real transactional supply chain data (OLIST dataset).

The focus is on designing a robust data model and transformation layer that enables consistent, scalable measurement of fulfillment performance, lead time variability, and SLA adherence across sellers and regions.

---

## 🧱 Architecture
Python (Ingestion) → Snowflake (Warehouse) → dbt (Transformations) → Power BI (Dashboard)

Key principles:
- ELT-based pipeline (load first, transform in warehouse)
- Separation of transformation and reporting layers
- Modular and reusable data modeling using dbt

---

## 🎯 Use Case
Supply chain and operations teams require visibility into:

- End-to-end order fulfillment timelines
- Where delays occur (approval, shipping, delivery)
- Which sellers or regions underperform
- On-time delivery rates and SLA adherence

This project enables these analyses through a structured dimensional model at the order-item level.

---

## 📊 Data Grain
**Fact Table Grain:**
- order_id + order_item_number
Each row represents a single order item and its fulfillment lifecycle.

---

## 📐 Data Model

### Dimensions
- dim_date  
- dim_product  
- dim_seller  
- dim_customer_location  

### Fact
- fct_order_fulfillment  

### Marts
- mart_delivery_sla  
- mart_seller_performance  
- mart_regional_delays  

The model is designed to ensure consistent metric definitions and support flexible downstream analysis.

ERD available at:  
`/docs/schema/olist_erd.png`

---

## ⚙️ dbt Implementation
The transformation layer is built using dbt with:

- Staging models for data cleaning and standardization  
- Dimension and fact modeling following star schema principles  
- Analytical marts for business-facing metrics  

### Testing
Includes both standard and custom tests:
- Not null and uniqueness constraints  
- SLA consistency validation  
- Delivered orders must have delivery dates  

Some tests are intentionally designed to fail to highlight real data quality issues and edge cases.

---

## 📏 Core Metrics
All key metrics are defined within the transformation layer to ensure consistency across reporting:

- Total Lead Time (purchase → delivery)  
- Approval Duration  
- Shipping Duration  
- Delivery Delay (actual vs estimated)  
- On-Time Delivery Rate  
- Lead Time Gap (Late vs On-Time)  
- Revenue at Risk from delayed sellers  

---

## 📊 Analytics Focus
The project enables analysis across multiple dimensions:

- Seller performance and delay patterns  
- Regional delay concentration  
- SLA adherence and fulfillment reliability  
- Identification of high-risk sellers (high delay + high volume/revenue)  

---

## ⚙️ Tech Stack
- Python (pandas, snowflake-connector)  
- Snowflake  
- dbt  
- Power BI  

---

## 🚀 Status
- Project structure created  
- ERD designed  
- Snowflake warehouse setup  
- Data ingestion pipeline  
- dbt staging models  
- Dimension and fact models  
- Analytics marts  
- Power BI dashboard  
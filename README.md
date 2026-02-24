# OLIST Order-to-Delivery Fulfillment Analytics

## 📌 Project Overview

This project models the full order-to-delivery lifecycle using real transactional supply chain data (OLIST dataset).  

The goal is to design an analytics engineering pipeline that measures fulfillment performance, lead time variability, and SLA adherence across sellers, regions, and product categories.

---

## 🧱 Architecture

Python (Ingestion) → Snowflake (Warehouse) → dbt (Transformations) → Power BI (Dashboard)

---

## 🎯 Use Case

Supply chain and operations teams need visibility into:

- How long orders take from purchase to delivery
- Where delays occur (approval, shipping, final delivery)
- Which sellers or regions underperform
- On-time delivery rate trends

This project builds a dimensional model to answer those questions at the **order-item grain**.

---

## 📊 Grain

**Fact Table Grain:**  
`order_id + order_item_number`  
(One row per order item representing the latest known fulfillment state)

---

## 📐 Data Model

The dimensional model includes:

- `dim_date`
- `dim_product`
- `dim_seller`
- `dim_customer_location`
- `fct_order_fulfillment`

See ERD diagram in `/docs/schema/olist_erd.png`.

---

## 📏 Core KPIs

- Total Lead Time (purchase → delivery)
- Approval Duration
- Shipping Duration
- Delivery Delay (actual vs estimated)
- On-Time Delivery Rate
- Seller Performance Ranking
- Regional Delay Analysis

---

## ⚙️ Tech Stack

- Snowflake
- dbt
- Python (snowflake-connector, pandas)
- Power BI

---

## 🚀 Status

- [x] Project structure created
- [x] ERD designed
- [ ] Snowflake warehouse setup
- [ ] Data ingestion pipeline
- [ ] dbt staging models
- [ ] Fact + dimension models
- [ ] Analytics marts
- [ ] Power BI dashboard
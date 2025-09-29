# E-Commerce Analytics
An end-to-end data pipeline and Power BI dashboard built on the **Brazilian Olist E-Commerce dataset**.  
The project integrates **Airflow, MongoDB, PostgreSQL, and Power BI** to deliver business insights.

---

## Project Overview
- **Data Ingestion:** Raw CSVs -> MongoDB  
- **Transformation & Loading:** MongoDB -> PostgreSQL (structured fact/dim tables)  
- **Analytics:** Customer segmentation using **RFM Analysis**  
- **Visualization:** Power BI interactive dashboard

---

## Repository Structure
'''
├── airflow/       # DAGs, logs, plugins
├── data/          # Raw & processed datasets
├── docker/        # Docker Compose configs
├── scripts/       # ETL + analytics Python scripts
├── dashboard/     # Power BI files & PDF export
└── README.md
'''

---

## E-Commerce Dashboard Analysis
![Dashboard Preview](./dashboard/dashboard.pdf)

### Key Insights
- **Revenue & Orders**
  - **Total Revenue:** 15.86M
  - **Total Orders:** 99.44K
- **Customer Segmentation**
  - **Recent Customers:** 28.78% (high acquisition rate)
  - **Loyal Customers:** 17%
  - **At Risk Customers:** 14.15% (retention issue)
  - **Champions:** 6.93%
- **Geographical Distribution**
  - **Top revenue states:** São Paulo (SP), Rio de Janeiro (RJ)
  - **Top cities:** São Paulo, Rio de Janeiro, Belo Horizonte
- **Product Categories**
  - **Best-selling categories:** Beauty/Health (beleza_saude) and Gifts/Watches (relogios_presentes)
- **Payment Types**
  - **Credit Card** = 78.62% of revenue (dominant channel)
  - **Boleto** = 18.09%, still significant in Brazil
  - **AOV** highest for **voucher & credit card** users
- **Customer Reviews**
  - **Positive:** 77.07%
  - **Negative:** 14.69% (needs improvement)

### Strategic Recommendations
- **Customer Retention & Loyalty**
  - Launch loyalty programs (tier-based rewards, VIP for Champions)
  - Run re-engagement campaigns for At Risk customers with special offers
  - Introduce subscription models for repeat-purchase categories (e.g., beauty/health)
- **Regional Growth**
  - Focus expansion in secondary cities (e.g., Belo Horizonte, Curitiba, Porto Alegre)
  - Offer free shipping promotions in underperforming regions
- **Product Strategy**
  - Create bundle promotions (e.g., beauty + gift sets for seasonal campaigns)
  - Cross-sell with home décor and accessories
  - Invest in exclusive beauty/health products to strengthen loyalty
- **Payment & Checkout Optimization**
  - Promote installment plans for credit cards → increase AOV
  - Offer discounts for boleto payments to reduce checkout abandonment
  - Add digital wallets (Pix, PayPal) for broader coverage
- **Customer Experience**
  - Investigate negative reviews root causes (delivery, product quality, service)
  - Collect feedback through post-delivery surveys
  - Establish a customer care team for proactive engagement
- **Operational Efficiency**
  - Negotiate with logistics partners to reduce freight costs
  - Implement regional warehouses to optimize delivery distance
  - Introduce free shipping thresholds to distribute shipping cost

### Conclusion
The dashboard highlights strong revenue growth driven by credit card payments, beauty/health products, and major cities (SP & RJ).
However, there are opportunities to improve customer retention, regional expansion, freight cost management, and customer satisfaction.
By implementing the above strategies, the business can boost profitability, customer loyalty, and long-term growth.

---

## Tech Stack
- **Airflow** → Orchestration  
- **MongoDB** → Raw Data Store  
- **PostgreSQL** → Data Warehouse (star schema)  
- **Pandas / SQLAlchemy** → ETL scripts  
- **Power BI (2025)** → Visualization

---

## Key Learnings
- Built a full **data pipeline (Mongo → Postgres → Power BI)**  
- Applied **RFM segmentation** for customer behavior analysis  
- Learned to combine **technical ETL skills** with **business storytelling**  
- Identified actionable insights

---

## How to run
- git clone https://github.com/Mickjrp/E-Commerce-Analytics.git
- docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d
- Access Airflow at http://localhost:8080
- Run DAG: ecom_etl (ETL pipeline)
- Open Power BI file dashboard.pbix

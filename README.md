# Building a Modern Data Warehouse with SQL Server

## 📌 Project Overview
This project demonstrates the end-to-end process of building a modern Data Warehouse. It focuses on implementing robust ETL pipelines, data modeling, and preparing data for advanced analytics using industry best practices.

## 🏗️ Data Architecture
The project follows the **Medallion Architecture**, organizing data into distinct layers to ensure quality and scalability:

### **1. Data Sources**
*   **ERP & CRM Systems:** Raw business and customer data.
*   **Format:** Ingested via CSV files.
*   **Interface:** Folder-based file ingestion.

### **2. Logical Layers**
*   **Raw Layer:** Staging area where data is loaded "as-is" to maintain a historical record of source data.
*   **Cleaned Layer:** Middle layer focused on data cleansing, standardization, and normalization to resolve quality issues.
*   **Business-Ready Layer:** Final layer where data is transformed into a **Star Schema** (Dimensions & Facts) for optimized reporting.

## 📏 Engineering Standards
To ensure maintainability and professional-grade development, the following standards are applied:

### **Naming Conventions**
*   **Tables:** `dim_` for dimensions, `fact_` for facts, and `agg_` for aggregated summaries.
*   **Surrogate Keys:** All primary keys in dimension tables use the `_key` suffix.
*   **Technical Columns:** System-generated metadata columns start with the `dwh_` prefix (e.g., `dwh_load_date`).

## 🚀 Tech Stack & Skills
*   **Database Engine:** SQL Server 2025.
*   **Management Tool:** SQL Server Management Studio (SSMS).
*   **Core Concepts:** ETL Processes, Data Modeling, Schema Design, and Data Quality Management.

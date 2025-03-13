# 🥈 Silver Layer

The **Silver Layer** processes raw data from the Bronze layer, ensuring it is cleaned, structured, and ready for transformation. This stage focuses on **data validation, standardization, and enrichment**.

## 🔹 Key Transformations
- **Mounting ADLS Gen2** to access raw datasets.
- **Books Table Processing:**
  - Data type corrections and handling missing values.
  - Removing duplicates and ensuring data integrity.
- **Students & Transactions Data Cleaning:**
  - Validating email addresses and encrypting sensitive data.
  - Standardizing date formats and transaction types.

## 📂 Files in This Folder
- PySpark scripts for **data cleaning and transformation**.
- Delta Lake tables storing the **refined Silver layer data**.
- Logs documenting transformations and data validation steps.

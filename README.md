# QuickCart: The Data Integrity Crisis (Capstone)

## 📌 Project Overview
This project resolves a P0 financial incident for QuickCart. It establishes a bank-reconcilable **Source of Truth** by processing messy JSON transaction logs and reconciling them with a production PostgreSQL database.

## 🛠️ Tech Stack
- **Python**: Data cleaning, normalization, and ETL.
- **SQL (PostgreSQL)**: Complex reconciliation using CTEs and Window Functions.
- **MongoDB**: Archival of raw transaction logs for auditability.
- **Dotenv**: Secure management of database credentials.

## 🚀 How to Run the Project
1. **Clone the Repo**
2. **Set up Environment Variables**: Create a `.env` file based on `.env.example`.
3. **Install Dependencies**: `pip install -r requirements.txt`
4. **Run the ETL Script**: `python clean_transactions.py`
5. **Run Reconciliation**: Execute `reconciliation.sql` in your PostgreSQL tool.

## 📊 Executive Summary
1. **Objective**
The goal was to resolve the discrepancy between internal sales records and bank settlement statements by creating a single, bank-reconcilable Source of Truth.

2. **Technical Methodology**
Python ETL: Used python-dotenv for secure credential management and pymongo to archive raw JSON logs for auditability. Implemented a robust normalization function to standardize inconsistent currency formats (strings, cents, and symbols) into a unified USD float format.

SQL Reconciliation: Utilized Common Table Expressions (CTEs) to create staged, clean datasets. Addressed the "Multiple Payment Attempts" issue using the ROW_NUMBER() window function to isolate only the latest successful transaction per order.

Data Integrity: Screened out "Test/Sandbox" transactions by joining the payments table with the orders table to filter based on the is_test flag.

3. **Key Findings**
Total Successful Sales: Represents the deduplicated, non-test internal revenue.
Orphan Payments: Identified funds received by the bank that lack a corresponding internal order_id, representing a primary source of data mismatch.

Discrepancy Gap: Calculated as the difference between internal expected sales and actual bank settlements, providing a clear financial audit trail.

4. **Conclusion**
The resulting report is auditable and robust to edge cases, providing Finance with the necessary evidence to close the month with confidence.

# Real-Time Credit Card Fraud Transaction Analytics Pipeline

## Overview

The **Real-Time Credit Card Fraud Transaction Analytics Pipeline** is a scalable, end-to-end solution designed to detect fraudulent credit card transactions in real-time. By integrating **historical and streaming transaction data**, this system leverages AWS services and open-source technologies to provide efficient data processing, analysis, visualization, and monitoring capabilities.

## Description

This pipeline combines **historical data stored in Amazon S3** with **real-time streams from Confluent Kafka**. Using **Apache Spark**, the data is analyzed against predefined fraud detection rules. The results are stored back in S3 for further querying and visualization.

Key insights from processed data are accessible via **AWS Athena**, allowing SQL-based queries. **Tableau** connects to this pipeline to provide dynamic, real-time dashboards for visualization. The entire pipeline is automated through **GitHub Actions**, with infrastructure provisioning handled by **AWS CloudFormation templates (CFTs)**.

### Benefits
- **Real-time Fraud Detection:** Identifies fraudulent transactions on-the-fly.
- **Scalable and Automated:** Supports large datasets and seamless deployment.
- **Data Insights:** Offers comprehensive analytics through SQL queries and dashboards.

---

## Key Components

### Data Sources
- **Historical Data**: Stored in **Amazon S3**, consisting of 8+ million transactional records.
- **Streaming Data**: Ingested from **Confluent Kafka**, representing live credit card transactions.

### Technologies Used
- **AWS RDS**: Securely stores unique customer data.
- **Apache Spark**: Analyzes both historical and streaming data.
- **AWS EMR**: Managed big data framework for large-scale parallel processing.
- **Amazon S3**: Central storage for historical, processed, and streaming data.
- **AWS Glue**: Crawls, catalogs, and prepares data for analysis.
- **AWS Athena**: Runs ad-hoc SQL queries on S3-stored data using the Glue Data Catalog.
- **Tableau**: Connects to Athena for interactive, real-time data visualization.
- **AWS CloudFormation**: Automates AWS resource provisioning.
- **GitHub Actions**: Orchestrates CI/CD for continuous integration and deployment.

---

## Workflow

1. **Data Ingestion**: 
   - Historical data is fetched from **S3**.
   - Real-time transaction data streams from **Confluent Kafka**.
2. **Data Processing**: 
   - **Apache Spark** compares real-time transactions with historical records using fraud detection rules.
3. **Data Storage**: 
   - Analyzed data (both genuine and fraudulent transactions) is stored in an **S3 bucket**.
4. **Schema Management**: 
   - **AWS Glue** crawls and catalogs the data.
5. **Querying and Analysis**: 
   - **Athena** enables SQL-based exploration of the processed data.
6. **Visualization**: 
   - **Tableau** connects to Athena via the ODBC Athena connector, offering real-time dashboards.
7. **Automation**: 
   - Infrastructure is deployed with **CloudFormation templates**.
   - **GitHub Actions** automates the entire pipeline.

---

## Architecture

![Architecture Diagram](https://github.com/LakshMundhada/Real-Time-Fraudulent-Transaction-Analytics-Pipeline/assets/150781667/a0357a50-42e4-4a2f-96b9-006df45aae39)

---

## Deployment Steps

### Step 1: Set Up Data Sources
- Store historical data in **Amazon S3**.
- Configure **Confluent Kafka** to stream real-time transactional data.

### Step 2: Configure AWS Services
- Create an S3 bucket for processed data.
- Set up **AWS Glue** for schema management and data cataloging.
- Configure **Athena** for ad-hoc queries.

### Step 3: Develop Apache Spark Application
- Implement an **Apache Spark** job to process streaming and historical data.
- Test fraud detection algorithms locally for accuracy.

### Step 4: Automate with GitHub Actions and CloudFormation
- Use **CloudFormation templates** to provision AWS resources (EMR, Glue, S3, etc.).
- Set up **GitHub Actions** workflows for CI/CD and deployment.

### Step 5: Visualize Data with Tableau
- Install Tableau Server or Tableau Online.
- Connect Tableau to Athena using the **ODBC Athena connector**.

### Step 6: Monitor and Optimize
- Deploy and monitor the pipeline for performance and scalability.
- Implement logging mechanisms for operational insights.

---

## Visualization

### Dashboards:
1. **Dashboard 1: Fraud Trends**  
   ![Dashboard 1](https://github.com/LakshMundhada/Real-Time-Fraudulent-Transaction-Analytics-Pipeline/assets/150781667/95807139-afaa-4ab2-9d9b-9439243d2c02)

2. **Dashboard 2: Transaction Analytics**  
   ![Dashboard 2](https://github.com/LakshMundhada/Real-Time-Fraudulent-Transaction-Analytics-Pipeline/assets/150781667/d4e1b506-6e35-41db-aa04-f9b55fadbfcd)

3. **Dashboard 3: Fraud Location Heatmap**  
   ![Dashboard 3](https://github.com/LakshMundhada/Real-Time-Fraudulent-Transaction-Analytics-Pipeline/assets/150781667/ef71a291-2c73-4ddf-b71b-2e179db6ce60)

4. **Final Consolidated Dashboard**  
   ![Final Dashboard](https://github.com/LakshMundhada/Real-Time-Fraudulent-Transaction-Analytics-Pipeline/assets/150781667/fbd9ca04-a3f1-4583-aa97-423063f4dbe9)

---

## Conclusion

The **Real-Time Credit Card Fraud Transaction Analytics Pipeline** demonstrates how AWS and open-source technologies can be integrated to address real-world challenges like fraud detection. With its robust design, scalability, and automation, this solution empowers organizations to effectively safeguard against financial fraud while gaining actionable insights into transactional data.

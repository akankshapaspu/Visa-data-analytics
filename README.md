# OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics
## Introduction
By implementing OLTP and OLAP systems on H-1B visa data, we can process real-time data from operational data stores (ODS) and perform analytical tasks on the data warehouse (DWH). The H-1B visa is a nonimmigrant work visa for highly educated foreign professionals in specialty jobs requiring a bachelor's degree or equivalent. Using an Entity Relationship (ER) model derived from U.S. Citizenship and Immigration Services (USCIS) data, we developed the OLTP database on a cloud-based MySQL instance. Data from MySQL is extracted, transformed with Python, and loaded into the data warehouse, where analytics are performed and visualized to understand geographical, economical, and educational factors of applicants.
## Overview and Architecture
The objective is to automate the implementation of OLTP and OLAP systems in a unified data pipeline on the cloud. After thorough research on different cloud service providers, we have chosen Amazon Web Services (AWS) as the primary platform for this project due to its competitive pricing and versatile service offerings. The data, obtained in raw CSV format from USCIS, consists of 154 columns and is currently unnormalized.
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/0aa093a4-aeb3-4a8e-a0e2-09dded3a5d65)
Once the data is normalized, it is migrated to Amazon Simple Storage Service (located in US East (N. Virginia) region, us-east-1), a scalable infrastructure providing object storage services through a web service interface. Our OLTP system is hosted on a cloud SQL MySQL instance (located in us-east-1b), where Identity and Access Management (IAM) roles, along with user groups, are established to regulate access to the Relational Database service. Additionally, the instance is configured with VPC to monitor and filter incoming/outgoing traffic, granting access only to authorized users, while a backup policy is implemented for contingency purposes.

Python-based ETL scripts are developed to transform the data, and AWS Glue, a serverless data integration service built on Apache Spark Structured Streaming engine, orchestrates the ETL process. The data is extracted from the MySQL instance, transformed, and loaded into the data warehouse (DWH) via Python. Subsequently, analysis is conducted on the data within the warehouse, and SQL-based views are created for reporting purposes, which are then visualized to present the analysis findings.
## Project Flow
A streamlined on-demand workflow is established to implement the entire process of OLTP and OLAP systems. The workflow initiates with jobs responsible for creating tables in the MySQL database, followed by loading the corresponding CSV file data into these tables. Once all tables are successfully created, adhering to constraints, the subsequent job executes the ETL process. During this process, data is extracted from the newly populated MySQL tables, loaded into a pandas dataframe for data preprocessing, and then ingested into Snowflake using dependency libraries. After the ETL step, the Tableau data source is refreshed, ensuring that the updated data is showcased on the dashboards.
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/dca41c2f-0d6b-4eb6-99ec-cac065c2321d)
## Database Implementation
Data has been gathered from diverse sources, including Kaggle, the US Department of Labor website, and others, resulting in a total of 64 columns. The data undergoes multiple cleaning procedures, encompassing the removal of special characters, transformation of incomplete data into meaningful values, and correction of incorrectly formatted data to enhance consistency. Subsequently, the data is normalized into 8 tables and modeled into a relational schema, as depicted in the entity relationship diagram below:
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/054f7328-56ec-4ece-a4f2-a795d14e45ec)
## ETL Implementation
Python is utilized to implement the Extract, Transform, Load (ETL) process, which is orchestrated through an AWS Glue job.

A. Extraction:
Data extraction from the Cloud MySQL instance is achieved using access keys stored in a secured .py file, preventing any potential security risks associated with publicly exposed keys in the code. The access keys are uploaded to an S3 bucket. Subsequently, data is read from the file in the S3 bucket, and a connection is established to the MySQL instance. After successful connection, the data is extracted and saved into a dataframe using the read_sql function, with the connection being immediately closed after data retrieval.

B. Transform:
Once the data is loaded into the dataframe from the Cloud MySQL instance, several data transformations are performed, including column renaming, handling of null values, removal of unwanted columns, and cleaning of revenue field values by removing commas. String formatting for string-type columns is also handled, and a "Refreshed Date" column is added to each table, capturing the uploading timestamp.

C. Load:
Data is loaded from the dataframe to the Snowflake database using Python's database connectivity. Similar to the MySQL instance, access credentials for Snowflake are kept secure in a separate .py file. The connection details are read from the file, and once a successful connection is established, the preprocessed data in the dataframe is loaded into the respective tables.
## Datawarehouse Implementation
Once the ETL process is successfully completed, the data will be loaded into the Snowflake cloud data platform, which will serve as the data warehouse for H1-B visa data. Snowflake is recognized as one of the most robust data warehousing systems for handling large-scale data. It boasts features such as cross-cloud deployment capabilities, high scalability, and top-notch security. The data will be ingested while adhering to the specified data model. For this project, we constructed a data warehouse utilizing the STAR schema model, where the application acts as the fact table, and geography, prevailing wage, education info, and agent serve as dimension tables.
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/161b2df0-d6c7-434b-8138-08205ccc700d)
## Data Analysis
After inserting data into the snowflake, using SQL we queried the data for the required analysis. Following is the analysis was done on the data:
### Top 10 Countries of the applicants
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/9ed50dfe-05cb-459e-8ca9-3cd7961f7164)

### Popular Education background for the applicants
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/e74b157c-b637-43e6-ae0c-8af804011d09)

### Top 10 Employers that are filing the H1-B applications.
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/7daf18cd-d081-4370-8b1c-1212b501a537)

### Top 10 employers that have highest H1 approvals
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/3921e03c-feb2-421a-80d2-2222cb1b375f)

### State Distributions for all the applications
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/da95d580-3473-442d-b90c-7cd0732ed419)

### Average wage for the applicants accross various job titles
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/56de7653-ad67-4c1b-87d2-2845ce9be9b6)

### States and the maximum applications for each job title
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/56b12afb-e15c-482c-add2-9604a1d2fc5f)

## Data Visualization
![image](https://github.com/dharmateja36/OLTP-OLAP-on-United-States-H-1B-visa-data-for-operational-usage-and-analytics/assets/117693500/aa0ef781-ff59-4a32-9e64-ab692647c98e)

## Results
Based on the H1-B data analysis above, it is evident that the majority of H1-B visa applicants who applied and got approved are from India.

• Among the applicants, those with majors in Engineering fields such as "Computer Science," "Computer Engineering," and "Business Administration" have the highest number of applications.

• Prominent IT giants like Cognizant, Google, Microsoft, and Apple are the top companies filing for H1-B visas for their employees from various countries, making up more than 30% of the total applications.

• California state has the largest number of applicants, with most of them working in the role of "Software Engineer," totaling 2,006 individuals.

• The average salary for applicants with approved visa status is $112,350, and the highest average salary is observed among applicants from California state.

• Out of all the applications, 70.32% of applicants received an H1-B visa, 22.01% had their visa denied, and 7.67% withdrew their applications.
## Technical Difficulties
• The collected data contained numerous missing and inconsistent values, requiring extensive efforts to transform it into clean and usable data.

• The decision to adopt AWS, although beneficial, posed a challenge for the entire group as setting up the architecture and comprehending its functionalities demanded significant effort.

• Several columns in the data have null values, necessitating careful consideration during the analysis.

• To avoid sharing access keys in the code, we explored alternative methods to provide access, opting for secure access through files.

• Inserting the data into databases and data warehouses proved to be laborious due to certain issues and complexities encountered during development.

## Future Work
The entire process is automated, and the on-demand pipeline efficiently handles data for future fiscal years seamlessly. By adjusting parameters in the data warehouse, data for upcoming fiscal quarters can be easily appended to the existing data warehouse tables. While the current analysis focuses on one type of visa, the system is designed to accommodate multiple types, allowing for a broader range of analyses to be performed.

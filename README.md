# Social Media ETL
## Directory structure
```
----SocialMediaETL
    |---configs
    |---dags
    |   |---tiktok
    |   |---youtube
    |---data
    |   |---raw
    |   |   |---tiktok
    |   |   |---youtube
    |   |---transformed
    |       |---tiktok
    |       |---youtube
    |---factories
    |---plugins
    |   |---helpers
    |   |---hooks
    |   |---operators
    |   |---sensors
    |---scripts
    |---test
        |---tiktok
        |---ytb
```
"""
## Objective

The objective of this project is to showcase my skills and demonstrate what you have learned throughout the development process. Additionally, the goal is to increase the value of the project by ensuring that other Data Engineering enthusiasts can easily rebuild the entire project on their own.

## Summary
The Social Media ETL project represents a end-to-end data engineering solution that automate the management and sharing of social media video contents. Powered by the advanced technologies offered by Google Cloud Platform (GCP), including Google Kubernetes Engine (GKE), Google Cloud Storage (GCS), BigQuery,etc.

With Airflow deployed on GKE, the project achieves unparalleled scalability and maintainability, ensuring optimal performance at every step. Leveraging the power of BigQuery, Data Warehouse is much easiser to design, while Dbt enables efficient and seamless data transformation. Python and SQL serve as the project's backbone, facilitating its development and implementation with utmost precision.

The entire workflow, spanning data extraction, video customization, and social media uploads, has been fully automated, reducing manual effort and enhancing operational efficiency. By successfully crafting a robust and efficient solution, the project epitomizes the author's deep understanding and mastery of the technologies involved, cementing their status as a true data engineering virtuoso.
## Table of Contents

- [Architect](#architect)
- [Build](#build)
- - [Prerequisites](#prerequisites)
- - [Setup](#setup-infrastructure)
- - [CI/CD](#cicd)
- [Roadmap](#roadmap)
- [Author and Acknowledgement](#author-and-acknowledgement)

## Architect

In this section, describe the overall architecture of the project, including the pipeline and technologies used. Explain the rationale behind choosing each component and how they work together to achieve the desired outcome.
1. Infrastructure: Google Cloud Platform
2. Orchestration: Airflow
3. Trasformation: DBT
4. Data Warehouse: BigQuery

## Build

Break down the steps required to set up the infrastructure and environment necessary to run the project. Provide detailed instructions so that developers can easily follow along and recreate the project independently.

### Prerequisites

- GCP (Google Cloud Platform) active account
- Python3 installed in your local machine
- Github account

### Setup Infrastructure
1. Enable necessery 
### CI/CD

Describe the Continuous Integration and Continuous Deployment (CI/CD) process for the project. Explain how changes are tested, built, and deployed to ensure a smooth development workflow.

## Roadmap

Outline potential future developments or enhancements that can be made to the project. This could include additional features, optimizations, or scalability improvements.

## Author and Acknowledgement

Introduce yourself as the author of the project and provide any additional information you want to share. Acknowledge any individuals or resources that contributed to the project's success.

---

This README template follows a similar structure to a dbt (Data Build Tool) repository to provide a comprehensive overview of the project and make it easily understandable for others.

Feel free to customize and expand upon this template to suit the specific needs and details of your project.
"""

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

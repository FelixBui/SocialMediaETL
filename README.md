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
    |   |   |---source_1
    |   |   |---source_2
    |   |---transformed
    |       |---source_1
    |       |---source_2
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

## Build

Break down the steps required to set up the infrastructure and environment necessary to run the project. Provide detailed instructions so that developers can easily follow along and recreate the project independently.

### Prerequisites

- GCP (Google Cloud Platform) active account
- Python3 installed in your local machine
- Github account

### Setup Infrastructure

Explain how to set up the infrastructure, including any cloud services, databases, or other components required for the project. Include any necessary configuration files or scripts.
1. Enable nesseser
### CI/CD

<!-- Describe the Continuous Integration and Continuous Deployment (CI/CD) process for the project. Explain how changes are tested, built, and deployed to ensure a smooth development workflow. -->

1. Continuous integration:
    - Set up the environment on ubuntu and check the entire repository 
`   - uses: actions/checkout@v2`
    - Install python version 3.9 
`   - name: Set up Python 
      uses: actions/setup-python@v2
      with:
        python-version: 3.9`

    - Install dependencies and pytest
`   - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
        pip install pytest
        airflow db init`
    - Execute test import and syntax using pytest
        tests\dags\test_import.py
        tests\dags\test_syntax.py
`- name: Run tests
      run: |
        pytest`
2. Continuous delivery
    - Set up the environment on ubuntu and check the entire repository
`   - name: Checkout
      uses: actions/checkout@v3`
    - Set up gcloud CLI 
`    - uses: google-github-actions/setup-gcloud@94337306dda8180d967a56932ceb4ddcf01edae7
      with:
        service_account_key: ${{ secrets.GCP_SA_KEY }}
        project_id: ${{ secrets.GCP_SA_PROJECT_ID }}`
    - get gke credentials
`    - uses: google-github-actions/get-gke-credentials@fb08709ba27618c31c09e014e1d8364b02e5042e
      with:
        cluster_name: ${{ env.GKE_CLUSTER }}
        location: ${{ env.GKE_ZONE }}
        credentials: ${{ secrets.GCP_SA_KEY }}`
    - deploy the dependencies in scheduler and worker pod
`   - name: Deploy
      run: |-
        export SCHEDULER_POD=$(kubectl get pods -n airflow | grep scheduler | awk '{print $1}')
        export WORKER_POD=$(kubectl get pods -n airflow | grep worker | awk '{print $1}')
        export REQUIREMENTS=/opt/airflow/dags/repo/requirements.txt
        kubectl exec -it $WORKER_POD -n airflow -- /home/airflow/.local/bin/pip install -r $REQUIREMENTS
        kubectl exec -it $SCHEDULER_POD -n airflow -- /home/airflow/.local/bin/pip install -r $REQUIREMENTS`
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

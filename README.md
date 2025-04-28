
# Data Engineering ETL Pipeline with PostgreSQL, Docker, and GitHub Actions

## Overview
This project automates the process of ingesting data, performing transformations, running quality checks, and storing the data in a PostgreSQL database. The entire pipeline is orchestrated using **GitHub Actions**, and **Docker** is used to deploy the PostgreSQL container. The pipeline includes multiple stages such as:

1. **Ingestion**: Extracts and loads raw data from CSV files into PostgreSQL & created Master Data to ensure that the data adheres to predefined rules and integrates external data sources (if required).
2. **Transformations**: Cleans and prepares data for analysis or further processing & Quality Checks to validate data against business rules and ensure data integrity.
3. **Workflow Orchestration**: Automate the pipeline to run daily using GITHUB ACTION.
4. **CI/CD**: Set up a CI/CD to run tests and deploy the code using Docket Containers & GITLAB.

## Setup Instructions

### Prerequisites
Before setting up the project, ensure you have the following installed:

1. **Docker**  
   Docker is used to run the PostgreSQL container. Install it by following the [Docker Installation Guide](https://docs.docker.com/get-docker/).

2. **GitHub Account**  
   You'll need a GitHub account to manage the repository and the workflows. If you don't have one, sign up at [GitHub](https://github.com/join).

3. **Python Environment**  
   This project uses **Python 3.11**. It's recommended to set up a virtual environment:
   - Install Python 3.11: [Download Python](https://www.python.org/downloads/)
   - Create a virtual environment and activate it:
     ```bash
     python -m venv .venv
     source .venv/bin/activate  # On Windows, use .venv\Scripts\activate
     ```

4. **PostgreSQL Docker Image**  
   The PostgreSQL service is deployed using Docker, so ensure Docker is installed and running.

### Clone the Repository
Clone the repository to your local machine:
```bash
git clone https://github.com/your-username/your-repo-name.git
cd your-repo-name
```

### Install Python Dependencies
Install all necessary dependencies by running:
```bash
pip install -r requirements.txt
```

### Set Up PostgreSQL Docker Container
Start the PostgreSQL container with Docker:
```bash
docker-compose up -d
```
This will launch a PostgreSQL instance with the following configuration:
- **Host**: `localhost`
- **Port**: `5432`
- **Database**: `my_db`
- **User**: `my_user`
- **Password**: `my_password`

### GitHub Actions Workflow
The pipeline is set up to run via **GitHub Actions**. It includes the following steps:
1. **Deploy PostgreSQL**: Uses Docker to deploy the PostgreSQL container.
2. **Run Python Scripts**: Executes Python scripts in sequence:
   - `ingestion.py`
   - `masterdata.py`
   - `quality_checks.py`
   - `transformations.py`
     
#### Workflow Triggers
- **Push** to the `main` branch.
- **Scheduled run** (daily).
- **Manual Trigger**: You can trigger the workflow manually from the **Actions** tab in GitHub.

### Running the Pipeline Locally (for Development/Testing)
If you want to run the pipeline locally to test changes:
1. **Start PostgreSQL Container**:
   ```bash
   docker-compose up -d
   ```
2. **Run the Python Scripts**:
   ```bash
   python ingestion.py
   python masterdata.py
   python quality_checks.py
   python transformations.py
   ```

## Approach
The pipeline is designed in a **modular** fashion:
1. **Ingestion Stage**: Data is extracted from CSV files (stored in Azure Blob Storage) and loaded into PostgreSQL.
2. **Master Data Stage**: The data is validated against predefined rules and integrated with external data sources if necessary.
3. **Quality Checks**: Ensures the data is correct and complies with business rules, such as format, completeness, and consistency.
4. **Transformations**: The data is cleaned, aggregated, or transformed for further analysis or reporting.

GitHub Actions orchestrates the entire CI/CD workflow, while Docker provides a consistent environment for PostgreSQL.

## Assumptions
- The raw data files are in **CSV format** and follow a predefined schema.
- The target **PostgreSQL database** is the data storage for the ingested data.
- The pipeline runs in a cloud environment (such as GitHub Actions) for automation.

## Challenges
1. **Data Quality**: Ensuring the consistency and integrity of the data during the ingestion and transformation phases. Data quality checks were introduced to handle missing or invalid data.
2. **Database Connectivity**: Managing the connection to the PostgreSQL container, ensuring the service is properly running before the Python scripts begin.
3. **CI/CD Integration**: Configuring GitHub Actions to handle dependencies and run the pipeline seamlessly without errors.

## Improvement Ideas
1. **Error Handling**: Improve error handling in Python scripts to address unexpected issues like missing files or connection failures more gracefully.
2. **Testing**: Incorporate unit tests for Python scripts and end-to-end testing of the pipeline to ensure robustness.
3. **Scaling**: Use cloud storage and parallel processing to handle larger datasets and improve performance.

## LLM (Large Language Model) Usage
A **Large Language Model (LLM)**, specifically **ChatGPT**, was used to assist in generating parts of the code and YAML configuration for the project. The model helped with:
- Writing and optimizing Python scripts.
- Utilized it to create the documentation needed.


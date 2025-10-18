# Medical Data Stack

This project implements a data engineering pipeline for processing French medical data. It uses Docker to orchestrate a stack of modern data tools, including Apache Airflow for workflow management, Apache Spark for data processing, and Apache Hive for data warehousing.

## Prerequisites

Before you begin, ensure you have the following installed on your system:
-   [Docker](https://docs.docker.com/get-docker/)
-   [Docker Compose](https://docs.docker.com/compose/install/)

## Setup and Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/InToXy/BigData.git
    cd BigData
    ```

2.  **Create Local Directories:**
    Some directories required for the pipeline to run are ignored by Git and may not exist after cloning. You need to create them manually:
    ```bash
    mkdir -p logs data/bronze data/silver data/gold
    ```

3.  **Download the Data:**
    This repository does not include the source data files due to their large size. Your team must download them manually and place them in the correct directories.

    Once downloaded, the file structure inside the `data/source` directory must be as follows:
    ```
    data/
    └── source/
        ├── csv/
        │   ├── DPA_SSR_recueil2014_donnee2013_table_es.csv
        │   ├── DPA_SSR_recueil2014_donnee2013_table_lexique.csv
        │   ├── DPA_SSR_recueil2014_donnee2013_table_participant.csv
        │   ├── ESATIS48H_MCO_recueil2017_donnees.csv
        │   ├── Hospitalisations.csv
        │   ├── RCP_MCO_recueil2014_donnee2013_table_es.csv
        │   ├── RCP_MCO_recueil2014_donnee2013_table_participant.csv
        │   ├── activite_professionnel_sante.csv
        │   ├── dan_mco_recueil2016_donnee2015_donnees.csv
        │   ├── deces.csv
        │   ├── dpa-ssr-recueil2018-donnee2017-donnees.csv
        │   ├── dpa_had_recueil2016_donnee2015_donnees.csv
        │   ├── etablissement_sante.csv
        │   ├── ete-ortho-ipaqss-2017-2018-donnees.csv
        │   ├── hpp_mco_recueil2015_donnee2014_tables_es.csv
        │   ├── idm_mco_recueil2015_donnee2014_tables_es.csv
        │   ├── professionnel_sante.csv
        │   ├── rcp-mco-recueil2018-donnee2017-donnees.csv
        │   ├── resultats-esatis48h-mco-open-data-2019.csv
        │   ├── resultats-esatisca-mco-open-data-2019.csv
        │   └── resultats-iqss-open-data-2019.csv
        └── xlsx/
            ├── dpa_had_recueil2016_donnee2015_donnees.xlsx
            ├── resultats-esatis48h-mco-open-data-2020.xlsx
            ├── resultats-esatisca-mco-open-data-2020.xlsx
            └── resultats-iqss-open-data-2020.xlsx
    ```

## Usage

1.  **Start the services:**
    Run the following command from the root of the project to build and start all services in the background:
    ```bash
    docker-compose up -d
    ```

2.  **Access Services:**
    -   **Apache Airflow UI:** `http://localhost:8080`
    -   You can check the status of all containers with `docker-compose ps`.

3.  **Run the Pipeline:**
    Once the services are running, access the Airflow UI, enable the `medical_data_pipeline` DAG, and trigger it to start the data ingestion and processing workflow.

## Project Structure

-   `config/`: Configuration files for Hadoop, Hive, and Spark.
-   `dags/`: Contains the Airflow DAGs (e.g., `medical_pipeline.py`).
-   `data/`: Local directory for data storage (ignored by Git).
-   `spark_jobs/`: Spark processing scripts (ingestion, transformation, aggregation).
-   `scripts/`: Utility and initialization scripts.
-   `docker-compose.yml`: Defines the services, networks, and volumes for the Docker environment.

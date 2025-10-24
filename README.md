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
    -   **Jupyter Lab:** `http://localhost:8888` (token: admin123)
    -   **MinIO Console:** `http://localhost:9001` (minioadmin/minioadmin123)
    -   **Superset:** `http://localhost:8088` (admin/admin123)
    -   **Trino Web UI:** `http://localhost:8090/ui` (Query Engine for PowerBI)
    -   You can check the status of all containers with `docker-compose ps`.

3.  **Initialize Trino for PowerBI:**
    After starting the services, wait 30 seconds for Trino to be ready, then run:
    ```bash
    ./trino/init_trino_tables.sh
    ```
    
    **See detailed guide:** [TRINO_QUICKSTART.md](TRINO_QUICKSTART.md)

4.  **Run the Pipeline:**
    Execute Spark jobs to process data through Bronze → Silver → Gold zones:
    ```bash
    # From Jupyter container or using spark-submit
    docker exec chu_jupyter spark-submit /home/jovyan/jobs/main_jobs/bronze_ingestion.py
    docker exec chu_jupyter spark-submit /home/jovyan/jobs/main_jobs/silver_transformation.py
    docker exec chu_jupyter spark-submit /home/jovyan/jobs/main_jobs/gold_aggregation.py
    ```

## Project Structure

-   `config/`: Configuration files for Hadoop, Hive, and Spark.
-   `dags/`: Contains the Airflow DAGs (e.g., `medical_pipeline.py`).
-   `data/`: Local directory for data storage (ignored by Git).
-   `spark_jobs/`: Spark processing scripts (ingestion, transformation, aggregation).
-   `trino/`: **Trino query engine configuration and PowerBI connection guide**.
-   `tests_gold/`: **Complete Gold zone documentation (12 tables, 8 charts, 75+ pages)**.
-   `docker-compose.yml`: Defines the services, networks, and volumes for the Docker environment.

## PowerBI Connection

**Quick Start:** See [TRINO_QUICKSTART.md](TRINO_QUICKSTART.md)

**Detailed Guide:** [trino/POWERBI_CONNECTION_GUIDE.md](trino/POWERBI_CONNECTION_GUIDE.md)

**Summary:**
1. Start services: `docker-compose up -d`
2. Initialize Trino tables: `./trino/init_trino_tables.sh`
3. Install Trino ODBC driver (64-bit) on Windows
4. Configure ODBC source (Host: localhost, Port: 8090, Catalog: minio, Schema: gold)
5. Connect PowerBI Desktop via ODBC

**Test connection:**
```bash
./trino/test_trino_connection.sh
```

## Gold Zone Documentation

Complete documentation available in `tests_gold/`:
- **12 KPI tables** (1,563 rows total)
- **8 performance charts** (PNG format)
- **75+ pages** of documentation
- **17 validated tests**

**Quick access:** `tests_gold/START_HERE_GOLD.md`

## JAR Dependencies

```
sudo wget -P jars/   https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar   https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar   https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar```
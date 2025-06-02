# covid_data

A data engineering project for managing, transforming, and analyzing COVID-19 datasets using [dbt](https://www.getdbt.com/), [Dagster](https://dagster.io/), and Python.

## Features

- **Data Ingestion:** Collects and stores COVID-19 data from various sources.
- **Transformation:** Uses dbt for data modeling and transformation.
- **Orchestration:** Leverages Dagster for workflow orchestration and asset management.
- **Testing:** Includes unit and integration tests for data pipelines.

## Project Structure

```
covid_data/
├── data/                        # Data storage (contains .keep to retain folder)
│   └── .keep
├── dbt_covid_data/              # dbt project files
│   ├── dbt_project.yml
│   └── ... (other dbt files)
├── dagster_covid_data/          # Dagster and Python source code
│   ├── __init__.py
│   └── utils/
│       ├── __init__.py
│       ├── common.py
│       ├── data.py
│       └── duckdb.py
├── tests/                       # Unit tests
│   ├── test_utils_common.py
│   ├── test_utils_data.py
│   └── test_utils_duckdb.py
├── requirements.txt             # Python dependencies
├── Dockerfile                   # Docker container setup
├── docker-compose.yml           # Docker Compose orchestration
├── .gitignore                   # Git ignore rules
├── settings.py                  # Project-wide configuration and paths
└── README.md                    # Project documentation
```

## Getting Started

### Prerequisites

- Python 3.12.10
- [dbt](https://docs.getdbt.com/docs/installation)
- [Dagster](https://docs.dagster.io/getting-started)
- Docker & Docker Compose

### Installation

1. **Clone the repository:**
   ```sh
   git clone https://github.com/allen-morales/covid_data.git
   cd covid_data
   ```

### Running with Docker Compose

Start all services:
```sh
docker compose up
```

This will build and start the containers as defined in `docker-compose.yml`.

### Running Locally (without Docker)

- **Install dependencies:**
  ```sh
  pip install -r requirements.txt
  ```
- **Start Dagster development server:**
  ```sh
  dagster dev
  ```
- **Run dbt commands:**
  ```sh
  cd dbt_covid_data
  dbt run
  ```

## Data Analysis

1. What are the top 5 most common values in a particular column, and what is their frequency?
    ```
    
    ```
2. How does a particular metric change over time within the dataset?
3. Is there a correlation between two specific columns? Explain your findings.

---
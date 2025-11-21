## 1) Clone repo and open terminal at dbt project directory
[Product Analytics Repo](https://github.com/LeafLink/product-analytics)

cd dbt_sigma_poc

## 2) Build the image
docker compose build

## 3) Start the database (first start runs the seed/init scripts)
docker compose up -d

## 4) Virtual Environment Setup
poetry config virtualenvs.in-project true
poetry install
source .venv/bin/activate

### make sure dbt version is 1.9.0
poetry run dbt --version

## 5) Tell dbt to use in repo profiles.yml
export DBT_PROFILES_DIR="$(pwd)/profiles"

## 6) Make sure dbt is setup correctly by running
dbt debug



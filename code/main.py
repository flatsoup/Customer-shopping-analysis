from  __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.engine import Engine
import os
import logging


import pandas as pd
import sqlalchemy
from pathlib import Path
from dotenv import load_dotenv

LOG_FORMAT = '%(levelname)s | %(asctime)s | %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("customers_etl")


@dataclass(frozen=True)
class Config:
    user: str
    password: str
    host: str
    port: int
    name: str

    @staticmethod
    def from_env() -> "Config":
        code_dir = Path(__file__).resolve().parent
        load_dotenv(dotenv_path=code_dir / ".env")

        def need(key: str) -> str:
            value = os.getenv(key)
            if not value:
                raise ValueError(f'Environmental key does not exist {key}')
            return value

        return Config(
            user=need("DB_USER"),
            password=need("DB_PASSWORD"),
            host=need("DB_HOST"),
            port=int(need("DB_PORT")),
            name=need("DB_NAME"),
        )


frequency = {
    'Fortnightly': 14,
    'Weekly': 7,
    'Monthly': 30,
    'Quarterly': 90,
    'Bi-Weekly': 14,
    'Annually': 365,
    'Every 3 Months': 90,
}
labels = ['Young Adult', 'Adult', 'Middle-aged', 'Senior']


logger.info(f'Programme {__name__} has started ETL process.')


def read_csv(filepath: Path) -> pd.DataFrame:
    logger.info("Reading CSV: %s", filepath)
    return pd.read_csv(filepath)


def standardize_pd(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Standardizing DataFrame...')
    df = df.copy()
    df.columns = (df.columns.str.strip()
                  .str.lower()
                  .str.replace(' ', '_', regex=False)
                  )
    df = df.rename(columns={'purchase_amount_(usd)': 'purchase_amount'})
    return df


def impute_rating(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "review_rating" in df.columns and "category" in df.columns:
        df['review_rating'] = df.groupby('category')['review_rating'].transform(lambda x: x.fillna(x.median()))
    return df


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Adding new Features to DataFrame...')
    df = df.copy()

    if 'age' in df.columns:
        df['age_group'] = pd.qcut(df['age'], q=4, labels=labels)
    if 'frequency_of_purchases' in df.columns:
        df['purchase_frequency_days'] = df['frequency_of_purchases'].map(frequency)

    logger.info("Columns: \n %s", df.columns.tolist())

    return df


def drop_column(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Deleting unused column...')
    df = df.copy()
    df = df.drop('promo_code_used', axis=1, errors='ignore')
    return df

def build_engine(config: Config) -> Engine:
    logger.info('Creating Engine to connect to DataBase.')
    url =  f"postgresql+psycopg2://{config.user}:{config.password}@{config.host}:{config.port}/{config.name}"
    return sqlalchemy.create_engine(url)

def load_to_postgres(df: pd.DataFrame, engine: Engine, table_name: str) -> None:
    logger.info('Connecting to PostgresSQL table: %s', table_name)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    logger.info("Loaded {} rows into {}.".format(len(df), table_name))


def main():
    code_dir = Path(__file__).resolve().parent
    project_root = code_dir.parent
    file_path = project_root / "data" / "customer_shopping_behavior.csv"

    cfg = Config.from_env()
    engine = build_engine(cfg)

    df = read_csv(file_path)
    df = standardize_pd(df)
    df = impute_rating(df)
    df = add_features(df)
    df = drop_column(df)

    logger.info("Columns: %s", df.columns.tolist())
    logger.info("Sample:\n%s", df.head(5).to_string(index=False))

    load_to_postgres(df, engine, table_name="customer")

    logger.info("ETL finished successfully")

if __name__ == "__main__":
    main()
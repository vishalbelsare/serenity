import os

import pandas as pd
import quandl

from money import Money

from sqlalchemy import create_engine, TypeDecorator
from sqlalchemy.dialects.postgresql import MONEY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

Base = declarative_base()


def create_sharadar_session(hostname: str = os.getenv('TIMESCALEDB_NODEPORT_SERVICE_HOST', 'localhost'),
                            port: int = os.getenv('TIMESCALEDB_NODEPORT_SERVICE_PORT', '5432'),
                            username: str = os.getenv('POSTGRES_SHARADAR_USER', 'sharadar'),
                            password: str = os.getenv('POSTGRES_SHARADAR_PASSWORD', None)) -> Session:
    engine = create_engine(f'postgresql://{username}:{password}@{hostname}:{port}/')
    session = sessionmaker(bind=engine)
    return session()


def init_quandl(quandl_api_key: str = os.getenv('QUANDL_API_KEY')):
    quandl.ApiConfig.api_key = quandl_api_key


def clean_nulls(value):
    if pd.isnull(value):
        return None
    else:
        return value


class USD(TypeDecorator):
    impl = MONEY

    @property
    def python_type(self):
        raise Money

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(MONEY())

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        else:
            return str(value)

    def process_literal_param(self, value, dialect):
        raise NotImplementedError()

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return Money(value.replace(',', '').replace('$', ''), 'USD')

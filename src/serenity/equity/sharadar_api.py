import os

import quandl
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


def create_sharadar_session(hostname: str = os.getenv('TIMESCALEDB_NODEPORT_SERVICE_HOST', 'localhost'),
                            port: int = os.getenv('TIMESCALEDB_NODEPORT_SERVICE_PORT', '5432'),
                            username: str = os.getenv('POSTGRES_SHARADAR_USER', 'sharadar'),
                            password: str = os.getenv('POSTGRES_SHARADAR_PASSWORD', None)):
    engine = create_engine(f'postgresql://{username}:{password}@{hostname}:{port}/')
    session = sessionmaker(bind=engine)
    return session()


def init_quandl(quandl_api_key: str = os.getenv('QUANDL_API_KEY')):
    quandl.ApiConfig.api_key = quandl_api_key



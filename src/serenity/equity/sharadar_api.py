import os

import quandl
from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Session, sessionmaker

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


class UnitType(Base):
    __tablename__ = 'unit_type'

    unit_type_id = Column(Integer, primary_key=True)
    unit_type_code = Column(String(32))

    def __init__(self, unit_type_code: str):
        self.unit_type_code = unit_type_code

    @classmethod
    def find_by_code(cls, session: Session, unit_type_code: str):
        return session.query(UnitType).filter(UnitType.unit_type_code == unit_type_code).one_or_none()


class Indicator(Base):
    __tablename__ = 'indicators'

    indicator_id = Column(Integer, primary_key=True)
    table_name = Column(String(32))
    indicator = Column(String(32))
    is_filter = Column(Boolean)
    is_primary_key = Column(Boolean)
    title = Column(String(256))
    description = Column(String(2048))
    unit_type_id = Column(Integer, ForeignKey('unit_type.unit_type_id'))
    unit_type = relationship('UnitType')

    @classmethod
    def find_by_name(cls, session: Session, table_name: str, indicator: str):
        return session.query(Indicator).filter(Indicator.table_name == table_name,
                                               Indicator.indicator == indicator).one_or_none()

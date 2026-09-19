from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

connection = "postgresql://admin:admin@postgres:5432/moja_baza"
engine = create_engine(connection)

Session = sessionmaker(bind=engine)
Base = declarative_base()
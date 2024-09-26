from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase


DB_ENGINE = 'postgresql'
DB_USER = 'sale_klosar_user'
DB_PASSWORD = 'sale_klosar_pass'
DB_HOST = 'localhost'
DB_PORT = 31234
DB_NAME = 'sale_klosar_db'

db_url = f'{DB_ENGINE}+psycopg2://' \
         f'{DB_USER}:{DB_PASSWORD}' \
         f'@{DB_HOST}:{DB_PORT}/' \
         f'{DB_NAME}'

print("Connecting to the database")

engine = create_engine(url=db_url)
SessionLocal = sessionmaker(autoflush=True, bind=engine)

class Base(DeclarativeBase):
    pass

db_session = SessionLocal()
print('Database connected!!!')

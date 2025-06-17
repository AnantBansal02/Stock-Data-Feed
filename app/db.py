# app/db.py - Database connection and session management

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import os

load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Sync DATABASE URL (psycopg2)
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Async DATABASE URL (asyncpg)
ASYNC_DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Sync engine and session
sync_engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SyncSessionLocal = scoped_session(sessionmaker(bind=sync_engine, autocommit=False, autoflush=False))

# Async engine and session
async_engine = create_async_engine(ASYNC_DATABASE_URL, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(bind=async_engine, expire_on_commit=False, class_=AsyncSession)

# Base model for both (shared)
Base = declarative_base()

# Dependency for sync routes
def get_sync_session():
    db: Session = SyncSessionLocal()
    try:
        yield db
    finally:
        db.close()

# Dependency for async routes
async def get_async_session():
    async with AsyncSessionLocal() as session:
        yield session
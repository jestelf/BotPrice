from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from .config import settings
import ssl

connect_args = {}
if settings.DB_URL.startswith("postgresql"):
    connect_args["ssl"] = ssl.create_default_context()

engine = create_async_engine(settings.DB_URL, echo=False, future=True, connect_args=connect_args)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

class Base(DeclarativeBase):
    pass

async def get_session() -> AsyncSession:
    async with SessionLocal() as session:
        yield session

async def init_db():
    from . import models  # import to register metadata
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)

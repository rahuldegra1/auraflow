import asyncio

# Explicitly import all models so SQLAlchemy's metadata registers them
import auraflow.models
from auraflow.core.database import Base, engine

try:
    import auraflow.models.run
    import auraflow.models.task
    import auraflow.models.workflow
except ImportError:
    pass

async def main():
    print(f"Registered tables in Base.metadata: {list(Base.metadata.tables.keys())}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Database tables initialized successfully!")

if __name__ == "__main__":
    asyncio.run(main())

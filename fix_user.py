import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from core.config import settings

async def update_db():
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as db:
        await db.execute(text("UPDATE users SET metsights_profile_id = 'test_profile_' || user_id::text WHERE metsights_profile_id IS NULL"))
        await db.execute(text("UPDATE assessment_instances SET metsights_record_id = 'test_record_' || assessment_instance_id::text WHERE metsights_record_id IS NULL"))
        await db.commit()
    print('DB updated successfully for ALL users!')

if __name__ == "__main__":
    asyncio.run(update_db())

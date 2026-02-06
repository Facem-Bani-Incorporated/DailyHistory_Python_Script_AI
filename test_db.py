import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# FOLOSEȘTE URL-UL CARE "NU MERGE" AICI (cel cu postgresql://...)
URL_NORMAL = "postgresql://postgres:DqZqSSWSMzuyoaHioJqfrALlRdKipdpE@turntable.proxy.rlwy.net:56263/railway"


async def test_conn():
    # Adăugăm manual prefixul async ca să vedem dacă asta e buba
    async_url = URL_NORMAL.replace("postgresql://", "postgresql+asyncpg://")
    print(f"Trying: {async_url}")

    engine = create_async_engine(async_url)
    try:
        async with engine.connect() as conn:
            res = await conn.execute(text("SELECT 1"))
            print(f"✅ REUȘIT: {res.fetchone()}")
    except Exception as e:
        print(f"❌ EȘUAT: {e}")
    finally:
        await engine.dispose()


asyncio.run(test_conn())
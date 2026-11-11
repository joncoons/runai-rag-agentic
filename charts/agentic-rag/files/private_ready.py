"""Wait only for the isolated schema; no permanent database credentials needed."""
import asyncio,os
import asyncpg
async def main():
 for attempt in range(180):
  connection=None
  try:
   connection=await asyncpg.connect(os.environ['PRIVATE_POSTGRES_DSN'],timeout=5)
   if not await connection.fetchval("SELECT to_regclass('private_control.workspaces') IS NOT NULL"):raise ValueError()
   return
  except Exception:
   if attempt==179:raise RuntimeError('Private schema readiness deadline exceeded') from None
   await asyncio.sleep(5)
  finally:
   if connection:await connection.close()
asyncio.run(main())

from motor import motor_asyncio


async def connect_to_mongodb():
  client = motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
  db = client["test"]
  collection = db["sample_collection"]
  return collection
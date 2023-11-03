from datetime import datetime
from config.database import connect_to_mongodb


def create_aggregation_pipeline(dt_from: str, dt_upto: str, group_type: str) -> list:
  date_formats = {
    "month": "%Y-%m",
    "day": "%Y-%m-%d",
    "hour": "%Y-%m-%dT%H:00:00"
  }
  date_format = date_formats.get(group_type, "%Y-%m")

  pipeline = [
      {"$match": {"dt": {"$gte": datetime.fromisoformat(dt_from), "$lte": datetime.fromisoformat(dt_upto)}}},
      {"$group": {"_id": {"$dateToString": {"format": date_format, "date": "$dt"},}, "total": {"$sum": "$value"}}},
      {"$sort": {"_id": 1}},
  ]

  if group_type != 'hour':
    rest_part = ["$_id", "-01T00:00:00"] if group_type == "month" else ["$_id", "T00:00:00"]
    project = {"$project": {"_id": {"$concat": rest_part}, "total": 1}}
    pipeline.append(project)

  return pipeline


async def aggregate_data(dt_from: str, dt_upto: str, group_type: str) -> dict:
  try:
    collection = await connect_to_mongodb()
    pipeline = create_aggregation_pipeline(dt_from, dt_upto, group_type)
    cursor = collection.aggregate(pipeline=pipeline)

    result = {
      "dataset": [],
      "labels": []
    }
    async for document in cursor: 
      result["dataset"].append(document["total"])
      result["labels"].append(document["_id"])

    return result
  except Exception as e:
    print(f"An error occured: {str(e)}")
    return None
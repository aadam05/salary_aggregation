from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
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


async def get_result(dt_from: str, dt_upto: str, group_type: str, cursor):
  date1 = datetime.fromisoformat(dt_from)
  date2 = datetime.fromisoformat(dt_upto)

  delta_formats = {
    "month": relativedelta(months=1),
    "day": timedelta(days=1),
    "hour": timedelta(hours=1)
  }

  date_dict = {}
  current_date = date1

  # Создаем словарь со всеми датами с dt_from по dt_upto 
  # даем им значения 0
  while current_date <= date2:
    date_dict[current_date.strftime("%Y-%m-%dT%H:%M:%S")] = 0
    current_date += delta_formats[group_type]

  # Назначем сумму зарплат по нужным датам  
  async for document in cursor: 
    doc_id = document["_id"]
    if doc_id in date_dict:
      date_dict[doc_id] = document["total"]

  # создаем валидный объект, для телеграм бота 
  result = {
    "dataset": list(date_dict.values()),
    "labels": list(date_dict.keys())
  }

  return result


async def aggregate_data(dt_from: str, dt_upto: str, group_type: str) -> dict:
  try:
    collection = await connect_to_mongodb()
    pipeline = create_aggregation_pipeline(dt_from, dt_upto, group_type)
    cursor = collection.aggregate(pipeline=pipeline)

    return await get_result(dt_from, dt_upto, group_type, cursor)
  except Exception as e:
    print(f"An error occured: {str(e)}")
    return "Произошла ошибка"
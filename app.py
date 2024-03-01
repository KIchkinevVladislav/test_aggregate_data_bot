import pandas as pd
from pymongo import MongoClient


def aggregate_salary_data(dt_from, dt_upto, group_type):
    # Подключение к MongoDB
    client = MongoClient('localhost', 27017)

    db = client['sampleDB']
    collection = db['sample_collection']

    dt_from = pd.to_datetime(dt_from)
    dt_upto = pd.to_datetime(dt_upto)

    # агрегация по месяцам
    if group_type == 'month':
        aggregated_data = collection.aggregate([
            {'$match': {'$and': [{'dt': {'$gte': dt_from}}, {'dt': {'$lte': dt_upto}}]}},
            {'$group': {'_id': {'$dateToString': {'format': '%Y-%m', 'date': '$dt'}}, 'total_salary': {'$sum': '$value'}}},
            {'$sort': {'_id': 1}}
        ])

    dataset = []
    labels = []

    for doc in aggregated_data:
        dataset.append(doc['total_salary'])
        labels.append(doc['_id'] + '-01T00:00:00')

    return {'dataset': dataset, 'labels': labels}


# Пример запроса на агрегацию данных
input_data = {
   "dt_from": "2022-09-01T00:00:00",
   "dt_upto": "2022-12-31T23:59:00",
   "group_type": "month"
}

result = aggregate_salary_data(input_data["dt_from"], input_data["dt_upto"], input_data["group_type"])
print(result)


# x = {"dataset": [5906586, 5515874, 5889803, 6092634],
# "labels": ["2022-09-01T00:00:00", "2022-10-01T00:00:00", "2022-11-01T00:00:00", "2022-12-01T00:00:00"]}
# y = {'dataset': [5906586, 5515874, 5889803, 6092634], 'labels': ['2022-09-01T00:00:00', '2022-10-01T00:00:00', '2022-11-01T00:00:00', '2022-12-01T00:00:00']}

# print(x == y)

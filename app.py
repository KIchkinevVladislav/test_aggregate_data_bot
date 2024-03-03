import pandas as pd
from pymongo import MongoClient
import json
import datetime

import config


def aggregate_salary_data(dt_from, dt_upto, group_type):
    # Подключение к MongoDB
    client = MongoClient(config.MONGO_HOST, config.MONGO_PORT)

    db = client[config.MONGO_DB]
    collection = db[config.MONGO_COLLECTION]

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

        return json.dumps({'dataset': dataset, 'labels': labels})

    # агрегация по дням
    elif group_type == 'day':
        # Проверяем, является ли время конечной даты T00:00:00
        if dt_upto.time() == datetime.time(0, 0):
            date_range = pd.date_range(start=dt_from, end=dt_upto, freq='D')
            add_zero_for_last_day = True
        else:
            date_range = pd.date_range(start=dt_from, end=dt_upto, freq='D')
            add_zero_for_last_day = False

        aggregated_data = []
        labels = date_range.strftime('%Y-%m-%dT%H:%M:%S').tolist()

        for date in date_range:
            data_for_day = collection.aggregate([
                {'$match': {'dt': {'$gte': date, '$lt': date + pd.Timedelta(days=1)}}},
                {'$group': {'_id': None, 'total_salary': {'$sum': '$value'}}}
            ])
            
            total_salary = next(data_for_day, {'total_salary': 0})['total_salary']
            aggregated_data.append(total_salary)

        # Добавляем 0 для последнего дня, если время конечной даты T00:00:00
        if add_zero_for_last_day:
            aggregated_data[-1] = 0

        return json.dumps({"dataset": aggregated_data, "labels": [str(label) for label in labels]})
    
    # агрегация по часам  
    elif group_type == 'hour':
        # Проверяем, является ли время конечной даты T00:00:00
        if dt_upto.time() == datetime.time(0, 0): 
            date_range = pd.date_range(start=dt_from, end=dt_upto, freq='h')
            add_zero_for_last_hour = True
        else:
            date_range = pd.date_range(start=dt_from, end=dt_upto, freq='h')
            add_zero_for_last_hour = False

        aggregated_data = []
        for date in date_range:
            data_for_hour = collection.aggregate([
                {'$match': {'dt': {'$gte': date, '$lt': date + pd.Timedelta(hours=1)}}},
                {'$group': {'_id': None, 'total_salary': {'$sum': '$value'}}}
            ])
            total_salary = next(data_for_hour, {'total_salary': 0})['total_salary']

            aggregated_data.append(total_salary)

        # Добавляем 0 для последнего часа, если время конечной даты T00:00:00
        if add_zero_for_last_hour:
            aggregated_data[-1] = 0

        labels = date_range.strftime('%Y-%m-%dT%H:%M:%S').tolist()
        return json.dumps({"dataset": aggregated_data, "labels": [str(label) for label in labels]})    
    else:
        raise ValueError('Неподдерживаемый тип агрегации')
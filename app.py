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
    # агрегация по дням
    elif group_type == 'day':
        date_range = pd.date_range(start=dt_from, end=dt_upto, freq='D')

        aggregated_data = []

        for date in date_range:
            
            data_for_day = collection.aggregate([
                {'$match': {'dt': {'$gte': date, '$lt': date + pd.Timedelta(days=1)}}},
                {'$group': {'_id': None, 'total_salary': {'$sum': '$value'}}}
            ])

            total_salary = next(data_for_day, {'total_salary': 0})['total_salary']
            aggregated_data.append(total_salary)

        labels = date_range.strftime('%Y-%m-%dT%H:%M:%S').tolist()
        return {'dataset': aggregated_data, 'labels': labels}
    # агрегация по часа  
    elif group_type == 'hour':

        date_range = pd.date_range(start=dt_from, end=dt_upto, freq='h')

        aggregated_data = []
        for date in date_range:
            data_for_hour = collection.aggregate([
                {'$match': {'dt': {'$gte': date, '$lt': date + pd.Timedelta(hours=1)}}},
                {'$group': {'_id': None, 'total_salary': {'$sum': '$value'}}}
            ])
            total_salary = next(data_for_hour, {'total_salary': 0})['total_salary']

            aggregated_data.append(total_salary)

        aggregated_data[-1] = 0

        labels = date_range.strftime('%Y-%m-%dT%H:%M:%S').tolist()
        return {'dataset': aggregated_data, 'labels': labels}
    
    else:
        raise ValueError('Неподдерживаемый тип агрегации')
import os
from pathlib import Path
import environ
"""
Конфигурации проекта
"""
env = environ.Env(
    MONGO_HOST=(str),
    MONGO_PORT=(int),
    MONGO_DB=(str),
    MONGO_COLLECTION=(str),
)

BASE_DIR = Path(__file__).resolve().parent.parent
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))


MONGO_HOST = env('MONGO_HOST', default='localhost')

MONGO_PORT = env('MONGO_PORT', default=27017)

MONGO_DB = env('MONGO_DB', default='sampleDB')

MONGO_COLLECTION = env('MONGO_COLLECTION', default='sample_collection')
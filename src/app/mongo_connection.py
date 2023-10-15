from pymongo import MongoClient
from decouple import config

# Obter a connection string a partir das variáveis de ambiente
MONGO_CONNECTION_STRING = config('MONGO_CONNECTION_STRING')

client = MongoClient(MONGO_CONNECTION_STRING)
db = client['project_management']

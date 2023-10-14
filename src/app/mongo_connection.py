from pymongo import MongoClient

client = MongoClient(
    'mongodb+srv://bruunamoura41:ftb123@cluster0.gz86vmy.mongodb.net/'
    '?retryWrites=true&w=majority')
db = client['project_management']

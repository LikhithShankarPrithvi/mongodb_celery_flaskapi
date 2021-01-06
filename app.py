# from flask import Flask
# from flask_pymongo import pymongo
# import json
# from bson.objectid import ObjectId
import pymongo
from celery import Celery
broker_url='amqp://localhost//'
backend_url='mongodb+srv://student0:<password>@studentid.8h5vs.mongodb.net/<dbname>?retryWrites=true&w=majority'
app = Celery('app', broker=broker_url, backend='mongodb+srv://student0:student0@studentid.8h5vs.mongodb.net/<dbname>?retryWrites=true&w=majority')

@app.task
def hello():
    return("hello world")

client = pymongo.MongoClient("mongodb+srv://student0:student0@studentid.8h5vs.mongodb.net/<dbname>?retryWrites=true&w=majority")
#print(client.list_database_names())
#print(db.list_collection_names())
db=client.school_db
students=db.students
#Id:objectId, name: string, email: string, password: string, percentage : float


@app.task
def addStudent(id,name,email,password):
    stu_doc={
        "id":id,
        "name":str(name),
        "email":str(email),
        "password":str(password),
        "percentage":float(0)
    }
    students.insert(stu_doc)


# print(client.list_database_names())
# print(db.list_collection_names())
# print(students.find_one({"name":"likhith"}))

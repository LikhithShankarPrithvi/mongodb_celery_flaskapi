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
subjects=db.subjects
student_marks=db.student_marks
#Id:objectId, name: string, email: string, password: string, percentage : float


@app.task
def addStudent(name,email,password):
    stu_doc={
        "name":str(name),
        "email":str(email),
        "password":str(password),
        "percentage":float(0)
    }
    students.insert(stu_doc)

@app.task
def addSubjects(subject):
    sub_doc={
        'name':str(subject)
    }
    subjects.insert(sub_doc)

@app.task
def addMarks(stuName,subName,marks):
    stu_id=students.find_one({'name':str(stuName)})
    stu_id=str(stu_id['_id'])
    sub_id=subjects.find_one({'name':str(subName)})
    sub_id=str(sub_id['_id'])
    marks_doc={
        'student_id':stu_id,
        'subject_id':sub_id,
        'marks':marks
    }
    student_marks.insert(marks_doc)

print(client.list_database_names())
print(db.list_collection_names())
for each in students.find({}):
    print(each)
for each in subjects.find({}):
    print(each)
for each in student_marks.find({}):
    print(each)


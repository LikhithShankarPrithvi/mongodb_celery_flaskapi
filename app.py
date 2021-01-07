from flask import Flask
# from flask_pymongo import pymongo
# import json
# from bson.objectid import ObjectId
import pymongo
import bson
from celery import Celery
from flask_celery import make_celery
broker_url='amqp://localhost//'
backend_url='mongodb+srv://student0:<password>@studentid.8h5vs.mongodb.net/<dbname>?retryWrites=true&w=majority'
api = Celery('app', broker=broker_url, backend='mongodb+srv://student0:student0@studentid.8h5vs.mongodb.net/<dbname>?retryWrites=true&w=majority')


app=Flask(__name__)
app.config['CELERY_BROKER_URL']='amqp://localhost//'
app.config['CELERY_RESULT_BACKEND']='mongodb+srv://student0:<password>@studentid.8h5vs.mongodb.net/<dbname>?retryWrites=true&w=majority'
celery=make_celery(app)

@app.route('/hello')
def hello():
    return("hello world")

client = pymongo.MongoClient("mongodb+srv://student0:student0@studentid.8h5vs.mongodb.net/<dbname>?retryWrites=true&w=majority")
#print(client.list_database_names())
#print(db.list_collection_names())
db=client.school_db
students=db.students
subjects=db.subjects
student_marks=db.student_marks
global number_of_subjects
number_of_subjects=0
#Id:objectId, name: string, email: string, password: string, percentage : float

@app.route("/addStudent/values?name=<name>email=<email>password=<password>")
def addingStudent(name,email,password):
    addStudent.delay(name,email,password)
    return OK

@celery.task(name="app.addStudent")
def addStudent(name,email,password):
    stu_doc={
        "name":str(name),
        "email":str(email),
        "password":str(password),
        "percentage":float(0)
    }
    students.insert(stu_doc)

@celery.task(name="app.addSubjects")
def addSubjects(subject):
    sub_doc={
        'name':str(subject)
    }
    subjects.insert(sub_doc)


@celery.task(name="app.addMarks")
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
    tot_subjects=student_marks.find({'student_id':stu_id}).count()
    number_of_subjects=subjects.find({}).count()
    if(tot_subjects==number_of_subjects):
        sum_of_marks=0
        for each in subjects.find({}):
            subId=str(each['_id'])
            marks_in_each=student_marks.find({'student_id':stu_id,'subject_id':subId})
            for doc in marks_in_each:
                print(doc['marks'])
                sum_of_marks+=int(doc['marks'])
            #marks_in_each=int(marks_in_each['marks'])
        percent=sum_of_marks/number_of_subjects
        students.update_one(
            {'name':stuName},
            {'$set':{'percentage':percent}},
            upsert=True
        )


print(client.list_database_names())
print(db.list_collection_names())
for each in students.find({}):
    print(each)
for each in subjects.find({}):
    print(each)
for each in student_marks.find({}):
    print(each)

if(__name__ == '__main__'):
    app.run(debug=True)

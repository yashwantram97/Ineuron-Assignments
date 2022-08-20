import mysql.connector as connection
from flask import  Flask , request, jsonify
import pymongo
from pymongo import MongoClient
app = Flask(__name__)

mydb = connection.connect(host='localhost', user='root', passwd= '')

############################################################################
########### BEFORE RUNNING THIS CODE MAKE SURE YOU CREATE A MYSQL ##########
########### DB NAME "YASH" AND TABLE NAME "INEURON" WITH FIELDS  ##########
########### "ID, EMP_NAME, EMP_MAIL_ID, EMP_SALARY" AND CREATE    ##########
########### AN ATLAS ACCOUNT WITH A DATABASE OF "EMPLOYEE"        ##########
############################################################################

###################################################################################
# Post man collection - https://www.getpostman.com/collections/aa7eb0dad532090760ac
###################################################################################

def get_database():

    CONNECTION_STRING = "mongodb+srv://admin:<password>@<cluster>.mongodb.net/employee"
    client = MongoClient(CONNECTION_STRING)
    return client['employee']

@app.route('/employee/mysql/insert',methods=['POST'])
def employeInsertMysql():
    response = {}
    cursor = None
    try:
        cursor = mydb.cursor()
        insert_query = """insert into yash.ineuron (employee_id, emp_name, emp_mail_id, emp_salary) values(%(id)s,%(name)s,%(email)s,%(salary)s)"""
        cursor.execute(insert_query, request.json)
        mydb.commit()
        cursor.close()
        response['message'] = 'Data inserted successfully'
        return jsonify(response)
    except Exception as e:
        if cursor:
            cursor.close()
        response['error'] = e
        return jsonify(str(response))

@app.route('/employees/mysql/',methods=['GET'])
def getAllEmployeeMysql():
    response = {}
    cursor = None
    try:
        cursor = mydb.cursor()
        get_query = """select * from yash.ineuron"""
        cursor.execute(get_query)
        response['data'] = []
        for val in cursor.fetchall():
            employee = dict()
            employee['id'], employee['name'], employee['email'], employee['salary'] = val
            response['data'].append(employee)
        cursor.close()
        response['message'] = 'Data fetched successfully'
        return jsonify(response)
    except Exception as e:
        if cursor:
            cursor.close()
        response['error'] = e
        print(e)
        return jsonify(str(response))

@app.route('/employee/mysql/update',methods=['PUT'])
def employeUpdateMysql():
    response = {}
    cursor = None
    try:
        updateField = request.json['updateField']
        updateValue = request.json['updateValue']
        id = request.json['id']
        cursor = mydb.cursor()
        insert_query = f"""update yash.ineuron set {updateField} = %s where employee_id = %s"""
        cursor.execute(insert_query,(updateValue, id))
        mydb.commit()
        cursor.close()
        response['message'] = 'Data updated successfully'
        return jsonify(response)
    except Exception as e:
        if cursor:
            cursor.close()
        response['error'] = e
        return jsonify(str(response))

@app.route('/employee/mysql/',methods=['DELETE'])
def deleteEmployeeMysql():
    response = {}
    cursor = None
    try:
        cursor = mydb.cursor()
        delete_query = f"""DELETE from yash.ineuron where employee_id = {request.json['id']}"""
        cursor.execute(delete_query)
        mydb.commit()
        cursor.close()
        response['message'] = 'Data deleted successfully'
        return jsonify(response)
    except Exception as e:
        if cursor:
            cursor.close()
        response['error'] = e
        print(e)
        return jsonify(str(response))


@app.route('/employee/mongo/insert',methods=['POST'])
def employeInsertMongo():
    response = {}
    try:
        collection_name = db_name['info']
        collection_name.insert_one(request.json)
        response['message'] = 'Data inserted successfully'
        return jsonify(response)
    except Exception as e:
        response['error'] = e
        return jsonify(str(response))

@app.route('/employees/mongo/',methods=['GET'])
def employeeGetMongo():
    response = {}
    try:
        collection_name = db_name['info']
        infos = collection_name.find()
        response['data'] = []
        for info in infos:
            employee = dict()
            employee['id'], employee['name'], employee['email'], employee['salary'] = info['id'], info['name'], info['email'], info['salary']
            response['data'].append(employee)
        response['message'] = 'Data fetched successfully'
        return jsonify(response)
    except Exception as e:
        response['error'] = e
        return jsonify(str(response))

@app.route('/employee/mongo/update',methods=['PUT'])
def employeeUpdateMongo():
    response = {}
    try:
        collection_name = db_name['info']
        myquery = {"id": request.json['id']}
        newvalues = {"$set": {request.json['updateField']: request.json['updateValue']}}
        collection_name.update_one(myquery, newvalues)
        response['message'] = 'Data updated successfully'
        return jsonify(response)
    except Exception as e:
        response['error'] = e
        return jsonify(str(response))

@app.route('/employee/mongo/',methods=['DELETE'])
def deleteEmployeeMongo():
    response = {}
    try:
        collection_name = db_name['info']
        myquery = {"id": request.json['id']}
        collection_name.delete_one(myquery)
        response['message'] = 'Data deleted successfully'
        return jsonify(response)
    except Exception as e:
        response['error'] = e
        return jsonify(str(response))

if __name__=='__main__'  :
    db_name = get_database()
    app.run()

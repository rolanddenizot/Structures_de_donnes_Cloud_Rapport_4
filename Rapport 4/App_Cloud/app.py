from flask import Flask, render_template, redirect, request
from pymongo import MongoClient
import pandas as pd

app = Flask(__name__)
client = MongoClient('mongodb://mesiin592022-0022.westeurope.cloudapp.azure.com:30000/')
db = client['SalesDB']
collection = db['Unique_table']

classes = "table table-striped table-bordered"

lim = 5

@app.route('/', methods=['GET', 'POST'])
def home():
    global lim
    lim = request.args.get("lim")
    lim = 5 if lim is None else int(lim)
    return render_template('base.html')

@app.route('/requete1', methods=("POST", "GET"))
def requete1():
    R1 = [{'$match': {"FirstName_customer": "Joseph"}}, {'$project': {"SalesID": 1, "FirstName_customer": 1, "_id": 0}}]
    result = list(collection.aggregate(R1))
    df = pd.DataFrame(result).head(lim)
    return render_template('simple.html', requete_nb="Requete 1",  result=df.to_html(classes="table table-striped table-bordered", index=False))

@app.route('/requete2', methods=("POST", "GET"))
def requete2():
    R2 = [{'$match': {'LastName_employee': 'Ringer'}}, {'$group': {'_id': '$LastName_employee','Sum of quantity': {'$sum': '$Quantity'}}}]
    result = list(collection.aggregate(R2))
    df = pd.DataFrame(result).head(lim)
    return render_template('simple.html', requete_nb="Requete 2",  result=df.to_html(classes="table table-striped table-bordered", index=False))

@app.route('/requete3', methods=("POST", "GET"))
def requete3():
    R3 = [{'$match': {"Name":{ '$regex' : 'de', '$options' : 'i' }, "Price":{'$lt': 500}}},{'$project': {"SalesID":1,"Price":1,"_id":0}}]
    result = list(collection.aggregate(R3))
    df = pd.DataFrame(result).head(lim)
    return render_template('simple.html', requete_nb="Requete 3",  result=df.to_html(classes="table table-striped table-bordered", index=False))

@app.route('/requete4', methods=("POST", "GET"))
def requete4():
    R4 = [{'$match': {'$expr': {'$lt': [{'$strLenCP': '$FirstName_employee'}, {'$strLenCP': '$LastName_customer'}]}}}, {'$project': {'Name': 1,'FirstName_employee': 1,'LastName_customer': 1,'_id': 0}}]
    result = list(collection.aggregate(R4))
    df = pd.DataFrame(result).head(lim)
    return render_template('simple.html', requete_nb="Requete 4",  result=df.to_html(classes="table table-striped table-bordered", index=False))

@app.route('/requete5', methods=("POST", "GET"))
def requete5():
    middleName_employee = request.args.get("middleName_employee")
    middleName_employee = 'e' if middleName_employee is None else middleName_employee
    somme = request.args.get("somme")
    somme = 1000 if (somme is None or somme=='') else int(somme)

    R5 = [{'$group': {'_id': '$CustomerID','FirstName_customer': {'$first': '$FirstName_customer'},'LastName_customer': {'$first': '$LastName_customer'},'MiddleInitial_employee': {'$first': '$MiddleInitial_employee'},'Somme du nombre d\'achats': {'$sum': '$Quantity'}}}, {'$match': {'MiddleInitial_employee': middleName_employee,'Somme du nombre d\'achats': {'$gte': somme}}}, {'$project': {'FirstName_customer': 1,'LastName_customer': 1,'_id': 0}}, {'$sort': {'LastName_customer': 1}}]
    result = list(collection.aggregate(R5))
    df = pd.DataFrame(result).head(lim)
    return render_template('hard_5.html', requete_nb="Requete 5", result=df.to_html(classes="table table-striped table-bordered", index=False), req=R5)

@app.route('/requete6', methods=("POST", "GET"))
def requete6():
    limite = request.args.get("limite")
    limite = 1 if limite is None else int(limite)

    R6 = [{'$project': {'Benefit': {'$multiply': ['$Quantity','$Price']},'EmployeeID': 1,'FirstName_employee': 1,'Quantity': 1,'CustomerID': 1,'Price': 1}}, {'$group': {'_id': {'EmployeeID': '$EmployeeID','CustomerID': '$CustomerID'},'CustomerID': {'$first': '$CustomerID'},'EmployeeID': {'$first': '$EmployeeID'},'FirstName_employee': {'$first': '$FirstName_employee'},'Somme des benefices': {'$sum': '$Benefit'}}}, {'$sort': {'Somme des benefices': 1}}, {'$limit': limite}, {'$project': {'FirstName_employee': 1,'_id': 0}}]
    result = list(collection.aggregate(R6))
    df = pd.DataFrame(result).head(limite)
    return render_template('hard_6.html', requete_nb="Requete 6", result=df.to_html(classes="table table-striped table-bordered", index=False), req=R6)

@app.route('/requete7', methods=("POST", "GET"))
def requete7():
    nb = request.args.get("nb")
    nb = 3 if nb is None else int(nb)

    R7 = [{'$match': {'$expr': {'$ne': ['$FirstName_customer','$FirstName_employee']}}}, {'$group': {'_id': {'CustomerID': '$CustomerID','LastName_employee': '$LastName_employee'},'LastName_customer': {'$first': '$LastName_customer'},'DistinctCount': {'$sum': 1}}}, {'$match': {'DistinctCount': {'$gte': nb}}}, {'$project': {'LastName_customer': 1,'_id': 0}}, {'$sort': {'LastName_customer': 1}}]
    result = list(collection.aggregate(R7))
    df = pd.DataFrame(result).head(lim)
    return render_template('hard_7.html', requete_nb="Requete 7", result=df.to_html(classes="table table-striped table-bordered", index=False), req=R7)

@app.route('/requete8', methods=("POST", "GET"))
def requete8():
    middleName_employee1 = request.args.get("middleName_employee1")
    middleName_employee1 = 'e' if middleName_employee1 is None else middleName_employee1
    limite1 = request.args.get("limite1")
    limite1 = 1 if limite1 is None else int(limite1)

    R8 = [{'$match': {'MiddleInitial_employee': middleName_employee1}}, {'$group': {'_id': {'EmployeeID': '$EmployeeID','ProductID': '$ProductID'},'Name': {'$first': '$Name'},'Nb': {'$sum': '$Quantity'}}}, {'$sort': {'Nb': -1}}, {'$limit': limite1}, {'$project': {'Name': 1,'Nb': 1,'_id': 0}}]
    result = list(collection.aggregate(R8))
    df = pd.DataFrame(result).head(limite1)
    return render_template('hard_8.html', requete_nb="Requete 8", result=df.to_html(classes="table table-striped table-bordered", index=False), req=R8)

@app.route('/adminView', methods=("POST", "GET"))
def adminView():
    infos = db.command("collstats", "Unique_table")
    indexes_existants = list(infos["indexSizes"].keys())
    nb_shards = len(infos["shards"])
    nb_chunks = infos["nchunks"]
    stats = []
    for k, v in infos["shards"].items():
        stats.append({
            "Nom du Shard": k,
            "Nombre de documents": infos["shards"][k]["count"],
            "Taille des données stockées": str(round(infos["shards"][k]["size"] * 1e-6, 2)) + " Mo",
            "Pourcentage des données stockées": str(round(infos["shards"][k]["size"] / infos["size"] * 100, 2)) + "%"
        })
    stats = pd.DataFrame(stats).sort_values("Nom du Shard")
    return render_template('AdminView.html',
                           indexes_existants=indexes_existants,
                           nb_shards=nb_shards,
                           nb_chunks=nb_chunks,
                           result=stats.to_html(classes="table table-striped table-bordered", index=False))

if __name__ == "__main__":
    app.run(debug=True)
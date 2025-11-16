# IMPORTACIONES
from cassandra.cluster import Cluster
import uuid
import pydgraph
import os
from pymongo import MongoClient

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB_NAME", "test")

# CONEXIONES CON LAS DIFERENTES BASES DE DATOS

# Conexion a casandra
def cassandra_session():
    cluster = Cluster(['127.0.0.1'], connect_timeout=30)
    session = cluster.connect()
    return cluster, session

def cassandra_cerrar(cluster):
    cluster.shutdown()

# Conexion a dgraph
def dgraph_conexion():
    client_stub = pydgraph.DgraphClientStub(DGRAPH_URI)
    client = pydgraph.DgraphClient(client_stub)
    return client, client_stub

def dgraph_cerrar(client_stb):
    client_stb.close()

# Conexion a mongo
def mongo_conexion():
    db_name = MONGO_DB

    client = MongoClient(MONGO_URI)
    db = client[db_name]
    return client, db


def mongo_cerrar(client):
    client.close()

#PARTE TEST QUE SOLO ESTA DE FORMA TEMPORAL PARA PROBAR LAS CONEXIONES SE ELIMINARA EN EL FUTURO
#para provar crear y entrar en el entorno py -3.11 -m venv venv
# instalar las librerias pip install cassandra-driver pymongo pydgraph requests
# y ejecutar python connect.py

print("PROBANDO CASSANDRA")
try:
    cluster, session = cassandra_session() #intentamos conectar
    rows = session.execute("SELECT release_version FROM system.local;") # ejecutamos algo
    for row in rows:
        print("Cassandra OK ", row.release_version) # si funciono
        break
    cassandra_cerrar(cluster) # cerramos
except Exception as e:
    print("Error Cassandra:", e) # si no funciono


print("\nPROBANDO MONGODB")
try:
    client, db = mongo_conexion() #intentamos conectar
    print("MongoDB OK ", db.list_collection_names()) # ejecutamos algo y vemos si funciona
    mongo_cerrar(client) # si funciono cerramos
except Exception as e:
    print("Error MongoDB:", e) # sino vemos que paso


print("\nPROBANDO DGRAPH")
try:
    client, stub = dgraph_conexion() #intentamos conectar
    txn = client.txn() # transaccion vacia
    txn.discard() # la descartamos
    print("Dgraph OK ") # si podemos crear una transaccion vacia entonces tenemos conexion
    dgraph_cerrar(stub) # cerramos
except Exception as e:
    print("Error Dgraph:", e)# si no podemos
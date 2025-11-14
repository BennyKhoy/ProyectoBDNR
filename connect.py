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
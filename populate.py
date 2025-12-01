from connect import dgraph_conexion, dgraph_cerrar
from Dgraph.modelD import set_schema, load_data

def main():
    client, stub = dgraph_conexion()
    try:
        print("Creando schema de Dgraph")
        set_schema(client)
        print("Cargado datos de Dgraph")
        load_data(client)
        print("Carga de Dgraph termianda")

    finally:
        dgraph_cerrar(stub)
        print("Conexion de Dgraph cerrada")

if __name__ == "__main__":
    main()
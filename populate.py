import os
import csv
import random
import uuid
import datetime

# Importar conexiones y modelos
from connect import mongo_conexion, mongo_cerrar, cassandra_session, cassandra_cerrar, dgraph_conexion, dgraph_cerrar
import Mongo.insertsM as im
import Cassandra.modelC as mc
# Importamos funciones de carga de Dgraph
from Dgraph import modelD

# Configuración de Rutas para CSVs de Dgraph
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DGRAPH_DATA_DIR = os.path.join(BASE_DIR, 'data', 'data_dgraph')
NODES_DIR = os.path.join(DGRAPH_DATA_DIR, 'nodes')
REL_DIR = os.path.join(DGRAPH_DATA_DIR, 'relations')

# Asegurar directorios
os.makedirs(NODES_DIR, exist_ok=True)
os.makedirs(REL_DIR, exist_ok=True)

# Datos semilla
NOMBRES = ["Ana", "Beto", "Carla", "Daniel", "Elena", "Fernando", "Gaby", "Hector"]
APELLIDOS = ["Lopez", "Perez", "Ruiz", "Gomez", "Diaz", "Santos", "Mendez", "Ortega"]
CARRERAS_DATA = [
    {"nombre": "Ingenieria en Sistemas", "codigo": 3001, "desc": "Desarrollo de Software y TI", "facultad": "Ingenieria"},
    {"nombre": "Ciencia de Datos", "codigo": 3002, "desc": "Analisis de datos e IA", "facultad": "Ingenieria"}
]
MATERIAS_DATA = [
    {"nombre": "Bases de Datos NoSQL", "codigo": 2001, "depto": "Sistemas", "cat": "Obligatoria"},
    {"nombre": "Inteligencia Artificial", "codigo": 2002, "depto": "Sistemas", "cat": "Optativa"},
    {"nombre": "Programacion Web", "codigo": 2003, "depto": "Sistemas", "cat": "Obligatoria"}
]

def generar_datos_maestros():
    profesores = []
    for i in range(3):
        nombre = f"{random.choice(NOMBRES)} {random.choice(APELLIDOS)}"
        profesores.append({
            "nombre": nombre,
            "correo": f"{nombre.split()[0].lower()}@iteso.mx",
            "password": "123",
            "rol": "maestro",
            "uuid": uuid.uuid4(), # Para Cassandra
            "mongo_id": None # Se llenará al insertar en Mongo
        })
    return profesores

def generar_datos_alumnos():
    alumnos = []
    expediente_base = 700000
    for i in range(5):
        nombre = f"{random.choice(NOMBRES)} {random.choice(APELLIDOS)}"
        alumnos.append({
            "nombre": nombre,
            "correo": f"{nombre.split()[0].lower()}@alumno.iteso.mx",
            "password": "123",
            "rol": "alumno",
            "expediente": expediente_base + i, # Para Dgraph
            "uuid": uuid.uuid4(), # Para Cassandra
            "mongo_id": None # Se llenará al insertar en Mongo
        })
    return alumnos

def escribir_csv(filename, headers, data):
    path = filename
    with open(path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

def main():
    print("=== INICIANDO POBLADO DE DATOS MASIVO (3 BDs) ===")
    
    # 1. Generar Datos en Memoria
    print("Generando datos en memoria")
    profesores = generar_datos_maestros()
    alumnos = generar_datos_alumnos()
    carreras = CARRERAS_DATA
    materias = MATERIAS_DATA
    
    # Generar Cursos (Instancias de materias)
    cursos = []
    curso_code_base = 1000
    for m in materias:
        prof = random.choice(profesores)
        cursos.append({
            "codigo_str": f"CUR-{curso_code_base}",
            "codigo_int": curso_code_base, # Para Dgraph
            "nombre": m["nombre"] + " 2025",
            "periodo": "2025B",
            "estado": "Activo",
            "creditos": 8,
            "materia_ref": m,
            "profesor_ref": prof,
            "mongo_id": None,
            "uuid": uuid.uuid4() # Para Cassandra (logs de curso)
        })
        curso_code_base += 1

    #  MONGODB 
    print("\nPoblando MongoDB...")
    client_m, db_m = mongo_conexion()
    
    # Limpieza Mongo
    db_m.usuarios.drop()
    db_m.carreras.drop()
    db_m.materias.drop()
    db_m.cursos.drop()
    db_m.tareas.drop()
    db_m.entregas.drop()
    
    # Insertar Carreras
    carrera_mongo_ids = []
    for c in carreras:
        res = im.insertar_carrera(db_m, c["nombre"], c["desc"], c["facultad"], [])
        c["mongo_id"] = res.inserted_id
    
    # Insertar Materias
    for m in materias:
        res = im.insertar_materia(db_m, str(m["codigo"]), m["nombre"], "Desc...", m["cat"], [])
        m["mongo_id"] = res.inserted_id

    # Insertar Profesores
    for p in profesores:
        # Asumimos carrera 0 para profes por simplicidad
        res = im.insertar_usuario(db_m, p["nombre"], p["correo"], p["password"], "maestro", carreras[0]["mongo_id"])
        p["mongo_id"] = res.inserted_id

    # Insertar Cursos
    for c in cursos:
        res = im.insertar_curso(db_m, c["codigo_str"], c["nombre"], c["periodo"], c["estado"], c["profesor_ref"]["mongo_id"], c["materia_ref"]["mongo_id"])
        c["mongo_id"] = res.inserted_id

    # Insertar Alumnos y Entregas
    for a in alumnos:
        # Asignar carrera random
        carrera = random.choice(carreras)
        # Crear progreso (Inscrito en un curso random)
        curso_inscrito = random.choice(cursos)
        progreso = [{
            "curso_id": curso_inscrito["mongo_id"],
            "codigo_curso": curso_inscrito["codigo_str"],
            "nombre_curso": curso_inscrito["nombre"],
            "estado": "En curso"
        }]
        
        res = im.insertar_usuario(db_m, a["nombre"], a["correo"], a["password"], "alumno", carrera["mongo_id"], progreso)
        a["mongo_id"] = res.inserted_id
        a["curso_inscrito"] = curso_inscrito # Guardar ref para Dgraph y Cassandra

        # Crear tarea y entrega en Mongo para este alumno
        res_t = im.insertar_tarea(db_m, curso_inscrito["mongo_id"], "Tarea 1", "Investigacion", datetime.datetime.now(), 100)
        im.insertar_entrega(db_m, res_t.inserted_id, curso_inscrito["mongo_id"], a["mongo_id"], datetime.datetime.now(), 95, "link", "http://tarea.com")

    mongo_cerrar(client_m)
    print("MongoDB Terminado.")

    # CASSANDRA
    print("\nPoblando Cassandra...")
    cluster_c, session_c = cassandra_session()
    
    # Crear Schema
    mc.create_keyspace(session_c, 'proyecto_bdnr', 1)
    session_c.set_keyspace('proyecto_bdnr')
    mc.create_schema(session_c)
    
    # Insertar Logs y Datos usando los UUIDs generados arriba
    for a in alumnos:
        # Log de login
        mc.insert_log_usuario(session_c, str(a["uuid"]), "Inicio de sesion")
        # Inscripcion bitácora
        mc.insert_inscripcion(session_c, str(a["curso_inscrito"]["uuid"]), str(a["uuid"]), a["nombre"])
        # Asistencia
        mc.insert_asistencia(session_c, str(a["uuid"]), str(a["curso_inscrito"]["uuid"]), str(uuid.uuid4()), "Presente")
        # Mensaje simulado
        prof = a["curso_inscrito"]["profesor_ref"]
        mc.enviar_mensaje(session_c, str(a["uuid"]), a["nombre"], str(prof["uuid"]), "Hola profe, duda con la tarea.")

    for p in profesores:
        # Movimiento profesor
        # Buscar un curso que imparta
        curso_del_profe = next((c for c in cursos if c["profesor_ref"] == p), cursos[0])
        mc.insert_movimiento_profesor(session_c, str(p["uuid"]), str(curso_del_profe["uuid"]), "Calificar", "Tarea 1 calificada")

    cassandra_cerrar(cluster_c)
    print("Cassandra Terminado")

    # DGRAPH
    print("\nGenerando CSVs y Poblando Dgraph...")
    
    # Preparar datos para CSVs
    # Nodos
    csv_materias = [{'codigo': m['codigo'], 'nombre': m['nombre'], 'departamento': m['depto']} for m in materias]
    csv_carreras = [{'codigo': c['codigo'], 'nombre': c['nombre'], 'descripcion': c['desc']} for c in carreras]
    csv_profesores = [{'nombre': p['nombre'], 'correo': p['correo']} for p in profesores]
    csv_cursos = [{'codigo': c['codigo_int'], 'nombre': c['nombre'], 'descripcion': "Curso semestral", 'creditos': c['creditos']} for c in cursos]
    csv_alumnos = [{'expediente': a['expediente'], 'nombre': a['nombre'], 'correo': a['correo']} for a in alumnos]
    
    # Relaciones
    # Alumno inscrito en Curso
    csv_alumno_curso = [{'alumno_expediente': a['expediente'], 'curso_codigo': a['curso_inscrito']['codigo_int']} for a in alumnos]
    # Profesor imparte Curso
    csv_profesor_curso = [{'profesor_correo': c['profesor_ref']['correo'], 'curso_codigo': c['codigo_int']} for c in cursos]
    # Carrera tiene materias (Simulado: todas las carreras tienen todas las materias)
    csv_carrera_materia = []
    for c in carreras:
        for m in materias:
            csv_carrera_materia.append({'carrera_codigo': c['codigo'], 'materia_codigo': m['codigo']})
    # Materia tiene cursos
    csv_materia_curso = [{'materia_codigo': c['materia_ref']['codigo'], 'curso_codigo': c['codigo_int']} for c in cursos]

    # Escribir Archivos
    escribir_csv(os.path.join(NODES_DIR, 'materias.csv'), ['codigo', 'nombre', 'departamento'], csv_materias)
    escribir_csv(os.path.join(NODES_DIR, 'carreras.csv'), ['codigo', 'nombre', 'descripcion'], csv_carreras)
    escribir_csv(os.path.join(NODES_DIR, 'profesores.csv'), ['nombre', 'correo'], csv_profesores)
    escribir_csv(os.path.join(NODES_DIR, 'cursos.csv'), ['codigo', 'nombre', 'descripcion', 'creditos'], csv_cursos)
    escribir_csv(os.path.join(NODES_DIR, 'alumnos.csv'), ['expediente', 'nombre', 'correo'], csv_alumnos)
    
    # vacio
    escribir_csv(os.path.join(NODES_DIR, 'actividades.csv'), ['codigo', 'titulo', 'descripcion', 'fecha_limite'], [])
    escribir_csv(os.path.join(NODES_DIR, 'comentarios.csv'), ['codigo', 'cuerpo', 'fecha'], [])

    escribir_csv(os.path.join(REL_DIR, 'alumno_curso.csv'), ['alumno_expediente', 'curso_codigo'], csv_alumno_curso)
    escribir_csv(os.path.join(REL_DIR, 'profesor_curso.csv'), ['profesor_correo', 'curso_codigo'], csv_profesor_curso)
    escribir_csv(os.path.join(REL_DIR, 'carrera_materia.csv'), ['carrera_codigo', 'materia_codigo'], csv_carrera_materia)
    escribir_csv(os.path.join(REL_DIR, 'materia_curso.csv'), ['materia_codigo', 'curso_codigo'], csv_materia_curso)
    
    # vacio
    escribir_csv(os.path.join(REL_DIR, 'materia_prerequisito.csv'), ['materia1_codigo', 'materia2_codigo'], [])
    escribir_csv(os.path.join(REL_DIR, 'carrera_alumno.csv'), ['carrera_codigo', 'alumno_expediente'], [])
    escribir_csv(os.path.join(REL_DIR, 'profesor_alumno.csv'), ['profesor_correo', 'alumno_expediente'], [])
    escribir_csv(os.path.join(REL_DIR, 'curso_actividad.csv'), ['curso_codigo', 'actividad_codigo'], [])
    escribir_csv(os.path.join(REL_DIR, 'alumno_actividad.csv'), ['alumno_expediente', 'actividad_codigo'], [])
    escribir_csv(os.path.join(REL_DIR, 'actividad_comentario.csv'), ['actividad_codigo', 'comentario_codigo'], [])
    escribir_csv(os.path.join(REL_DIR, 'comentario_alumno.csv'), ['comentario_codigo', 'alumno_expediente'], [])

    # Cargar en Dgraph
    client_d, stub_d = dgraph_conexion()
    try:
        modelD.set_schema(client_d)
        
        # Llamar manualmente a las funciones de carga de modelD para asegurar orden
        # Nodos
        m_uids = modelD.load_materias(client_d, os.path.join(NODES_DIR, 'materias.csv'))
        car_uids = modelD.load_carreras(client_d, os.path.join(NODES_DIR, 'carreras.csv'))
        p_uids = modelD.load_profesores(client_d, os.path.join(NODES_DIR, 'profesores.csv'))
        cur_uids = modelD.load_cursos(client_d, os.path.join(NODES_DIR, 'cursos.csv'))
        a_uids = modelD.load_alumnos(client_d, os.path.join(NODES_DIR, 'alumnos.csv'))
        
        # Relaciones
        modelD.create_alumno_inscrito_en_edge(client_d, os.path.join(REL_DIR, 'alumno_curso.csv'), a_uids, cur_uids)
        modelD.create_profesor_profesor_curso_edge(client_d, os.path.join(REL_DIR, 'profesor_curso.csv'), p_uids, cur_uids)
        modelD.create_carrera_tiene_materias_edge(client_d, os.path.join(REL_DIR, 'carrera_materia.csv'), car_uids, m_uids)
        modelD.create_materia_tiene_cursos_edge(client_d, os.path.join(REL_DIR, 'materia_curso.csv'), m_uids, cur_uids)
        
        print("Dgraph Terminado.")
    except Exception as e:
        print(f"Error cargando Dgraph: {e}")
    finally:
        dgraph_cerrar(stub_d)

    print("\n=== CARGA COMPLETA EXITOSA ===")
    print("\n--- DATOS PARA PRUEBAS ---")
    print("\n[ALUMNOS]")
    for a in alumnos:
        print(f"Nombre: {a['nombre']} | Email: {a['correo']} | UUID (Cassandra): {a['uuid']} | Mongo ID: {a['mongo_id']} | Expediente (Dgraph): {a['expediente']}")
    
    print("\n[PROFESORES]")
    for p in profesores:
        print(f"Nombre: {p['nombre']} | Email: {p['correo']} | UUID (Cassandra): {p['uuid']} | Mongo ID: {p['mongo_id']}")

if __name__ == "__main__":
    main()
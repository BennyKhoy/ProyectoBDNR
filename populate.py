import os
import csv
import random
import uuid
import datetime
import pydgraph

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
NOMBRES = [
    "Ana", "Beto", "Carla", "Daniel", "Elena", "Fernando", "Gaby", "Hector",
    "Jon", "Daenerys", "Tyrion", "Cersei", "Jaime", "Arya", "Sansa", "Bran",
    "Robb", "Ned", "Catelyn", "Joffrey", "Theon", "Yara", "Ramsay", "Brienne"
]


APELLIDOS = [
    "Lopez", "Perez", "Ruiz", "Gomez", "Diaz", "Santos", "Mendez", "Ortega",
    "Snow", "Sand", "Rivers", "Pyke", "Storm", "Hill", "Stone", "Flowers", "Waters",
    "Lopez", "Perez", "Ruiz", "Gomez", "Diaz", "Santos", "Mendez", "Ortega",
    "Hernandez", "Martinez", "Rodriguez", "Garcia", "Ramirez", "Cruz",
    "Vargas", "Flores", "Navarro", "Castillo", "Morales", "Torres"
]

NOMBRES_CARRERAS = [
    "Ingenieria en Sistemas", "Ciencia de Datos", "Ingenieria Industrial",
    "Ingenieria Electronica", "Administracion de Empresas", "Mercadotecnia",
    "Biotecnologia", "Ingenieria Mecanica", "Ingenieria Quimica",
    "Arquitectura", "Diseño Digital", "Economia",
    "Finanzas", "Contaduria y Auditoria"
]


DESCRIPCIONES_CARR = [
    "Desarrollo de Software y TI",
    "Analisis de datos e IA",
    "Optimizacion de procesos",
    "Circuitos y comunicaciones",
    "Gestion empresarial",
    "Modelos economicos",
    "Calculo y ciencias aplicadas"
]

FACULTADES = [
    "Ingenieria",
    "Administracion",
    "Ciencias",
    "Arquitectura",
    "Economico Administrativa"
]

NOMBRES_MATERIAS = [
    "Bases de Datos NoSQL",
    "Inteligencia Artificial",
    "Programacion Web",
    "Sistemas Operativos",
    "Mineria de Datos",
    "Calculo Diferencial",
    "Estructuras de Datos",
    "Redes de Computadoras",
    "Modelado de Software"
]

DEPARTAMENTOS = [
    "Sistemas",
    "Matematicas",
    "Computacion",
    "Ingenieria",
    "Ciencias"
]

CATEGORIAS = ["Obligatoria", "Optativa"]

def generar_carreras():
    carreras = []
    usadas = set()
    codigo = 3001
    for _ in range(5):
        nombre = random.choice(NOMBRES_CARRERAS)
        while nombre in usadas:
            nombre = random.choice(NOMBRES_CARRERAS)
        usadas.add(nombre)
        carreras.append({
            "nombre": nombre,
            "codigo": codigo,
            "desc": random.choice(DESCRIPCIONES_CARR),
            "facultad": random.choice(FACULTADES)
        })
        codigo += 1
    return carreras

def generar_materias():
    materias = []
    codigo = 2001

    nombres_barajados = NOMBRES_MATERIAS.copy()
    random.shuffle(nombres_barajados)

    for i in range(5):
        nombre = nombres_barajados[i % len(nombres_barajados)]
        depto = random.choice(DEPARTAMENTOS)
        cat = random.choice(CATEGORIAS)

        materias.append({
            "nombre": nombre,
            "codigo": codigo,
            "depto": depto,
            "cat": cat
        })
        codigo += 1

    return materias


def generar_datos_maestros():
    profesores = []
    correos_usados = set()
    for i in range(5):
        nombre = f"{random.choice(NOMBRES)} {random.choice(APELLIDOS)}"
        correo = f"{nombre.split()[0].lower()}@iteso.mx"
        while correo in correos_usados:
            nombre = f"{random.choice(NOMBRES)} {random.choice(APELLIDOS)}"
            correo = f"{nombre.split()[0].lower()}@iteso.mx"
        correos_usados.add(correo)
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
    correos_usados = set()
    expediente_base = 700000
    for i in range(5):
        nombre = f"{random.choice(NOMBRES)} {random.choice(APELLIDOS)}"
        correo = f"{nombre.split()[0].lower()}@alumno.iteso.mx"
        while correo in correos_usados:
            nombre = f"{random.choice(NOMBRES)} {random.choice(APELLIDOS)}"
            correo = f"{nombre.split()[0].lower()}@alumno.iteso.mx"

        correos_usados.add(correo)

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
    carreras = generar_carreras()
    materias = generar_materias()
    profesores = generar_datos_maestros()
    alumnos = generar_datos_alumnos()

    
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
    db_m.comentarios.drop()
    db_m.comentarios.drop()

    # Insertar Carreras
    for c in carreras:
        res = im.insertar_carrera(db_m, c["nombre"], c["desc"], c["facultad"], [])
        c["mongo_id"] = res.inserted_id
    
    # Insertar Materias
    for m in materias:
        res = im.insertar_materia(db_m, str(m["codigo"]), m["nombre"], "Desc... Automatica", m["cat"], [])
        m["mongo_id"] = res.inserted_id

    # Asignar materias a las carreras en Mongo
    for c in carreras:
        # seleccionar un subconjunto aleatorio de materias
        num_mats = random.randint(2, len(materias))
        materias_sel = random.sample(materias, num_mats)
        ids_materias = [m["mongo_id"] for m in materias_sel]
        db_m.carreras.update_one(
            {"_id": c["mongo_id"]},
            {"$set": {"materias": ids_materias}}
        )

    # Asignación de requisitos para materias
    for i in range(len(materias)):
        materia_actual = materias[i]
        # Materias anteriores pueden ser requisitos
        posibles_reqs = materias[:i]
        # Si no hay anteriores, no puede tener requisito
        if not posibles_reqs:
            requisitos = []
        else:
            # Elegir entre 0 y 2 requisitos aleatorios
            num_reqs = random.randint(0, min(2, len(posibles_reqs)))
            seleccionadas = random.sample(posibles_reqs, num_reqs)
            requisitos = [m["mongo_id"] for m in seleccionadas]
        # Actualizar en Mongo
        db_m.materias.update_one(
            {"_id": materia_actual["mongo_id"]},
            {"$set": {"requisitos": requisitos}}
        )


    # Insertar Profesores
    for p in profesores:
        # Asumimos carrera 0 para profes por simplicidad
        res = im.insertar_usuario(db_m, p["nombre"], p["correo"], p["password"], "maestro", carreras[0]["mongo_id"], None, str(p["uuid"]) )
        p["mongo_id"] = res.inserted_id

    # Insertar Cursos
    for c in cursos:
        res = im.insertar_curso(db_m, c["codigo_str"], c["nombre"], c["periodo"], c["estado"], c["profesor_ref"]["mongo_id"], c["materia_ref"]["mongo_id"])
        c["mongo_id"] = res.inserted_id


    # Arreglos auxiliares para las entregas
    actividades_dg = []
    comentarios_dg = []
    rel_curso_actividad = []
    rel_alumno_actividad = []
    rel_actividad_comentario = []
    rel_comentario_alumno = []
    rel_carrera_alumno = []
    rel_profesor_alumno = []

    actividad_codigo = 1000
    comentario_codigo = 2000
    num_tarea = 1

    # Insertar Alumnos y Entregas
    for a in alumnos:

        titulo_tarea = f"Tarea {num_tarea}"
        # Asignar carrera random
        carrera = random.choice(carreras)
        a["carrera_ref"] = carrera

        # Crear progreso (Inscrito en un curso random)
        curso_inscrito = random.choice(cursos)
        progreso = [{"curso_id": curso_inscrito["mongo_id"], "codigo_curso": curso_inscrito["codigo_str"], "nombre_curso": curso_inscrito["nombre"], "estado": "En curso"}]
        
        res = im.insertar_usuario(db_m, a["nombre"], a["correo"], a["password"], "alumno", carrera["mongo_id"],a["expediente"], str(a["uuid"]),   progreso)
        a["mongo_id"] = res.inserted_id
        a["curso_inscrito"] = curso_inscrito # Guardar ref para Dgraph y Cassandra

        # Crear tarea y entrega en Mongo para este alumno
        res_t = im.insertar_tarea(db_m, curso_inscrito["mongo_id"], titulo_tarea, "Investigacion", datetime.datetime.now(), 100)
        res_ent = im.insertar_entrega(db_m, res_t.inserted_id, curso_inscrito["mongo_id"], a["mongo_id"], datetime.datetime.now(), 95, "link", "http://tarea.com")

        # Comentario en Mongo (profesor)
        prof = curso_inscrito["profesor_ref"]
        im.insertar_comentario(db_m, prof["mongo_id"], curso_inscrito["mongo_id"], res_t.inserted_id, "Revisa las instrucciones", datetime.datetime.now())

        # NODO Actividad en Dgraph
        actividad_codigo += 1
        actividades_dg.append({"codigo": actividad_codigo, "titulo": titulo_tarea, "descripcion": "Actividad generada automaticamente", "fecha_limite": datetime.datetime.now().isoformat()})

        # NODO Comentario en Dgraph (uno por alumno)
        comentario_codigo += 1
        comentarios_dg.append({"codigo": comentario_codigo,"cuerpo": "Comentario del alumno en la actividad", "fecha": datetime.datetime.now().isoformat()})

        # Relaciones Dgraph para esta actividad y comentario
        rel_curso_actividad.append({"curso_codigo": curso_inscrito["codigo_int"], "actividad_codigo": actividad_codigo})
        rel_alumno_actividad.append({"alumno_expediente": a["expediente"], "actividad_codigo": actividad_codigo})
        rel_actividad_comentario.append({"actividad_codigo": actividad_codigo, "comentario_codigo": comentario_codigo})
        rel_comentario_alumno.append({"comentario_codigo": comentario_codigo, "alumno_expediente": a["expediente"]})

        # Relaciones carrera–alumno y profesor–alumno
        rel_carrera_alumno.append({"carrera_codigo": carrera["codigo"], "alumno_expediente": a["expediente"]})
        rel_profesor_alumno.append({"profesor_correo": prof["correo"], "alumno_expediente": a["expediente"]})

        num_tarea += 1

    
        # para saber quien hiso cada comentario sobre que curso
    comentarios_ids = []

    for com in db_m.comentarios.find({}):
        comentarios_ids.append({
            "usuario_id": str(com["usuario_id"]),
            "curso_id": str(com["curso_id"]),
            "tarea_id": str(com["tarea_id"])
        })

    mongo_cerrar(client_m)
    print("MongoDB Terminado.")

    # CASSANDRA
    print("\n3. Poblando Cassandra...")
    cluster_c, session_c = cassandra_session()

    #creamos la estructura de la base de datos
    mc.create_keyspace(session_c, 'proyecto_bdnr', 1)
    session_c.set_keyspace('proyecto_bdnr')
    mc.create_schema(session_c)

    #borramos cualquier dato que pudieramos haber tenido antes
    rows = session_c.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='proyecto_bdnr'")

    for row in rows:
        t = row.table_name
        session_c.execute(f"TRUNCATE {t}")
        print("Truncada:", t)


    # DATOS DE ALUMNOS
    print("   -> Generando datos de alumnos (Logs, Inscripciones, Asistencias, Notificaciones)...")
    for a in alumnos:
        # Log de logins
        mc.insert_log_usuario(session_c, str(a["uuid"]), "Inicio de sesion")
        
        # Inscripcion bitácora
        mc.insert_inscripcion(session_c, str(a["curso_inscrito"]["uuid"]), str(a["uuid"]), a["nombre"])
        
        #Historial Académico
        mc.insert_historial_academico(
            session_c, 
            str(a["uuid"]), 
            str(a["curso_inscrito"]["uuid"]), 
            a["curso_inscrito"]["nombre"], 
            "Inscrito en curso"
        )
        
        #Asistencia
        mc.insert_asistencia(session_c, str(a["uuid"]), str(a["curso_inscrito"]["uuid"]), str(uuid.uuid4()), "Presente")
        
        # Entrega registrada
        mc.insert_entrega(session_c, str(a["uuid"]), str(uuid.uuid4()), str(a["curso_inscrito"]["uuid"]), "Entrega registrada desde poblamiento")


        #Mensaje simulado
        prof = a["curso_inscrito"]["profesor_ref"]
        mc.enviar_mensaje(session_c, str(a["uuid"]), a["nombre"], str(prof["uuid"]), "Hola profe, duda con la tarea.")

        #Notificaciones
        mc.insert_notificacion(session_c, str(a["uuid"]), "Sistema", "Bienvenido a la plataforma")
        mc.insert_notificacion(session_c, str(a["uuid"]), "Tarea", "Recordatorio: Tarea vence pronto")

        # Asesorías agendadas
        mc.agendar_asesoria(
            session_c, 
            str(prof["uuid"]), 
            prof["nombre"], 
            str(a["uuid"]), 
            a["nombre"], 
            "Revision de Proyecto"
        )

    print("Generando datos de profesores...")
    for p in profesores:
        #Movimiento profesor
        cursos_del_profe = [c for c in cursos if c["profesor_ref"] == p]
        
        if cursos_del_profe:
            curso_actual = cursos_del_profe[0]
            
            # Movimiento
            mc.insert_movimiento_profesor(session_c, str(p["uuid"]), str(curso_actual["uuid"]), "Calificar", "Tarea calificada")
            
            #Sesión de clase
            mc.insert_sesion_profesor(
                session_c, 
                str(p["uuid"]), 
                str(curso_actual["uuid"]), 
                25,
                "Lunes 10:00 - 12:00"
            )

    # Estados de cursos
    for c in cursos:
        #Historial de estados del curso
        mc.insert_estado_curso(session_c, str(c["uuid"]), "Creado")
        mc.insert_estado_curso(session_c, str(c["uuid"]), "Inscripciones Abiertas")
        mc.insert_estado_curso(session_c, str(c["uuid"]), "En Curso")

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
    
    csv_actividades = actividades_dg
    csv_comentarios = comentarios_dg


    # Relaciones
    # Alumno inscrito en Curso
    csv_alumno_curso = [{'alumno_expediente': a['expediente'], 'curso_codigo': a['curso_inscrito']['codigo_int']} for a in alumnos]
    # Profesor imparte Curso
    csv_profesor_curso = [{'profesor_correo': c['profesor_ref']['correo'], 'curso_codigo': c['codigo_int']} for c in cursos]
    # Carrera tiene materias 
    csv_carrera_materia = []
    for c in carreras:
        for m in materias:
            csv_carrera_materia.append({'carrera_codigo': c['codigo'], 'materia_codigo': m['codigo']})
    # Materia tiene cursos
    csv_materia_curso = [{'materia_codigo': c['materia_ref']['codigo'], 'curso_codigo': c['codigo_int']} for c in cursos]

    csv_materia_prerequisito = []
    for i in range(1, len(materias)):
        csv_materia_prerequisito.append({'materia1_codigo': materias[i]['codigo'], 'materia2_codigo': materias[i-1]['codigo']})

    # Las relaciones hechas en Mongo
    csv_carrera_alumno = rel_carrera_alumno
    csv_profesor_alumno = rel_profesor_alumno
    csv_curso_actividad = rel_curso_actividad
    csv_alumno_actividad = rel_alumno_actividad
    csv_actividad_comentario = rel_actividad_comentario
    csv_comentario_alumno = rel_comentario_alumno


    # Escribir Archivos
    escribir_csv(os.path.join(NODES_DIR, 'materias.csv'), ['codigo', 'nombre', 'departamento'], csv_materias)
    escribir_csv(os.path.join(NODES_DIR, 'carreras.csv'), ['codigo', 'nombre', 'descripcion'], csv_carreras)
    escribir_csv(os.path.join(NODES_DIR, 'profesores.csv'), ['nombre', 'correo'], csv_profesores)
    escribir_csv(os.path.join(NODES_DIR, 'cursos.csv'), ['codigo', 'nombre', 'descripcion', 'creditos'], csv_cursos)
    escribir_csv(os.path.join(NODES_DIR, 'alumnos.csv'), ['expediente', 'nombre', 'correo'], csv_alumnos)
    escribir_csv(os.path.join(NODES_DIR, 'actividades.csv'), ['codigo', 'titulo', 'descripcion', 'fecha_limite'], csv_actividades)
    escribir_csv(os.path.join(NODES_DIR, 'comentarios.csv'), ['codigo', 'cuerpo', 'fecha'], csv_comentarios)

    # Escribir las relaciones
    escribir_csv(os.path.join(REL_DIR, 'alumno_curso.csv'), ['alumno_expediente', 'curso_codigo'], csv_alumno_curso)
    escribir_csv(os.path.join(REL_DIR, 'profesor_curso.csv'), ['profesor_correo', 'curso_codigo'], csv_profesor_curso)
    escribir_csv(os.path.join(REL_DIR, 'carrera_materia.csv'), ['carrera_codigo', 'materia_codigo'], csv_carrera_materia)
    escribir_csv(os.path.join(REL_DIR, 'materia_curso.csv'), ['materia_codigo', 'curso_codigo'], csv_materia_curso)
    
    escribir_csv(os.path.join(REL_DIR, 'materia_prerequisito.csv'), ['materia1_codigo', 'materia2_codigo'], csv_materia_prerequisito)
    escribir_csv(os.path.join(REL_DIR, 'carrera_alumno.csv'), ['carrera_codigo', 'alumno_expediente'], csv_carrera_alumno)
    escribir_csv(os.path.join(REL_DIR, 'profesor_alumno.csv'), ['profesor_correo', 'alumno_expediente'], csv_profesor_alumno)
    escribir_csv(os.path.join(REL_DIR, 'curso_actividad.csv'), ['curso_codigo', 'actividad_codigo'], csv_curso_actividad)
    escribir_csv(os.path.join(REL_DIR, 'alumno_actividad.csv'), ['alumno_expediente', 'actividad_codigo'], csv_alumno_actividad)
    escribir_csv(os.path.join(REL_DIR, 'actividad_comentario.csv'), ['actividad_codigo', 'comentario_codigo'], csv_actividad_comentario)
    escribir_csv(os.path.join(REL_DIR, 'comentario_alumno.csv'), ['comentario_codigo', 'alumno_expediente'], csv_comentario_alumno)

    # Cargar en Dgraph
    client_d, stub_d = dgraph_conexion()

    print("Limpiando Dgraph")
    # drop_all nativo de Dgraph
    op = pydgraph.Operation(drop_all=True)
    client_d.alter(op)

    try:
        modelD.set_schema(client_d)
        
        # Llamar manualmente a las funciones de carga de modelD para asegurar orden
        # Nodos
        m_uids = modelD.load_materias(client_d, os.path.join(NODES_DIR, 'materias.csv'))
        car_uids = modelD.load_carreras(client_d, os.path.join(NODES_DIR, 'carreras.csv'))
        p_uids = modelD.load_profesores(client_d, os.path.join(NODES_DIR, 'profesores.csv'))
        cur_uids = modelD.load_cursos(client_d, os.path.join(NODES_DIR, 'cursos.csv'))
        a_uids = modelD.load_alumnos(client_d, os.path.join(NODES_DIR, 'alumnos.csv'))
        act_uids = modelD.load_actividades(client_d, os.path.join(NODES_DIR, 'actividades.csv'))
        com_uids = modelD.load_comentarios(client_d, os.path.join(NODES_DIR, 'comentarios.csv'))

        # Relaciones
        modelD.create_alumno_inscrito_en_edge(client_d, os.path.join(REL_DIR, 'alumno_curso.csv'), a_uids, cur_uids)
        modelD.create_profesor_profesor_curso_edge(client_d, os.path.join(REL_DIR, 'profesor_curso.csv'), p_uids, cur_uids)
        modelD.create_carrera_tiene_materias_edge(client_d, os.path.join(REL_DIR, 'carrera_materia.csv'), car_uids, m_uids)
        modelD.create_materia_tiene_cursos_edge(client_d, os.path.join(REL_DIR, 'materia_curso.csv'), m_uids, cur_uids)
        modelD.create_materia_tiene_prerequisito_edge(client_d, os.path.join(REL_DIR, 'materia_prerequisito.csv'), m_uids)
        modelD.create_carrera_contiene_alumnos_edge(client_d, os.path.join(REL_DIR, 'carrera_alumno.csv'), car_uids, a_uids)
        modelD.create_profesor_tiene_alumnos_edge(client_d, os.path.join(REL_DIR, 'profesor_alumno.csv'), p_uids, a_uids)
        modelD.create_curso_tiene_actividades_edge(client_d, os.path.join(REL_DIR, 'curso_actividad.csv'), cur_uids, act_uids)
        modelD.create_alumno_tiene_asignado_edge(client_d, os.path.join(REL_DIR, 'alumno_actividad.csv'), a_uids, act_uids)
        modelD.create_actividad_tiene_comentarios_edge(client_d, os.path.join(REL_DIR, 'actividad_comentario.csv'), act_uids, com_uids)
        modelD.create_comentario_escrito_por_edge(client_d, os.path.join(REL_DIR, 'comentario_alumno.csv'), com_uids, a_uids)
        
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
    print("\n[CURSOS]")
    for c in cursos:
        print(f"Nombre: {c['nombre']} | Código: {c['codigo_str']} | UUID (Cassandra): {c['uuid']} | Mongo ID: {c['mongo_id']}")
    print("\n[MATERIAS]")
    for m in materias:
        print(f"Nombre: {m['nombre']} | Codigo: {m['codigo']} | Departamento: {m['depto']} | Categoria: {m['cat']} | MongoID: {m['mongo_id']}")
    print("\n[CARRERAS]")
    for c in carreras:
        print(f"Nombre: {c['nombre']} | Codigo: {c['codigo']} | Facultad: {c['facultad']} | MongoID: {c['mongo_id']}")
    print("\n[ACTIVIDADES]")
    for act in actividades_dg:
        print(f"Codigo: {act['codigo']} | Titulo: {act['titulo']} | Fecha Limite: {act['fecha_limite']}")
    print("\n[COMENTARIOS]")
    for com in comentarios_dg:
        print(f"Codigo: {com['codigo']} | Fecha: {com['fecha']} | Cuerpo: {com['cuerpo']}")
    print("\n[COMENTARIOS - IDs]")
    for c in comentarios_ids:
        print(f"usuario_id: {c['usuario_id']} | curso_id: {c['curso_id']} | tarea_id: {c['tarea_id']}")





    #Creamos archivo txt para tener acceso a la mano de los ids para pruebas
    print("\nGenerando archivo de ids de prueba 'ids_pruebas.txt'...")
    with open("ids_pruebas.txt", "w", encoding="utf-8") as f:
        f.write("=== Archivo DATOS PARA PRUEBA ===\n")
        
        f.write("[ALUMNOS]\n")
        for a in alumnos:
            f.write(f"Nombre: {a['nombre']} | Email: {a['correo']} | "
                    f"UUID (Cassandra): {a['uuid']} | "
                    f"Mongo ID: {a['mongo_id']} | "
                    f"Expediente (Dgraph): {a['expediente']}\n")
        
        f.write("\n[PROFESORES]\n")
        for p in profesores:
            f.write(f"Nombre: {p['nombre']} | Email: {p['correo']} | "
                    f"UUID (Cassandra): {p['uuid']} | "
                    f"Mongo ID: {p['mongo_id']}\n")

        f.write("\n[CURSOS]\n")
        for c in cursos:
            f.write(f"Nombre: {c['nombre']} | Código: {c['codigo_str']} | "
                    f"UUID (Cassandra): {c['uuid']} | "
                    f"Mongo ID: {c['mongo_id']}\n")
            
        f.write("[MATERIAS]\n")
        for m in materias:
            f.write(f"Nombre: {m['nombre']} | Codigo: {m['codigo']} | Departamento: {m['depto']} | Categoria: {m['cat']} | MongoID: {m['mongo_id']}\n")

        f.write("\n[CARRERAS]\n")
        for c in carreras:
            f.write(f"Nombre: {c['nombre']} | Codigo: {c['codigo']} | Facultad: {c['facultad']} | MongoID: {c['mongo_id']}\n")

        f.write("\n[ACTIVIDADES]\n")
        for act in actividades_dg:
            f.write(f"Codigo: {act['codigo']} | Titulo: {act['titulo']} | Fecha Limite: {act['fecha_limite']}\n")

        f.write("\n[COMENTARIOS]\n")
        for com in comentarios_dg:
            f.write(f"Codigo: {com['codigo']} | Fecha: {com['fecha']} | Cuerpo: {com['cuerpo']}\n")

        f.write("\n[COMENTARIOS - IDs]\n")
        for c in comentarios_ids:
            f.write(
                f"usuario_id: {c['usuario_id']} | curso_id: {c['curso_id']} | tarea_id: {c['tarea_id']}\n"
            )


    print("Archivo 'ids_pruebas.txt' generado exitosamente")

if __name__ == "__main__":
    main()
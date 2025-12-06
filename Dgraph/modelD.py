import csv
import pydgraph
import os #para las carpetas de los archivos 
import json # facilitar algunas cosas


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', 'data', 'data_dgraph')#ruta de data general
#las subcarpetas
NODES_DIR = os.path.join(DATA_DIR, 'nodes')
REL_DIR = os.path.join(DATA_DIR, 'relations')



def set_schema(client):
    schema = """
        type Materia {
            nombre
            codigo
            departamento
            tiene_prerequisito
            tiene_cursos
        }
        type Carrera {
            nombre
            codigo
            descripcion
            tiene_materias
            contiene_alumnos
        }
        type Profesor {
            nombre
            correo
            profesor_curso
            tiene_alumnos
        }
        type Curso {
            nombre
            descripcion
            creditos
            codigo
            tiene_actividades
        }
        type Alumno {
            nombre
            correo
            expediente
            inscrito_en
            tiene_asignado
        }
        type Actividad {
            titulo
            codigo
            descripcion
            fecha_limite
            tiene_comentarios
        }
        type Comentarios {
            cuerpo
            codigo
            fecha
            escrito_por
        }
        nombre: string @index(term) .
        codigo: int @index(int) . 
        departamento: string @index(exact) .
        descripcion: string @index(fulltext) .
        correo: string . 
        creditos: int @index(int) .
        expediente: int @index(int) .
        titulo: string @index(term) .
        cuerpo: string @index(fulltext) .
        fecha: datetime @index(day) .
        fecha_limite: datetime @index(day) .
        tiene_prerequisito: [uid] .
        tiene_cursos: [uid] .
        tiene_materias: [uid] .
        contiene_alumnos: [uid] .
        profesor_curso: [uid] @reverse .
        tiene_alumnos: [uid] .
        tiene_actividades: [uid] .
        inscrito_en: [uid] @reverse .
        tiene_asignado: [uid] .
        tiene_comentarios: [uid] .
        escrito_por: [uid] .
    """
    return client.alter(pydgraph.Operation(schema=schema))

#Carga de los datos de los nodos
def load_materias(client, file_path):
    txt = client.txn()
    resp = None
    try:
        materias = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                materias.append({
                    'uid': '_:' + row['codigo'],#usamos codigo para que sirva como identificador unico
                    'dgraph.type': 'Materia',
                    'nombre': row['nombre'],
                    'codigo': int(row['codigo']),
                    'departamento': row['departamento']
                })
            print(f'Cargando materias: {materias} ')
            resp = txt.mutate(set_obj=materias)
        txt.commit()
    finally:    
        txt.discard()
    return resp.uids

def load_carreras(client, file_path):
    txt = client.txn()
    resp = None
    try:
        carreras = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                carreras.append({
                    'uid': '_:' + row['codigo'], #usamos codigo para que sirva como identificador unico
                    'dgraph.type': 'Carrera',
                    'nombre': row['nombre'],
                    'codigo': int(row['codigo']),
                    'descripcion': row['descripcion']
                })
            print(f'Cargando carreras: {carreras}')
            resp = txt.mutate(set_obj=carreras)
        txt.commit()
    finally:    
        txt.discard()
    return resp.uids

def load_profesores(client, file_path):
    txt = client.txn()
    resp = None
    try:
        profesores = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                profesores.append({
                    'uid': '_:' + row['correo'],#usamos correo para que sirva como identificador unico
                    'dgraph.type': 'Profesor',
                    'nombre': row['nombre'],
                    'correo': row['correo']
                })
            print(f'Cargando profesores: {profesores}')
            resp = txt.mutate(set_obj=profesores)
        txt.commit()
    finally:    
        txt.discard()
    return resp.uids

def load_cursos(client, file_path):
    txt = client.txn()
    resp = None
    try:
        cursos = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                cursos.append({
                    'uid': '_:' + row['codigo'], #usamos codigo para que sirva como identificador unico
                    'dgraph.type': 'Curso',
                    'nombre': row['nombre'],
                    'descripcion': row['descripcion'],
                    'creditos': int(row['creditos']),
                    'codigo': int(row['codigo'])
                })
            print(f'Cargando cursos: {cursos}')
            resp = txt.mutate(set_obj=cursos)
        txt.commit()
    finally:    
        txt.discard()
    return resp.uids

def load_alumnos(client, file_path):
    txt = client.txn()
    resp = None
    try:
        alumnos = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                alumnos.append({
                    'uid': '_:' + row['expediente'], #usamos expediente para que sirva como identificador unico
                    'dgraph.type': 'Alumno',
                    'nombre': row['nombre'],
                    'correo': row['correo'],
                    'expediente': int(row['expediente'])
                })
            print(f'Cargando alumnos: {alumnos}' )
            resp = txt.mutate(set_obj=alumnos)
        txt.commit()
    finally:    
        txt.discard()
    return resp.uids

def load_actividades(client, file_path):
    txt = client.txn()
    resp = None
    try:
        actividades = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                actividades.append({
                    'uid': '_:' + row['codigo'], #usamos codigo
                    'dgraph.type': 'Actividad',
                    'titulo': row['titulo'],
                    'codigo': int(row['codigo']),
                    'descripcion': row['descripcion'],
                    'fecha_limite': row['fecha_limite']
                })
            print(f'Cargando actividades: {actividades}')
            resp = txt.mutate(set_obj=actividades)
        txt.commit()
    finally:    
        txt.discard()
    return resp.uids

def load_comentarios(client, file_path):
    txt = client.txn()
    resp = None
    try:
        comentarios = []
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            i = 0
            for row in reader:
                comentarios.append({                                 #mismo caso que actividades
                    'uid': '_:' + row['codigo'], # para que sirva como identificador unico
                    'dgraph.type': 'Comentarios',
                    'cuerpo': row['cuerpo'],
                    'codigo': int(row['codigo']),
                    'fecha': row['fecha']
                })
                i += 1
            print(f'Cargando comentarios: {comentarios}')
            resp = txt.mutate(set_obj=comentarios)
        txt.commit()
    finally:    
        txt.discard()
    return resp.uids

#Creación de las relaciones entre los nodos
def create_materia_tiene_cursos_edge(client, file_path, materias_uids, cursos_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                materia = row['materia_codigo']
                curso = row['curso_codigo']
                mutation = {
                    'uid':materias_uids[materia],
                    'tiene_cursos': {
                        'uid': cursos_uids[curso]
                    }
                }
                print(f'Generating relationships {materia} tiene {curso}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def create_materia_tiene_prerequisito_edge(client, file_path, materias_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                materia1 = row['materia1_codigo']
                materia2 = row['materia2_codigo']
                mutation = {
                    'uid': materias_uids[materia1],
                    'tiene_prerequisito': {
                        'uid':materias_uids[materia2]
                    }
                }
                print(f'Generating relationships {materia1} tiene prerequisito a {materia2}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def create_carrera_tiene_materias_edge(client, file_path, carrera_uids, materias_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                carrera = row['carrera_codigo']
                materia = row['materia_codigo']
                mutation = {
                    'uid': carrera_uids[carrera],
                    'tiene_materias': {
                        'uid': materias_uids[materia]
                    }
                }
                print(f'Generating relationships {carrera} tiene {materia}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def create_carrera_contiene_alumnos_edge(client, file_path, carrera_uids, alumnos_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                carrera = row['carrera_codigo']
                alumno = row['alumno_expediente']
                mutation = {
                    'uid': carrera_uids[carrera],
                    'contiene_alumnos': {
                        'uid': alumnos_uids[alumno]
                    }
                }
                print(f'Generating relationships {carrera} contiene {alumno}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def create_profesor_profesor_curso_edge(client, file_path, profesor_uids, cursos_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                profesor = row['profesor_correo']
                curso = row['curso_codigo']
                mutation = {
                    'uid': profesor_uids[profesor],
                    'profesor_curso': {
                        'uid': cursos_uids[curso]
                    }
                }
                print(f'Generating relationships {profesor} profesor_curso {curso}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def create_profesor_tiene_alumnos_edge(client, file_path, profesor_uids, alumnos_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                profesor = row['profesor_correo']
                alumno = row['alumno_expediente']
                mutation = {
                    'uid': profesor_uids[profesor],
                    'tiene_alumnos': {
                        'uid': alumnos_uids[alumno]
                    }
                }
                print(f'Generating relationships {profesor} tiene {alumno}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def create_curso_tiene_actividades_edge(client, file_path, cursos_uids, actividades_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                curso = row['curso_codigo']
                actividad = row['actividad_codigo']
                mutation = {
                    'uid': cursos_uids[curso],
                    'tiene_actividades': {
                        'uid': actividades_uids[actividad]
                    }
                }
                print(f'Generating relationships {curso} tiene {actividad}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def create_alumno_inscrito_en_edge(client, file_path, alumnos_uids, cursos_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                alumno = row['alumno_expediente']
                curso = row['curso_codigo']
                mutation = {
                    'uid': alumnos_uids[alumno],
                    'inscrito_en': {
                        'uid': cursos_uids[curso]
                    }
                }
                print(f'Generating relationships {alumno} inscrito en {curso}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def create_alumno_tiene_asignado_edge(client, file_path, alumnos_uids, actividades_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                alumno = row['alumno_expediente']
                actividad = row['actividad_codigo']
                mutation = {
                    'uid': alumnos_uids[alumno],
                    'tiene_asignado': {
                        'uid':actividades_uids[actividad]
                    }
                }
                print(f'Generating relationships {alumno} tiene asignada {actividad}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def create_actividad_tiene_comentarios_edge(client, file_path, actividades_uids, comentarios_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                actividad = row['actividad_codigo']
                comentario = row['comentario_codigo']
                mutation = {
                    'uid': actividades_uids[actividad],
                    'tiene_comentarios': {
                        'uid': comentarios_uids[comentario]
                    }
                }
                print(f'Generating relationships {actividad} tiene {comentario}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def create_comentario_escrito_por_edge(client, file_path, comentarios_uids, alumnos_uids):
    txt = client.txn()  
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                comentario = row['comentario_codigo']
                alumno = row['alumno_expediente']
                mutation = {
                    'uid': comentarios_uids[comentario],
                    'escrito_por': {
                        'uid': alumnos_uids[alumno]
                    }
                }
                print(f'Generating relationships {comentario} escrito por {alumno}')
                txt.mutate(set_obj=mutation)
        txt.commit()
    finally:
        txt.discard()

def load_data(client):
    #nodos
    materias_uids = load_materias(client, os.path.join(NODES_DIR, 'materias.csv'))
    carreras_uids = load_carreras(client, os.path.join(NODES_DIR, 'carreras.csv'))
    profesores_uids = load_profesores(client, os.path.join(NODES_DIR, 'profesores.csv'))
    cursos_uids = load_cursos(client, os.path.join(NODES_DIR, 'cursos.csv'))
    alumnos_uids = load_alumnos(client, os.path.join(NODES_DIR, 'alumnos.csv'))
    actividades_uids = load_actividades(client, os.path.join(NODES_DIR, 'actividades.csv'))
    comentarios_uids = load_comentarios(client, os.path.join(NODES_DIR, 'comentarios.csv'))
    #relaciones:
    create_materia_tiene_cursos_edge(client, os.path.join(REL_DIR, 'materia_tiene_cursos.csv'), materias_uids, cursos_uids)
    create_materia_tiene_prerequisito_edge(client, os.path.join(REL_DIR, 'materia_tiene_prerequisito.csv'), materias_uids)
    create_carrera_tiene_materias_edge(client, os.path.join(REL_DIR, 'carrera_tiene_materias.csv'), carreras_uids, materias_uids)
    create_carrera_contiene_alumnos_edge(client, os.path.join(REL_DIR, 'carrera_contiene_alumnos.csv'), carreras_uids, alumnos_uids)
    create_profesor_profesor_curso_edge(client, os.path.join(REL_DIR, 'profesor_profesor_curso.csv'), profesores_uids, cursos_uids)
    create_profesor_tiene_alumnos_edge(client, os.path.join(REL_DIR, 'profesor_tiene_alumnos.csv'), profesores_uids, alumnos_uids)
    create_curso_tiene_actividades_edge(client, os.path.join(REL_DIR, 'curso_tiene_actividades.csv'), cursos_uids, actividades_uids)
    create_alumno_inscrito_en_edge(client, os.path.join(REL_DIR, 'alumno_inscrito_en.csv'), alumnos_uids, cursos_uids)
    create_alumno_tiene_asignado_edge(client, os.path.join(REL_DIR, 'alumno_tiene_asignado.csv'), alumnos_uids, actividades_uids)
    create_actividad_tiene_comentarios_edge(client, os.path.join(REL_DIR, 'actividad_tiene_comentarios.csv'), actividades_uids, comentarios_uids)
    create_comentario_escrito_por_edge(client, os.path.join(REL_DIR, 'comentario_escrito_por.csv'), comentarios_uids, alumnos_uids)



# INSERTS INDIVIDUALES

def insertar_alumno_dg(client, nombre, correo, expediente):
    txn = client.txn()
    try:
        alumno = {
            "dgraph.type": "Alumno",
            "nombre": nombre,
            "correo": correo,
            "expediente": int(expediente)
        }
        txn.mutate(set_obj=alumno)
        txn.commit()
        print("Alumno creado en Dgraph")
    finally:
        txn.discard()


def insertar_profesor_dg(client, nombre, correo):
    txn = client.txn()
    try:
        profesor = {
            "dgraph.type": "Profesor",
            "nombre": nombre,
            "correo": correo
        }
        txn.mutate(set_obj=profesor)
        txn.commit()
        print("Profesor creado en Dgraph")
    finally:
        txn.discard()


def insertar_carrera_dg(client, nombre, descripcion, codigo=None):
    txn = client.txn()
    try:
        carrera = {
            "dgraph.type": "Carrera",
            "nombre": nombre,
            "descripcion": descripcion
        }
        if codigo is not None:
            carrera["codigo"] = int(codigo)
        txn.mutate(set_obj=carrera)
        txn.commit()
        print("Carrera creada en Dgraph")
    finally:
        txn.discard()


def insertar_materia_dg(client, codigo, nombre, categoria):
    txn = client.txn()
    try:
        materia = {
            "dgraph.type": "Materia",
            "nombre": nombre,
            "codigo": int(codigo),
            "departamento": categoria
        }
        txn.mutate(set_obj=materia)
        txn.commit()
        print("Materia creada en Dgraph")
    finally:
        txn.discard()


def insertar_curso_dg(client, codigo, nombre, descripcion, creditos):
    txn = client.txn()
    try:
        curso = {
            "dgraph.type": "Curso",
            "nombre": nombre,
            "descripcion": descripcion,
            "codigo": int(codigo),
            "creditos": int(creditos)
        }
        txn.mutate(set_obj=curso)
        txn.commit()
        print("Curso creado en Dgraph")
    finally:
        txn.discard()


def insertar_actividad_dg(client, codigo, titulo, descripcion, fecha_limite):
    txn = client.txn()
    try:
        actividad = {
            "dgraph.type": "Actividad",
            "codigo": int(codigo),
            "titulo": titulo,
            "descripcion": descripcion,
            "fecha_limite": fecha_limite
        }
        txn.mutate(set_obj=actividad)
        txn.commit()
        print("Actividad creada en Dgraph")
    finally:
        txn.discard()
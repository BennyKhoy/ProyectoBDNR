import csv
import pydgraph

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
            descripcion
            fecha_limite
            tiene_comentarios
        }
        type Comentarios {
            cuerpo
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
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                materias.append({
                    'uid': '_:materia_' + row['codigo'],#usamos codigo para que sirva como identificador unico
                    'dgraph.type': 'Materia',
                    'nombre': row['nombre'],
                    'codigo': int(row['codigo']),
                    'departamento': row['departamento']
                })
            print(f"Cargando materias: {materias} ")
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
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                carreras.append({
                    'uid': '_:carrera_' + row['codigo'], #usamos codigo para que sirva como identificador unico
                    'dgraph.type': 'Carrera',
                    'nombre': row['nombre'],
                    'codigo': int(row['codigo']),
                    'descripcion': row['descripcion']
                })
            print(f"Cargando carreras: {carreras}")
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
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                profesores.append({
                    'uid': '_:profesor_' + row['correo'],#usamos correo para que sirva como identificador unico
                    'dgraph.type': 'Profesor',
                    'nombre': row['nombre'],
                    'correo': row['correo']
                })
            print(f"Cargando profesores: {profesores}")
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
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                cursos.append({
                    'uid': '_:curso_' + row['codigo'], #usamos codigo para que sirva como identificador unico
                    'dgraph.type': 'Curso',
                    'nombre': row['nombre'],
                    'descripcion': row['descripcion'],
                    'creditos': int(row['creditos']),
                    'codigo': int(row['codigo'])
                })
            print(f"Cargando cursos: {cursos}")
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
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                alumnos.append({
                    'uid': '_:alumno_' + row['expediente'], #usamos expediente para que sirva como identificador unico
                    'dgraph.type': 'Alumno',
                    'nombre': row['nombre'],
                    'correo': row['correo'],
                    'expediente': int(row['expediente'])
                })
            print(f"Cargando alumnos: {alumnos}" )
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
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            i = 0
            for row in reader:
                actividades.append({
                    'uid': f'_:actividad_{i}', #no usamos ninguno de sus campos por que son posiblemente repetitivos, entonces usamos un contador
                    'dgraph.type': 'Actividad',
                    'titulo': row['titulo'],
                    'descripcion': row['descripcion'],
                    'fecha_limite': row['fecha_limite']
                })
                i += 1
            print(f"Cargando actividades: {actividades}")
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
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            i = 0
            for row in reader:
                comentarios.append({                                 #mismo caso que actividades
                    'uid': '_:comentario_' + row['fecha'] + f'_{i}', #usamos fecha y un contador para que sirva como identificador unico
                    'dgraph.type': 'Comentarios',
                    'cuerpo': row['cuerpo'],
                    'fecha': row['fecha']
                })
                i += 1
            print(f"Cargando comentarios: {comentarios}")
            resp = txt.mutate(set_obj=comentarios)
        txt.commit()
    finally:    
        txt.discard()
    return resp.uids

#Creación de las relaciones entre los nodos
def create_materia_tiene_cursos_edge(client):
    pass

def create_materia_tiene_prerequisito_edge(client):
    pass

def create_carrera_tiene_materias_edge(client):
    pass

def create_carrera_tiene_alumnos_edge(client):
    pass

def create_profesor_profesor_curso_edge(client):
    pass

def create_profesor_tiene_alumnos_edge(client):
    pass

def create_curso_tiene_actividades_edge(client):
    pass

def create_alumno_inscrito_en_edge(client):
    pass

def create_alumno_tiene_asignado_edge(client):
    pass

def create_actividad_tiene_comentarios_edge(client):
    pass

def create_comentario_escrito_por_edge(client):
    pass



    
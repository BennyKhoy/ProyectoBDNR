# IMPORTACIONES
from bson.objectid import ObjectId


# CREACION DE LOS INDICES

def crear_indices_mongo(db):

    # Usuarios: evitar correos duplicados
    db.usuarios.create_index([("correo", 1)], unique=True, name="idx_usuarios_correo_unique")

    # Entregas: consultas por curso y alumno
    db.entregas.create_index([("curso_id", 1), ("alumno_id", 1)], name="idx_entregas_curso_alumno")

    # Comentarios: consultas por usuario y curso
    db.comentarios.create_index([("usuario_id", 1), ("curso_id", 1)], name="idx_comentarios_usuario_curso")


# PIPELINES
# 1. Promedio de calificaciones por curso para un alumno
def pipeline_promedio_cursos_por_alumno(alumno_id):
    alumno_oid = ObjectId(alumno_id)

    pipeline = [
        {
            "$match": {
                "alumno_id": alumno_oid,
                "calificacion": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": "$curso_id",
                "promedio": {"$avg": "$calificacion"}
            }
        },
        {
            "$lookup": {
                "from": "cursos",
                "localField": "_id",
                "foreignField": "_id",
                "as": "curso"
            }
        },
        {"$unwind": "$curso"},
        {
            "$project": {
                "_id": 0,
                "curso_id": "$_id",
                "nombre_curso": "$curso.nombre",
                "promedio": 1,
                "numero_entregas": 1
            }
        }
    ]
    return pipeline


# 2. Entregas por alumno en un curso y promedio
def pipeline_entregas_por_alumno_curso(alumno_id, curso_id):
    alumno_oid = ObjectId(alumno_id)
    curso_oid = ObjectId(curso_id)

    pipeline = [
        {
            "$match": {
                "alumno_id": alumno_oid,
                "curso_id": curso_oid,
                "calificacion": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": {
                    "alumno_id": "$alumno_id",
                    "curso_id": "$curso_id"
                },
                "promedio": {"$avg": "$calificacion"},
                "entregas": {"$push": "$$ROOT"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "alumno_id": "$_id.alumno_id",
                "curso_id": "$_id.curso_id",
                "promedio": 1,
                "entregas": 1
            }
        }
    ]
    return pipeline

# 3. Promedio de calificaciones por curso impartido por profesor
def pipeline_promedio_cursos_profesor(profesor_id):
    profesor_oid = ObjectId(profesor_id)

    pipeline = [
        {"$match": {"calificacion": {"$ne": None}}},
        {
            "$lookup": {
                "from": "cursos",
                "localField": "curso_id",
                "foreignField": "_id",
                "as": "curso"
            }
        },
        {"$unwind": "$curso"},
        {"$match": {"curso.id_profesor": profesor_oid}},
        {
            "$group": {
                "_id": "$curso_id",
                "nombre_curso": {"$first": "$curso.nombre"},
                "promedio_curso": {"$avg": "$calificacion"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "curso_id": "$_id",
                "nombre_curso": 1,
                "promedio_curso": 1
            }
        }
    ]
    return pipeline



# 4. Promedio general por materia
def pipeline_promedio_general_por_materia():
    pipeline = [
        {"$match": {"calificacion": {"$ne": None}}},
        {
            "$group": {
                "_id": "$curso_id",
                "promedio_curso": {"$avg": "$calificacion"}
            }
        },
        {
            "$lookup": {
                "from": "cursos",
                "localField": "_id",
                "foreignField": "_id",
                "as": "curso"
            }
        },
        {"$unwind": "$curso"},
        {
            "$lookup": {
                "from": "materias",
                "localField": "curso.id_materia",
                "foreignField": "_id",
                "as": "materia"
            }
        },
        {"$unwind": "$materia"},
        {
            "$group": {
                "_id": "$materia._id",
                "nombre_materia": {"$first": "$materia.nombre"},
                "promedio_materia": {"$avg": "$promedio_curso"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "materia_id": "$_id",
                "nombre_materia": 1,
                "promedio_materia": 1
            }
        }
    ]

    return pipeline

def obtener_progreso_carrera(db, usuario_id):
    coleccion = db.usuarios
    doc = coleccion.find_one(
        {"_id": usuario_id},
        {"nombre": 1, "progreso_carrera": 1}
    )
    return doc

def comentarios_usuario_curso(db, usuario_id, curso_id):
    resultados = db.comentarios.find({
        "usuario_id": ObjectId(usuario_id),
        "curso_id": ObjectId(curso_id)
    }, {
        "_id": 0
    })
    return list(resultados)


# FUNCIÓN PARA EJECUTAR
def ejecutar_pipeline(db, pipeline):
    # sirve para ejecutar las pipelines de forma centralizada
    resultado = list(db.entregas.aggregate(pipeline))
    return resultado

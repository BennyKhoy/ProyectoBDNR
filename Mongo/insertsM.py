from bson import ObjectId

#   USUARIOS
def insertar_usuario(db, nombre, correo, password, rol, carrera_id, progreso_carrera=None):
    documento = {
        "nombre": nombre,
        "correo": correo,
        "password": password,
        "rol": rol,
        "carrera_id": ObjectId(carrera_id)
    }
    if progreso_carrera:
        documento["progreso_carrera"] = [
            {
                "curso_id": ObjectId(item["curso_id"]),
                "codigo_curso": item["codigo_curso"],
                "nombre_curso": item["nombre_curso"],
                "estado": item["estado"]
            }
            for item in progreso_carrera
        ]
    resultado = db.usuarios.insert_one(documento)
    return resultado


#   CARRERAS
def insertar_carrera(db, nombre, descripcion, facultad, materias):
    documento = {
        "nombre": nombre,
        "descripcion": descripcion,
        "facultad": facultad,
        "materias": [ObjectId(m) for m in materias]
    }
    resultado = db.carreras.insert_one(documento)
    return resultado


#   MATERIAS
def insertar_materia(db, codigo, nombre, descripcion, categoria, requisitos):
    documento = {
        "codigo": codigo,
        "nombre": nombre,
        "descripcion": descripcion,
        "categoria": categoria,
        "requisitos": [ObjectId(r) for r in requisitos]
    }
    resultado = db.materias.insert_one(documento)
    return resultado


#   CURSOS
def insertar_curso(db, codigo, nombre, periodo, estado, id_profesor, id_materia):
    documento = {
        "codigo": codigo,
        "nombre": nombre,
        "periodo": periodo,
        "estado": estado,
        "id_profesor": ObjectId(id_profesor),
        "id_materia": ObjectId(id_materia)
    }
    resultado = db.cursos.insert_one(documento)
    return resultado


#   TAREAS
def insertar_tarea(db, curso_id, titulo, descripcion, fecha_limite, puntuacion_maxima):
    documento = {
        "curso_id": ObjectId(curso_id),
        "titulo": titulo,
        "descripcion": descripcion,
        "fecha_limite": fecha_limite,
        "puntuacion_maxima": puntuacion_maxima
    }
    resultado = db.tareas.insert_one(documento)
    return resultado


#   ENTREGAS
def insertar_entrega(db, tarea_id, curso_id, alumno_id, fecha_entrega, calificacion, contenido_tipo, contenido):
    documento = {
        "tarea_id": ObjectId(tarea_id),
        "curso_id": ObjectId(curso_id),
        "alumno_id": ObjectId(alumno_id),
        "fecha_entrega": fecha_entrega,
        "calificacion": calificacion,
        "contenido_tipo": contenido_tipo,
        "contenido": contenido
    }
    resultado = db.entregas.insert_one(documento)
    return resultado


#   COMENTARIOS
def insertar_comentario(db, usuario_id, curso_id, tarea_id, texto, fecha):
    documento = {
        "usuario_id": ObjectId(usuario_id),
        "curso_id": ObjectId(curso_id),
        "tarea_id": ObjectId(tarea_id),
        "texto": texto,
        "fecha": fecha
    }
    resultado = db.comentarios.insert_one(documento)
    return resultado

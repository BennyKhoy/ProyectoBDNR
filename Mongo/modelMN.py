# from connect import monfo_conexion, mongo_cerrar

# AUN NO FUNCIONAL


# MODELOS (EJEMPLOS DE DOCUMENTOS)

# USUARIOS
{
    _id: ObjectId('001'),
    nombre: "Ana López",
    correo: "ana@simon.com",
    password: "Donaazul",
    rol: "alumno",
    carrera_id: ObjectId('001'),
    progreso_carrera: [
        {
            curso_id: ObjectId("001"),
            codigo_curso: "BD-2025A",
            nombre_curso: "Bases de Datos I",
            estado: "En curso"
        }
    ]
}


# CARRERAS
{
    "_id": ObjectId('001'),
    "nombre": "Ingenieria en sistemas",
    "descripcion": "Una Carrera enfocada en el desarrollo de sistemas",
    "facultad": "Facultad de Ingenieria",
    "materias": [ObjectId('001')]
}


# MATERIAS
{
    "_id": ObjectId('001'),
    "codigo": "IS-202",
    "nombre": "Bases de datos",
    "descripcion": "Fundamentos de las bases de datos",
    "categoria": "Obligatoria",
    "requisitos": [ObjectId('001')]
}


# CURSOS
{
    "_id": ObjectId('001'),
    "codigo": "IS.-202-2025",
    "nombre": "Bases de datos 2025",
    "periodo": "O2025",
    "estado": "En curso",
    "id_profesor": ObjectId('001'),
    "id_materia": ObjectId('001')
}


# TAREAS
{
    "_id": ObjectId('001'),
    "curso_id": ObjectId('001'),
    "titulo": "Normalizacion",
    "descripcion": "Aplicar la normalización a las tablas",
    "fecha_limite": ISODate("2025-03-01T23:59:00Z"),
    "puntuacion_maxima": 100.0
}


# ENTREGAS
{
    _id: ObjectId('001'),
    "tarea_id": ObjectId('001'),
    "curso_id": ObjectId('001'),
    "alumno_id": ObjectId('001'),
    "fecha_entrega": ISODate("2025-03-01T23:59:00Z"),
    "calificacion": 90,
    "contenido_tipo": "link",
    "contenido": " https://office.com/gus/tarea1"
}


# COMENTARIOS
{
    _id: ObjectId('001'),
    "usuario_id": ObjectId('001'),
    "curso_id": ObjectId('001'),
    "tarea_id": ObjectId('001'),
    texto: "Se puede hacer solo?",
    fecha: ISODate("2025-03-01T23:59:00Z")
}


# INDICES
db.comentarios.createIndex({usuario_id: 1, curso_id: 1})

db.entregas.createIndex({curso_id: 1, alumno_id: 1})

# posiblemente
db.usuarios.createIndex({correo: 1}, {unique: true})


# AGREGACIONES

# Promedio de calificaciones por curso para un alumno
db.entregas.aggregate([
    {
        $match: {
            alumno_id: ObjectId('ID_alumno'),
            calificacion: {$ne: null}
        }
    },
    {
        $group: {
            _id: "$curso_id",
            promedio: {$avg: "$calificacion"}
        }
    },
    {
        $lookup: {
            from: "cursos",
            localField: "_id",
            foreignField: "_id",
            as: "curso"
        }
    },
    {
        $unwind: "$curso"
    },
    {
        $project: {
            _id: 0,
            curso_id: "$_id",
            nombre_curso: "$curso.nombre",
            promedio: 1,
            numero_entregas: 1
        }
    }
])


# Entregas por alumno en un curso y promedio
db.entregas.aggregate([
    {
        $match: {
            alumno_id: ObjectId("ID_alumno"),
            curso_id: ObjectId("ID_curso"),
            calificacion: {$ne: null}
        }
    },
    {
        $group: {
            _id: {
                alumno_id: "$alumno_id",
                curso_id: "$curso_id"
            },
            promedio: {$avg: "$calificacion"},
            entregas: {$push: "$$ROOT"}
        }
    },
    {
        $project: {
            _id: 0,
            alumno_id: "$_id.alumno_id",
            curso_id: "$_id.curso_id",
            promedio,
            entregas
        }
    }
])


# Promedio de calificaciones por curso impartido por un profesor
db.entregas.aggregate([
    {
        $match: {
            calificacion: {$ne: null}
        }
    },
    {
        $lookup: {
            from: "cursos",
            localField: "curso_id",
            foreignField: "_id",
            as: "curso"
        }
    },
    {
        $unwind: "$curso"
    },
    {
        $match: {
            "curso.id_profesor": ObjectId("ID_PROFESOR")
        }
    },
    {
        $group: {
            _id: "$curso_id",
            nombre_curso: {$first: "$curso.nombre"},
            promedio_curso: {$avg: "$calificacion"}
        }
    },
    {
        $project: {
            _id: 0,
            nombre_curso: 1,
            promedio_curso: 1
        }
    }
])


# Promedio general por materia
db.entregas.aggregate([
    {
        $match: {
            calificacion: {$ne: null}
        }
    },
    {
        $group: {
            _id: "$curso_id",
            promedio_curso: {$avg: "$calificacion"}
        }
    },
    {
        $lookup: {
            from: "cursos",
            localField: "_id",
            foreignField: "_id",
            as: "curso"
        }
    },
    {
        $unwind: "$curso"
    },
    {
        $lookup: {
            from: "materias",
            localField: "curso.id_materia",
            foreignField: "_id",
            as: "materia"
        }
    },
    {
        $unwind: "$materia"
    },
    {
        $group: {
            _id: "$materia._id",
            nombre_materia: {$first: "$materia.nombre"},
            promedio_materia: {$avg: "$promedio_curso"}
        }
    },
    {
        $project: {
            _id: 0,
            nombre_materia: 1,
            promedio_materia: 1
        }
    }
])

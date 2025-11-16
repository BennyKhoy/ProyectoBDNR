import datetime
import json

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
    def create_data(client):
        #

    
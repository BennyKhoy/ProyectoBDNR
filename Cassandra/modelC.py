import uuid
import time_uuid
import datetime

from cassandra.query import BatchStatement
log = logging.getLogger()

CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_LOGS_USUARIO_TABLE = """
    CREATE TABLE IF NOT EXISTS logs_usuario (
        user_id uuid,
        fecha_hora timestamp,
        accion text,
        PRIMARY KEY (user_id, fecha_hora)
    ) WITH CLUSTERING ORDER BY (fecha_hora DESC);
    """

CREATE_ENTREGAS_ESTUDIANTE_TABLE = """
CREATE TABLE IF NOT EXISTS entregas_estudiante (
    user_id uuid,
    tarea_id uuid,
    fecha_entrega timestamp,
    id_curso uuid,
    contenido text,
    PRIMARY KEY (user_id, tarea_id, fecha_entrega)
) WITH CLUSTERING ORDER BY (tarea_id ASC, fecha_entrega DESC);
    """

CREATE_ASISTENCIA_POR_ALUMNO_TABLE = """
CREATE TABLE IF NOT EXISTS asistencia_por_alumno (
    user_id uuid,
    fecha_sesion timestamp,
    id_curso uuid,
    sesion_id uuid,
    estatus_asistencia text,
    PRIMARY KEY (user_id, fecha_sesion)
) WITH CLUSTERING ORDER BY (fecha_sesion DESC);
    """

CREATE_REGISTRO_NOTIFICACIONES_TABLE = """
CREATE TABLE IF NOT EXISTS registro_notificaciones (
    user_id uuid,
    fecha_envio timeuuid,
    tipo_notificacion text, 
    mensaje text,
    leida boolean,
    PRIMARY KEY (user_id, fecha_envio)
) WITH CLUSTERING ORDER BY (fecha_envio DESC);
    """

CREATE_HISTORIAL_ACADEMICO_TABLE = """
CREATE TABLE IF NOT EXISTS historial_academico (
    user_id uuid,
    fecha_evento timestamp,
    id_curso uuid,
    nombre_curso text,
    estado text,
    PRIMARY KEY (user_id, fecha_evento)
) WITH CLUSTERING ORDER BY (fecha_evento DESC);
    """

CREATE_LOG_INSCRIPCIONES_CURSO_TABLE = """
CREATE TABLE IF NOT EXISTS log_inscripciones_curso (
    id_curso uuid,
    fecha_alta timestamp,
    user_id uuid,
    nombre_alumno text,
    PRIMARY KEY (id_curso, fecha_alta, user_id)
) WITH CLUSTERING ORDER BY (fecha_alta ASC);
    """

CREATE_MOVIMIENTOS_POR_PROFESOR_TABLE = """
CREATE TABLE IF NOT EXISTS movimientos_por_profesor (
    id_profesor uuid,
    fecha_movimiento timestamp,
    id_curso uuid,
    accion_realizada text,
    detalle_contexto text, 
    PRIMARY KEY (id_profesor, fecha_movimiento)
) WITH CLUSTERING ORDER BY (fecha_movimiento DESC);
    """

CREATE_MENSAJES_USUARIO_TABLE = """
CREATE TABLE IF NOT EXISTS mensajes_usuario (
    user_id_inbox uuid,
    fecha_envio timeuuid,
    id_emisor uuid,
    id_receptor uuid,
    nombre_emisor text,
    texto_mensaje text,
    PRIMARY KEY (user_id_inbox, fecha_envio)
) WITH CLUSTERING ORDER BY (fecha_envio DESC);
    """

CREATE_ASESORIAS_POR_ALUMNO_TABLE = """
CREATE TABLE IF NOT EXISTS asesorias_por_alumno (
    user_id uuid,
    fecha_asesoria timestamp,
    id_profesor uuid,
    nombre_profesor text,
    tema text,
    PRIMARY KEY (user_id, fecha_asesoria)
) WITH CLUSTERING ORDER BY (fecha_asesoria DESC);
    """

CREATE_ASESORIAS_POR_PROFESOR_TABLE = """
CREATE TABLE IF NOT EXISTS asesorias_por_profesor (
    id_profesor uuid,
    fecha_asesoria timestamp,
    user_id uuid,
    nombre_alumno text,
    tema text,
    PRIMARY KEY (id_profesor, fecha_asesoria)
) WITH CLUSTERING ORDER BY (fecha_asesoria DESC);
    """

CREATE_SESIONES_PROFESOR_TABLE = """
CREATE TABLE IF NOT EXISTS sesiones_profesor (
    id_profesor uuid,
    fecha_sesion timestamp,
    id_curso uuid,
    id_sesion uuid,
    numero_asistentes int,
    horario text,
    PRIMARY KEY (id_profesor, fecha_sesion, id_curso)
) WITH CLUSTERING ORDER BY (fecha_sesion DESC);
    """

CREATE_HISTORIAL_ESTADOS_CURSO_TABLE = """
CREATE TABLE IF NOT EXISTS historial_estados_curso (
    id_curso uuid,
    fecha_cambio timestamp,
    nuevo_estado text, 
    PRIMARY KEY (id_curso, fecha_cambio)
) WITH CLUSTERING ORDER BY (fecha_cambio DESC);
    """
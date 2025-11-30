import logging
import uuid
import datetime
import time 
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.util import uuid_from_time

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


INSERT_LOG_USUARIO = "INSERT INTO logs_usuario (user_id, fecha_hora, accion) VALUES (?, ?, ?)"
INSERT_ENTREGA = "INSERT INTO entregas_estudiante (user_id, tarea_id, fecha_entrega, id_curso, contenido) VALUES (?, ?, ?, ?, ?)"
INSERT_ASISTENCIA = "INSERT INTO asistencia_por_alumno (user_id, fecha_sesion, id_curso, sesion_id, estatus_asistencia) VALUES (?, ?, ?, ?, ?)"
INSERT_NOTIFICACION = "INSERT INTO registro_notificaciones (user_id, fecha_envio, tipo_notificacion, mensaje, leida) VALUES (?, ?, ?, ?, ?)"
INSERT_HISTORIAL_ACAD = "INSERT INTO historial_academico (user_id, fecha_evento, id_curso, nombre_curso, estado) VALUES (?, ?, ?, ?, ?)"
INSERT_INSCRIPCION = "INSERT INTO log_inscripciones_curso (id_curso, fecha_alta, user_id, nombre_alumno) VALUES (?, ?, ?, ?)"
INSERT_MOVIMIENTO_PROF = "INSERT INTO movimientos_por_profesor (id_profesor, fecha_movimiento, id_curso, accion_realizada, detalle_contexto) VALUES (?, ?, ?, ?, ?)"
INSERT_MENSAJE = "INSERT INTO mensajes_usuario (user_id_inbox, fecha_envio, id_emisor, id_receptor, nombre_emisor, texto_mensaje) VALUES (?, ?, ?, ?, ?, ?)"
INSERT_ASESORIA_ALUMNO = "INSERT INTO asesorias_por_alumno (user_id, fecha_asesoria, id_profesor, nombre_profesor, tema) VALUES (?, ?, ?, ?, ?)"
INSERT_ASESORIA_PROFESOR = "INSERT INTO asesorias_por_profesor (id_profesor, fecha_asesoria, user_id, nombre_alumno, tema) VALUES (?, ?, ?, ?, ?)"
INSERT_SESION_PROF = "INSERT INTO sesiones_profesor (id_profesor, fecha_sesion, id_curso, id_sesion, numero_asistentes, horario) VALUES (?, ?, ?, ?, ?, ?)"
INSERT_ESTADO_CURSO = "INSERT INTO historial_estados_curso (id_curso, fecha_cambio, nuevo_estado) VALUES (?, ?, ?)"

SELECT_LOGS_BY_USER = "SELECT * FROM logs_usuario WHERE user_id = ?" #Menu Maestro 18
SELECT_ENTREGAS_BY_USER = "SELECT * FROM entregas_estudiante WHERE user_id = ?" # Menu Alumno 4 y5
SELECT_ASISTENCIA_BY_USER = "SELECT * FROM asistencia_por_alumno WHERE user_id = ?" # Menu Alumno 12
SELECT_NOTIFICACIONES_BY_USER = "SELECT * FROM registro_notificaciones WHERE user_id = ?" # Menu Alumno 9
SELECT_HISTORIAL_ACAD_BY_USER = "SELECT * FROM historial_academico WHERE user_id = ?" # Menu Alumno 10
SELECT_INSCRIPCIONES_BY_CURSO = "SELECT * FROM log_inscripciones_curso WHERE id_curso = ?" # Menu Maestro 16
SELECT_MOVIMIENTOS_BY_PROF = "SELECT * FROM movimientos_por_profesor WHERE id_profesor = ?" # Menu Maestro 17
SELECT_MENSAJES_BY_USER = "SELECT * FROM mensajes_usuario WHERE user_id_inbox = ?" # Menu Alumno 8
SELECT_ASESORIAS_BY_ALUMNO = "SELECT * FROM asesorias_por_alumno WHERE user_id = ?" # Menu Alumno 11
SELECT_ASESORIAS_BY_PROFESOR = "SELECT * FROM asesorias_por_profesor WHERE id_profesor = ?" # Menu Maestro 13
SELECT_SESIONES_BY_PROFESOR = "SELECT * FROM sesiones_profesor WHERE id_profesor = ?" # Menu Maestro 14
SELECT_ESTADOS_BY_CURSO = "SELECT * FROM historial_estados_curso WHERE id_curso = ?" # Menu Maestro 15



def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    log.info("Creando tablas en Cassandra")
    session.execute(CREATE_LOGS_USUARIO_TABLE)
    session.execute(CREATE_ENTREGAS_ESTUDIANTE_TABLE)
    session.execute(CREATE_ASISTENCIA_POR_ALUMNO_TABLE)
    session.execute(CREATE_REGISTRO_NOTIFICACIONES_TABLE)
    session.execute(CREATE_HISTORIAL_ACADEMICO_TABLE)
    session.execute(CREATE_LOG_INSCRIPCIONES_CURSO_TABLE)
    session.execute(CREATE_MOVIMIENTOS_POR_PROFESOR_TABLE)
    session.execute(CREATE_MENSAJES_USUARIO_TABLE)
    session.execute(CREATE_ASESORIAS_POR_ALUMNO_TABLE)
    session.execute(CREATE_ASESORIAS_POR_PROFESOR_TABLE)
    session.execute(CREATE_SESIONES_PROFESOR_TABLE)
    session.execute(CREATE_HISTORIAL_ESTADOS_CURSO_TABLE)
    log.info("Tablas creadas exitosamente")



def insert_log_usuario(session, user_id, accion):
    fecha_hora = datetime.datetime.now()
    stmt = session.prepare(INSERT_LOG_USUARIO)
    session.execute(stmt, [user_id, fecha_hora, accion])

def get_logs_usuario(session, user_id):
    stmt = session.prepare(SELECT_LOGS_BY_USER)
    rows = session.execute(stmt, [user_id])
    print(f"\n--- Logs del usuario {user_id} ---")
    for row in rows:
        print(f"[{row.fecha_hora}] Acción: {row.accion}")

def insert_entrega(session, user_id, tarea_id, id_curso, contenido):
    fecha = datetime.datetime.now()
    stmt = session.prepare(INSERT_ENTREGA)
    session.execute(stmt, [user_id, tarea_id, fecha, id_curso, contenido])

def get_entregas_alumno(session, user_id):
    stmt = session.prepare(SELECT_ENTREGAS_BY_USER)
    rows = session.execute(stmt, [user_id])
    print(f"\n--- Entregas del alumno {user_id} ---")
    for row in rows:
        print(f"Tarea: {row.tarea_id} | Fecha: {row.fecha_entrega} | Contenido: {row.contenido}")

def insert_asistencia(session, user_id, id_curso, sesion_id, estatus):
    fecha = datetime.datetime.now()
    stmt = session.prepare(INSERT_ASISTENCIA)
    session.execute(stmt, [user_id, fecha, id_curso, sesion_id, estatus])

def get_asistencia_alumno(session, user_id):
    stmt = session.prepare(SELECT_ASISTENCIA_BY_USER)
    rows = session.execute(stmt, [user_id])
    print(f"\n--- Asistencia del alumno {user_id} ---")
    for row in rows:
        print(f"Fecha: {row.fecha_sesion} | Estatus: {row.estatus_asistencia}")

def insert_notificacion(session, user_id, tipo, mensaje):
    fecha_uuid = uuid_from_time(datetime.datetime.now())
    stmt = session.prepare(INSERT_NOTIFICACION)
    session.execute(stmt, [user_id, fecha_uuid, tipo, mensaje, False])

def get_notificaciones(session, user_id):
    stmt = session.prepare(SELECT_NOTIFICACIONES_BY_USER)
    rows = session.execute(stmt, [user_id])
    print(f"\n--- Notificaciones ---")
    for row in rows:
        print(f"[{row.tipo_notificacion}] {row.mensaje} (Leída: {row.leida})")

def get_historial_academico(session, user_id):
    stmt = session.prepare(SELECT_HISTORIAL_ACAD_BY_USER)
    rows = session.execute(stmt, [user_id])
    print(f"\n--- Historial Académico ---")
    for row in rows:
        print(f"Curso: {row.nombre_curso} | Estado: {row.estado} | Fecha: {row.fecha_evento}")

def get_inscripciones_curso(session, id_curso):
    stmt = session.prepare(SELECT_INSCRIPCIONES_BY_CURSO)
    rows = session.execute(stmt, [id_curso])
    print(f"\n--- Inscritos en el curso {id_curso} ---")
    for row in rows:
        print(f"Alumno: {row.nombre_alumno} | Fecha Alta: {row.fecha_alta}")

def insert_movimiento_profesor(session, id_profesor, id_curso, accion, detalle):
    fecha = datetime.datetime.now()
    stmt = session.prepare(INSERT_MOVIMIENTO_PROF)
    session.execute(stmt, [id_profesor, fecha, id_curso, accion, detalle])

def get_movimientos_profesor(session, id_profesor):
    stmt = session.prepare(SELECT_MOVIMIENTOS_BY_PROF)
    rows = session.execute(stmt, [id_profesor])
    print(f"\n--- Movimientos del profesor ---")
    for row in rows:
        print(f"[{row.fecha_movimiento}] {row.accion_realizada}: {row.detalle_contexto}")

def enviar_mensaje(session, id_emisor, nombre_emisor, id_receptor, texto):
    fecha_uuid = uuid_from_time(datetime.datetime.now())
    batch = BatchStatement()
    stmt = session.prepare(INSERT_MENSAJE)

    batch.add(stmt, [id_receptor, fecha_uuid, id_emisor, id_receptor, nombre_emisor, texto])
    
    batch.add(stmt, [id_emisor, fecha_uuid, id_emisor, id_receptor, nombre_emisor, texto])
    
    session.execute(batch)
    print("Mensaje enviado correctamente.")

def get_mensajes(session, user_id_inbox):
    stmt = session.prepare(SELECT_MENSAJES_BY_USER)
    rows = session.execute(stmt, [user_id_inbox])
    print(f"\n--- Bandeja de Entrada ---")
    for row in rows:
        print(f"De: {row.nombre_emisor} | Mensaje: {row.texto_mensaje}")

def agendar_asesoria(session, id_profesor, nombre_profesor, user_id_alumno, nombre_alumno, tema):
    fecha = datetime.datetime.now()
    batch = BatchStatement()
    
    stmt_alumno = session.prepare(INSERT_ASESORIA_ALUMNO)
    stmt_prof = session.prepare(INSERT_ASESORIA_PROFESOR)

    batch.add(stmt_alumno, [user_id_alumno, fecha, id_profesor, nombre_profesor, tema])
    
    batch.add(stmt_prof, [id_profesor, fecha, user_id_alumno, nombre_alumno, tema])

    session.execute(batch)
    print("Asesoría agendada en ambos calendarios.")

def get_asesorias_alumno(session, user_id):
    stmt = session.prepare(SELECT_ASESORIAS_BY_ALUMNO)
    rows = session.execute(stmt, [user_id])
    print(f"\n--- Asesorías del Alumno ---")
    for row in rows:
        print(f"Profesor: {row.nombre_profesor} | Tema: {row.tema} | Fecha: {row.fecha_asesoria}")

def get_asesorias_profesor(session, id_profesor):
    stmt = session.prepare(SELECT_ASESORIAS_BY_PROFESOR)
    rows = session.execute(stmt, [id_profesor])
    print(f"\n--- Asesorías del Profesor ---")
    for row in rows:
        print(f"Alumno: {row.nombre_alumno} | Tema: {row.tema} | Fecha: {row.fecha_asesoria}")

def get_bitacora_clases(session, id_profesor):
    stmt = session.prepare(SELECT_SESIONES_BY_PROFESOR)
    rows = session.execute(stmt, [id_profesor])
    print(f"\n--- Bitácora de Clases ---")
    for row in rows:
        print(f"Fecha: {row.fecha_sesion} | Asistentes: {row.numero_asistentes} | Horario: {row.horario}")

def get_estados_curso(session, id_curso):
    stmt = session.prepare(SELECT_ESTADOS_BY_CURSO)
    rows = session.execute(stmt, [id_curso])
    print(f"\n--- Historial de Estados del Curso {id_curso} ---")
    for row in rows:
        print(f"[{row.fecha_cambio}] Nuevo Estado: {row.nuevo_estado}")
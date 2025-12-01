import logging
import uuid
import datetime
import random
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.util import uuid_from_time


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

#queries

INSERT_LOG_USUARIO = "INSERT INTO logs_usuario (user_id, fecha_hora, accion) VALUES (?, ?, ?)"
SELECT_LOGS_BY_USER = "SELECT * FROM logs_usuario WHERE user_id = ?"

INSERT_ENTREGA = "INSERT INTO entregas_estudiante (user_id, tarea_id, fecha_entrega, id_curso, contenido) VALUES (?, ?, ?, ?, ?)"
SELECT_ENTREGAS_BY_USER = "SELECT * FROM entregas_estudiante WHERE user_id = ?"

INSERT_ASISTENCIA = "INSERT INTO asistencia_por_alumno (user_id, fecha_sesion, id_curso, sesion_id, estatus_asistencia) VALUES (?, ?, ?, ?, ?)"
SELECT_ASISTENCIA_BY_USER = "SELECT * FROM asistencia_por_alumno WHERE user_id = ?"

INSERT_NOTIFICACION = "INSERT INTO registro_notificaciones (user_id, fecha_envio, tipo_notificacion, mensaje, leida) VALUES (?, ?, ?, ?, ?)"
SELECT_NOTIFICACIONES_BY_USER = "SELECT * FROM registro_notificaciones WHERE user_id = ?"

INSERT_HISTORIAL_ACAD = "INSERT INTO historial_academico (user_id, fecha_evento, id_curso, nombre_curso, estado) VALUES (?, ?, ?, ?, ?)"
SELECT_HISTORIAL_ACAD_BY_USER = "SELECT * FROM historial_academico WHERE user_id = ?"

INSERT_INSCRIPCION = "INSERT INTO log_inscripciones_curso (id_curso, fecha_alta, user_id, nombre_alumno) VALUES (?, ?, ?, ?)"
SELECT_INSCRIPCIONES_BY_CURSO = "SELECT * FROM log_inscripciones_curso WHERE id_curso = ?"

INSERT_MOVIMIENTO_PROF = "INSERT INTO movimientos_por_profesor (id_profesor, fecha_movimiento, id_curso, accion_realizada, detalle_contexto) VALUES (?, ?, ?, ?, ?)"
SELECT_MOVIMIENTOS_BY_PROF = "SELECT * FROM movimientos_por_profesor WHERE id_profesor = ?"

INSERT_MENSAJE = "INSERT INTO mensajes_usuario (user_id_inbox, fecha_envio, id_emisor, id_receptor, nombre_emisor, texto_mensaje) VALUES (?, ?, ?, ?, ?, ?)"
SELECT_MENSAJES_BY_USER = "SELECT * FROM mensajes_usuario WHERE user_id_inbox = ?"

INSERT_ASESORIA_ALUMNO = "INSERT INTO asesorias_por_alumno (user_id, fecha_asesoria, id_profesor, nombre_profesor, tema) VALUES (?, ?, ?, ?, ?)"
INSERT_ASESORIA_PROFESOR = "INSERT INTO asesorias_por_profesor (id_profesor, fecha_asesoria, user_id, nombre_alumno, tema) VALUES (?, ?, ?, ?, ?)"
SELECT_ASESORIAS_BY_ALUMNO = "SELECT * FROM asesorias_por_alumno WHERE user_id = ?"
SELECT_ASESORIAS_BY_PROFESOR = "SELECT * FROM asesorias_por_profesor WHERE id_profesor = ?"

INSERT_SESION_PROF = "INSERT INTO sesiones_profesor (id_profesor, fecha_sesion, id_curso, id_sesion, numero_asistentes, horario) VALUES (?, ?, ?, ?, ?, ?)"
SELECT_SESIONES_BY_PROFESOR = "SELECT * FROM sesiones_profesor WHERE id_profesor = ?"

INSERT_ESTADO_CURSO = "INSERT INTO historial_estados_curso (id_curso, fecha_cambio, nuevo_estado) VALUES (?, ?, ?)"
SELECT_ESTADOS_BY_CURSO = "SELECT * FROM historial_estados_curso WHERE id_curso = ?"


#conviertir str a uuid
def to_uuid(value):
    if isinstance(value, str):
            return uuid.UUID(value)
    return value

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creando keyspace: {keyspace}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    log.info("Creando esquema en Cassandra")
    tables = [
        CREATE_LOGS_USUARIO_TABLE,
        CREATE_ENTREGAS_ESTUDIANTE_TABLE,
        CREATE_ASISTENCIA_POR_ALUMNO_TABLE,
        CREATE_REGISTRO_NOTIFICACIONES_TABLE,
        CREATE_HISTORIAL_ACADEMICO_TABLE,
        CREATE_LOG_INSCRIPCIONES_CURSO_TABLE,
        CREATE_MOVIMIENTOS_POR_PROFESOR_TABLE,
        CREATE_MENSAJES_USUARIO_TABLE,
        CREATE_ASESORIAS_POR_ALUMNO_TABLE,
        CREATE_ASESORIAS_POR_PROFESOR_TABLE,
        CREATE_SESIONES_PROFESOR_TABLE,
        CREATE_HISTORIAL_ESTADOS_CURSO_TABLE
    ]
    for table_sql in tables:
        session.execute(table_sql)
    log.info("Tablas creadas correctamente.")


#funcines para main.py

# REQ 1
def insert_log_usuario(session, user_id, accion):
    user_uuid = to_uuid(user_id)
    fecha_hora = datetime.datetime.now()
    stmt = session.prepare(INSERT_LOG_USUARIO)
    session.execute(stmt, [user_uuid, fecha_hora, accion])
    log.info(f"Log insertado para usuario {user_id}")

def get_logs_usuario(session, user_id):
    user_uuid = to_uuid(user_id)
    stmt = session.prepare(SELECT_LOGS_BY_USER)
    rows = session.execute(stmt, [user_uuid])
    print(f"\n--- Logs del usuario {user_id} ---")
    for row in rows:
        print(f"[{row.fecha_hora}] {row.accion}")

# REQ 2
def insert_entrega(session, user_id, tarea_id, id_curso, contenido):
    user_uuid = to_uuid(user_id)
    tarea_uuid = to_uuid(tarea_id)
    curso_uuid = to_uuid(id_curso)
    fecha = datetime.datetime.now()
    
    stmt = session.prepare(INSERT_ENTREGA)
    session.execute(stmt, [user_uuid, tarea_uuid, fecha, curso_uuid, contenido])
    print("Entrega registrada exitosamente.")

def get_entregas_alumno(session, user_id):
    user_uuid = to_uuid(user_id)
    stmt = session.prepare(SELECT_ENTREGAS_BY_USER)
    rows = session.execute(stmt, [user_uuid])
    print(f"\n--- Entregas del alumno {user_id} ---")
    for row in rows:
        print(f"Tarea: {row.tarea_id} | Fecha: {row.fecha_entrega} | Contenido: {row.contenido}")

# REQ 3
def insert_asistencia(session, user_id, id_curso, sesion_id, estatus):
    user_uuid = to_uuid(user_id)
    curso_uuid = to_uuid(id_curso)
    sesion_uuid = to_uuid(sesion_id)
    fecha = datetime.datetime.now()

    stmt = session.prepare(INSERT_ASISTENCIA)
    session.execute(stmt, [user_uuid, fecha, curso_uuid, sesion_uuid, estatus])
    print("Asistencia registrada.")

def get_asistencia_alumno(session, user_id):
    user_uuid = to_uuid(user_id)
    stmt = session.prepare(SELECT_ASISTENCIA_BY_USER)
    rows = session.execute(stmt, [user_uuid])
    print(f"\n--- Asistencia ---")
    for row in rows:
        print(f"Fecha: {row.fecha_sesion} | Estatus: {row.estatus_asistencia}")

#REQ 4
def insert_notificacion(session, user_id, tipo, mensaje):
    user_uuid = to_uuid(user_id)
    fecha_uuid = uuid_from_time(datetime.datetime.now())
    
    stmt = session.prepare(INSERT_NOTIFICACION)
    session.execute(stmt, [user_uuid, fecha_uuid, tipo, mensaje, False])
    print("Notificación enviada.")

def get_notificaciones(session, user_id):
    user_uuid = to_uuid(user_id)
    stmt = session.prepare(SELECT_NOTIFICACIONES_BY_USER)
    rows = session.execute(stmt, [user_uuid])
    print(f"\n--- Notificaciones ---")
    for row in rows:
        fecha_legible = row.fecha_envio.datetime if row.fecha_envio else "N/A"
        print(f"[{fecha_legible}] {row.tipo_notificacion}: {row.mensaje} (Leída: {row.leida})")

#REQ 5
def insert_inscripcion(session, id_curso, user_id, nombre_alumno):
    curso_uuid = to_uuid(id_curso)
    user_uuid = to_uuid(user_id)
    fecha = datetime.datetime.now()

    stmt = session.prepare(INSERT_INSCRIPCION)
    session.execute(stmt, [curso_uuid, fecha, user_uuid, nombre_alumno])
    print("Inscripción registrada en bitácora.")

def get_inscripciones_curso(session, id_curso):
    curso_uuid = to_uuid(id_curso)
    stmt = session.prepare(SELECT_INSCRIPCIONES_BY_CURSO)
    rows = session.execute(stmt, [curso_uuid])
    print(f"\n--- Inscritos en el curso {id_curso} ---")
    for row in rows:
        print(f"Alumno: {row.nombre_alumno} | Fecha Alta: {row.fecha_alta}")

#REQ 7
def enviar_mensaje(session, id_emisor, nombre_emisor, id_receptor, texto):
    emisor_uuid = to_uuid(id_emisor)
    receptor_uuid = to_uuid(id_receptor)
    fecha_uuid = uuid_from_time(datetime.datetime.now())
    
    stmt = session.prepare(INSERT_MENSAJE)
    batch = BatchStatement()

    # Escribir en inbox del receptor
    batch.add(stmt, [receptor_uuid, fecha_uuid, emisor_uuid, receptor_uuid, nombre_emisor, texto])
    
    # Escribir en inbox del emisor
    batch.add(stmt, [emisor_uuid, fecha_uuid, emisor_uuid, receptor_uuid, "Yo", texto])
    
    session.execute(batch)
    print("Mensaje enviado y sincronizado en ambas bandejas.")

def get_mensajes(session, user_id_inbox):
    user_uuid = to_uuid(user_id_inbox)
    stmt = session.prepare(SELECT_MENSAJES_BY_USER)
    rows = session.execute(stmt, [user_uuid])
    print(f"\n--- Bandeja de Entrada ---")
    for row in rows:
        fecha = row.fecha_envio.datetime if row.fecha_envio else "?"
        print(f"[{fecha}] De: {row.nombre_emisor} | Mensaje: {row.texto_mensaje}")

#REQ 9
def agendar_asesoria(session, id_profesor, nombre_profesor, user_id_alumno, nombre_alumno, tema):
    prof_uuid = to_uuid(id_profesor)
    alumno_uuid = to_uuid(user_id_alumno)
    fecha = datetime.datetime.now()
    
    stmt_alumno = session.prepare(INSERT_ASESORIA_ALUMNO)
    stmt_prof = session.prepare(INSERT_ASESORIA_PROFESOR)
    
    batch = BatchStatement()
    # Insertar en tabla del alumno
    batch.add(stmt_alumno, [alumno_uuid, fecha, prof_uuid, nombre_profesor, tema])
    # Insertar en tabla del profesor
    batch.add(stmt_prof, [prof_uuid, fecha, alumno_uuid, nombre_alumno, tema])

    session.execute(batch)
    print("Asesoría agendada en ambos calendarios.")

def get_asesorias_profesor(session, id_profesor):
    prof_uuid = to_uuid(id_profesor)
    stmt = session.prepare(SELECT_ASESORIAS_BY_PROFESOR)
    rows = session.execute(stmt, [prof_uuid])
    print(f"\n--- Asesorías del Profesor ---")
    for row in rows:
        print(f"Alumno: {row.nombre_alumno} | Tema: {row.tema} | Fecha: {row.fecha_asesoria}")

# REQ 7,10,11
def insert_movimiento_profesor(session, id_profesor, id_curso, accion, detalle):
    prof_uuid = to_uuid(id_profesor)
    curso_uuid = to_uuid(id_curso)
    fecha = datetime.datetime.now()
    stmt = session.prepare(INSERT_MOVIMIENTO_PROF)
    session.execute(stmt, [prof_uuid, fecha, curso_uuid, accion, detalle])

def insert_sesion_profesor(session, id_profesor, id_curso, numero_asistentes, horario):
    prof_uuid = to_uuid(id_profesor)
    curso_uuid = to_uuid(id_curso)
    sesion_id = uuid.uuid4()
    fecha = datetime.datetime.now()
    
    stmt = session.prepare(INSERT_SESION_PROF)
    session.execute(stmt, [prof_uuid, fecha, curso_uuid, sesion_id, numero_asistentes, horario])
    print("Clase registrada en bitácora.")

#historial Académico
def insert_historial_academico(session, user_id, id_curso, nombre_curso, estado):
    user_uuid = to_uuid(user_id)
    curso_uuid = to_uuid(id_curso)
    fecha = datetime.datetime.now()
    stmt = session.prepare(INSERT_HISTORIAL_ACAD)
    session.execute(stmt, [user_uuid, fecha, curso_uuid, nombre_curso, estado])

# Estados del Curso
def insert_estado_curso(session, id_curso, nuevo_estado):
    curso_uuid = to_uuid(id_curso)
    fecha = datetime.datetime.now()
    stmt = session.prepare(INSERT_ESTADO_CURSO)
    session.execute(stmt, [curso_uuid, fecha, nuevo_estado])
from connect import mongo_conexion, mongo_cerrar, cassandra_session, cassandra_cerrar
import Mongo.insertsM as im
import Mongo.modelM as mm
from Cassandra import modelC
import datetime

# variables globales de conexion
client = None
db = None

# simulacion de login
def login():
    while True:
        print("\nInicio de sesión")
        correo = input("Correo: ").strip()
        password = input("Contraseña: ").strip()

        usuario = db.usuarios.find_one({
                "correo": correo,
                "password": password
            })
        
        if not usuario:
            print("Usuario o contraseña incorrectos")
            otra = input("Intentar de nuevo s/n: ").strip().lower()
            if otra != "s":
                print("\nSaliendo del sistema\n")
                return None
            else:
                continue

        print("\nInicio de sesion correcto\n")
        return usuario


def main():
    global client, db
    
    print("Conectando a Cassandra")
    cluster, session = cassandra_session()
    session.set_keyspace('proyecto_bdnr')

    print("Conectando a MongoDB")
    client, db = mongo_conexion()
    mm.crear_indices_mongo(db)

    while True:
        usuario = login()

        if usuario is None:
            break

        rol = usuario.get("rol")

        if rol == "alumno":
            menu_alumno(usuario, session)
        elif rol == "maestro":
            menu_maestro(usuario, session)

    mongo_cerrar(client)
    cassandra_cerrar(cluster)
    print("Conexiones cerradas correctamente")


def get_uuid_input(mensaje="Ingresa tu ID de Usuario UUID de Cassandra: "):
    val = input(mensaje).strip()
    return val

#  menu del alumno no funcional aun
def menu_alumno(usuario, session):
    while True:
        print("\n=== Menu Alumno ===")
        print("---- Informacion academica ----")
        print("1) Ver mis cursos")
        print("2) Ver materias de mi carrera")
        print("3) Ver progreso de mi carrera")

        print("\n--------- Tareas y notas ---------")
        print("4) Ver mis tareas")
        print("5) Entregar tarea texto o link")
        print("6) Ver mis calificaciones y promedios")
        print("7) Ver mis comentarios en tareas")

        print("\n----- Comunicacion y avisos -----")
        print("8) Ver mis mensajes")
        print("9) Ver mis notificaciones")

        print("\n--------- Historiales ---------")
        print("10) Ver mi historial academico")
        print("11) Ver mis asesorias academicas")
        print("12) Ver mi registro de asistencia")
        print("13) Ver compañeros con los que comparto cursos")

        print("\n0) Cerrar sesion")
        print("================================")

        opcion = input("Opcion: ").strip()

        if opcion == "1":
            print("\nMis cursos")
        elif opcion == "2":
            print("\nMaterias de mi carrera")
        elif opcion == "3":
            print("\nProgreso de mi carrera")
        elif opcion == "4":
            print("\nTareas de mis cursos")
        elif opcion == "5":
            print("\nEntregar tarea texto o link")
        elif opcion == "6":
            print("\nCalificaciones y promedios")                  
            alumno_id = input("Ingresa tu ID de alumno ObjectId en texto: ").strip()
            pipeline = mm.pipeline_promedio_cursos_por_alumno(alumno_id)
            resultados = mm.ejecutar_pipeline(db, pipeline)      
            if not resultados:
                print("No se encontraron calificaciones para este alumno.")
            else:
                print("Promedios por curso")
                for doc in resultados:
                    nombre_curso = doc.get("nombre_curso", "Curso sin nombre")
                    promedio = doc.get("promedio", 0)
                    print(f"- {nombre_curso}: {promedio}")

        elif opcion == "7":
            print("\nComentarios en tareas")

        elif opcion == "8":
            print("\n--- Mis Mensajes ---")
            uuid_in = get_uuid_input()
            modelC.get_mensajes(session, uuid_in)

        elif opcion == "9": 
            print("\n--- Mis Notificaciones ---")
            uuid_in = get_uuid_input()
            modelC.get_notificaciones(session, uuid_in)

        elif opcion == "10": 
            print("\n--- Historial Académico ---")
            uuid_in = get_uuid_input()
            stmt = session.prepare(modelC.SELECT_HISTORIAL_ACAD_BY_USER)
            rows = session.execute(stmt, [modelC.to_uuid(uuid_in)])
            for row in rows:
                print(f"[{row.fecha_evento}] {row.nombre_curso}: {row.estado}")

        elif opcion == "11": 
            print("\n--- Mis Asesorías ---")
            uuid_in = get_uuid_input()
            stmt = session.prepare(modelC.SELECT_ASESORIAS_BY_ALUMNO)
            rows = session.execute(stmt, [modelC.to_uuid(uuid_in)])
            for row in rows:
                print(f"[{row.fecha_asesoria}] Con: {row.nombre_profesor} | Tema: {row.tema}")

        elif opcion == "12":
            print("\n--- Registro de Asistencia ---")
            uuid_in = get_uuid_input()
            modelC.get_asistencia_alumno(session, uuid_in)
        elif opcion == "13":
            print("\nCompañeros relacionados por cursos")
        elif opcion == "0":
            print("\nCerrando sesion\n")
            break
        else:
            print("Opcion invalida")


# simulacion del menu del profesor
def menu_maestro(usuario, session):
    while True:
        print("\n=== Menu Maestro ===")
        print("\n----- Registro y configuracion -----")
        print("1) Registrar usuario")
        print("2) Registrar carrera")
        print("3) Registrar materia")

        print("\n----- Gestion de cursos y actividades -----")
        print("4) Gestionar cursos (creacion de cursos y cambiar su estado)")
        print("5) Ver cursos que imparto")
        print("6) Crear tarea")

        print("\n----- Entregas, calificaciones y comentarios -----")
        print("7) Ver entregas por alumno")
        print("8) Calificar entregas y ver promedio del curso")
        print("9) Ver promedio por materia")
        print("10) Ver comentarios de una tarea")
        print("11) Ver comentarios de un usuario en un curso")

        print("\n----- Comunicacion -----")
        print("12) Enviar mensaje a un alumno")

        print("\n----- Asesorias y clases -----")
        print("13) Ver historial de asesorias")
        print("14) Ver bitacora de clases")

        print("\n----- Trazabilidad e historiales -----")
        print("15) Ver estados de un curso a lo largo del tiempo")
        print("16) Ver inscripciones a un curso")
        print("17) Ver historial de mis movimientos")
        print("18) Ver registros de inicio y cierre de sesion")

        print("\n----- Relaciones dentro de la carrera -----")
        print("19) Ver profesores por carrera")
        print("20) Ver alumnos a los que les he dado clase")

        print("\n0) Cerrar sesion")
        print("============================================\n")

        opcion = input("Opcion: ").strip()

        if opcion == "1":
            print("\nRegistro de usuario")
            nombre = input("Nombre completo: ").strip()
            correo = input("Correo: ").strip()
            password = input("Contraseña: ").strip()
            rol = input("Rol alumno o maestro: ").strip()
            carrera_id = input("ID de la carrera ObjectId en texto: ").strip()
            progreso_carrera = None
            try:
                resultado = im.insertar_usuario(db, nombre, correo, password, rol, carrera_id, progreso_carrera)
                print(f"Usuario registrado con ID {resultado.inserted_id}")
            except Exception as e:
                print("Error al registrar usuario")
                print(f"Detalle del error {e}")

        elif opcion == "2":
            print("\nRegistro de carrera")
            nombre = input("Nombre de la carrera: ").strip()
            descripcion = input("Descripcion: ").strip()
            facultad = input("Facultad o departamento: ").strip()
            materias_str = input("IDs de materias separadas por espacio o enter si no hay: ").strip()
            materias = []
            if materias_str:
                materias = materias_str.split()
            try:
                resultado = im.insertar_carrera(db, nombre, descripcion, facultad, materias)
                print(f"Carrera registrada con ID {resultado.inserted_id}")
            except Exception as e:
                print("Error al registrar carrera")
                print(f"Detalle del error {e}")

        elif opcion == "3":
            print("\nRegistro de materia")
            codigo = input("Codigo de la materia: ").strip()
            nombre = input("Nombre de la materia: ").strip()
            descripcion = input("Descripcion: ").strip()
            categoria = input("Categoria por ejemplo obligatoria u optativa: ").strip()
            prereq_str = input("IDs de materias prerequisito separados por espacio o enter si no hay: ").strip()
            requisitos = []
            if prereq_str:
                requisitos = prereq_str.split()
            try:
                resultado = im.insertar_materia(db, codigo, nombre, descripcion, categoria, requisitos)
                print(f"Materia registrada con ID {resultado.inserted_id}")
            except Exception as e:
                print("Error al registrar materia")
                print(f"Detalle del error {e}")
            
        elif opcion == "4":
            print("\nGestion de cursos")
            codigo = input("Codigo del curso: ").strip()
            nombre = input("Nombre del curso: ").strip()
            periodo = input("Periodo por ejemplo 2025A: ").strip()
            estado = input("Estado inicial por ejemplo activo inactivo finalizado: ").strip()
            id_profesor = input("ID del profesor ObjectId en texto: ").strip()
            id_materia = input("ID de la materia ObjectId en texto: ").strip()
            try:
                resultado = im.insertar_curso(db, codigo, nombre, periodo, estado, id_profesor, id_materia)
                print(f"Curso creado con ID {resultado.inserted_id}")
            except Exception as e:
                print("Error al crear el curso")
                print(f"Detalle del error {e}")

        elif opcion == "5":
            print("\nCursos que imparto")

        elif opcion == "6":
            print("\nCrear tarea")
            curso_id = input("ID del curso ObjectId en texto: ").strip()
            titulo = input("Titulo de la tarea: ").strip()
            descripcion = input("Descripcion: ").strip()
            fecha_limite = input("Fecha limite formato AAAA-MM-DD: ").strip()
            puntuacion_maxima = input("Puntuacion maxima: ").strip()
            try:
                resultado = im.insertar_tarea(db, curso_id, titulo, descripcion, fecha_limite, puntuacion_maxima)
                print(f"Tarea creada con ID {resultado.inserted_id}")
            except Exception as e:
                print("Error al crear la tarea")
                print(f"Detalle del error {e}")

        elif opcion == "7":
            print("\nVer entregas por alumno")
            alumno_id = input("ID del alumno: ")
            curso_id = input("ID del curso: ")
            pipeline = mm.pipeline_entregas_por_alumno_curso(alumno_id, curso_id)
            resultados = mm.ejecutar_pipeline(db, pipeline)
            print(resultados)

        elif opcion == "8":
            print("\nCalificar entregas y ver promedio del curso")
            profesor_id = input("ID del profesor: ")
            pipeline = mm.pipeline_promedio_cursos_profesor(profesor_id)
            resultados = mm.ejecutar_pipeline(db, pipeline)
            print(resultados)
        
        elif opcion == "9":
            print("\nPromedio por materia")
            pipeline = mm.pipeline_promedio_general_por_materia()
            resultados = mm.ejecutar_pipeline(db, pipeline)
            print(resultados)

        elif opcion == "10":
            print("\nComentarios de una tarea")
        elif opcion == "11":
            print("\nComentarios del usuario en un curso")
        elif opcion == "12":
            print("\n--- Enviar Mensaje ---")
            emisor = get_uuid_input("Tu ID (UUID): ")
            receptor = get_uuid_input("ID Destinatario (UUID): ")
            msg = input("Mensaje: ")
            modelC.enviar_mensaje(session, emisor, "Profe", receptor, msg)

        elif opcion == "13": 
            print("\n--- Historial Asesorías ---")
            uuid_in = get_uuid_input("Tu ID de Profesor (UUID): ")
            modelC.get_asesorias_profesor(session, uuid_in)

        elif opcion == "14": 
            print("\n--- Bitácora de Clases ---")
            uuid_in = get_uuid_input("Tu ID de Profesor (UUID): ")
            stmt = session.prepare(modelC.SELECT_SESIONES_BY_PROFESOR)
            rows = session.execute(stmt, [modelC.to_uuid(uuid_in)])
            for row in rows:
                print(f"[{row.fecha_sesion}] Curso: {row.id_curso} | Asistentes: {row.numero_asistentes}")

        elif opcion == "15": 
            print("\n--- Estados del Curso ---")
            curso_id = get_uuid_input("ID del Curso (UUID): ")
            stmt = session.prepare(modelC.SELECT_ESTADOS_BY_CURSO)
            rows = session.execute(stmt, [modelC.to_uuid(curso_id)])
            for row in rows:
                print(f"[{row.fecha_cambio}] Estado: {row.nuevo_estado}")

        elif opcion == "16": 
            print("\n--- Inscripciones al Curso ---")
            curso_id = get_uuid_input("ID del Curso (UUID): ")
            modelC.get_inscripciones_curso(session, curso_id)

        elif opcion == "17": 
            print("\n--- Historial de Movimientos ---")
            uuid_in = get_uuid_input("Tu ID de Profesor (UUID): ")
            stmt = session.prepare(modelC.SELECT_MOVIMIENTOS_BY_PROF)
            rows = session.execute(stmt, [modelC.to_uuid(uuid_in)])
            for row in rows:
                print(f"[{row.fecha_movimiento}] {row.accion_realizada}: {row.detalle_contexto}")

        elif opcion == "18": 
            print("\n--- Logs de Usuario ---")
            uuid_in = get_uuid_input("ID de Usuario a consultar (UUID): ")
            modelC.get_logs_usuario(session, uuid_in)

        # --- DGRAPH ---
        elif opcion == "19":
            print("\nProfesores por carrera (Dgraph - Pendiente)")
        
        elif opcion == "20":
            print("\nAlumnos (Dgraph - Pendiente)")

        elif opcion == "0":
            print("\nCerrando sesion\n")
            break
        else:
            print("Opcion invalida")

# punto de entrada
if __name__ == '__main__':
    main()
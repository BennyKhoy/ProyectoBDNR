#importaciones
import Dgraph.querysD as dq
import Dgraph.modelD as md
from connect import mongo_conexion, mongo_cerrar, cassandra_session, cassandra_cerrar, dgraph_conexion, dgraph_cerrar
import Mongo.insertsM as im
import Mongo.modelM as mm
from Cassandra import modelC
import datetime
from bson import ObjectId


# variables globales de conexion
client = None
db = None

# simulacion de login
def login(session):
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

        # insertar log en Cassandra
        user_uuid = usuario.get("uuid")
        modelC.insert_log_usuario(session, user_uuid, "inicio de sesion")

        return usuario


def main():
    global client, db
    
    # configurando conexiones
    print("Conectando a Cassandra")
    cluster, session = cassandra_session()
    session.set_keyspace('proyecto_bdnr')

    print("Conectando a MongoDB")
    client, db = mongo_conexion()
    mm.crear_indices_mongo(db)

    print("Conectando a Dgraph")
    client_dg, stub_dg = dgraph_conexion()

    # CICLO PRINCIPAL DEL SISTEMA
    while True:
        usuario = login(session)

        if usuario is None:
            break

        rol = usuario.get("rol")

        if rol == "alumno":
            menu_alumno(usuario, session, client_dg)
        elif rol == "maestro":
            menu_maestro(usuario, session, client_dg)

    # cierre de conexiones
    mongo_cerrar(client)
    cassandra_cerrar(cluster)
    dgraph_cerrar(stub_dg)
    print("Conexiones cerradas correctamente")

# funcion para facilitar recibir el uuid
def get_uuid_input(mensaje="Ingresa tu ID de Usuario UUID de Cassandra: "):
    val = input(mensaje).strip()
    return val

#  menu del alumno
def menu_alumno(usuario, session, client_dg):
    # obtenemos los identificadores del usuario
    nom = usuario.get("nombre", None)
    exp = usuario.get("expediente", None)
    uid = usuario.get("uuid", None)
    mongo_id = usuario.get("_id", None)


    print("ID de usuario")
    print("  nombre", nom)
    print("  expediente:", exp)
    #print("  uuid:", uid)
    #print("  id de mongo:", mongo_id)

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

        print("\n--------- Informes ---------")
        print("14) Ver cursos de una materia")
        print("15) Ver materias prerequisito de una materia")
        print("16) Ver profesores de un curso")

        print("\n0) Cerrar sesion")
        print("================================")

        opcion = input("Opcion: ").strip()

        if opcion == "1":
            print("\nMis cursos")
            exp = usuario.get("expediente")
            if exp is None:
                print("No se encontro expediente registrado en tu perfil")
            else:
                dq.cursos_de_alumno2(client_dg, exp) #dgraph req 2

        elif opcion == "2":
            print("\nMaterias de mi carrera")
            dq.materias_de_carrera1(client_dg) #dgraph req 1

        elif opcion == "3":
            print("\nProgreso de mi carrera")
            doc = mm.obtener_progreso_carrera(db, usuario["_id"])
            if not doc or not doc.get("progreso_carrera"):
                print("No hay cursos registrados en tu progreso")
            else:
                print("Alumno:", doc.get("nombre", ""))
                print("Progreso de la carrera")
                print("------------------------------------")
                for curso in doc["progreso_carrera"]:
                    nombre_curso = curso.get("nombre_curso", "")
                    codigo_curso = curso.get("codigo_curso", "")
                    estado = curso.get("estado", "")
                    print("Curso:", nombre_curso)
                    print("Codigo:", codigo_curso)
                    print("Estado:", estado)

        elif opcion == "4":
            print("\nTareas de mis cursos")
            exp = usuario.get("expediente")
            if exp is None:
                print("No se encontro expediente registrado en tu perfil")
            else:
                dq.actividades_de_alumno9(client_dg, exp) #dgraph req 9

        elif opcion == "5":
            print("\nEntregar tarea texto o link")
            print("\nTareas disponibles")
            print("-------------------")

            tareas = list(db.tareas.find({}))
            for t in tareas:
                
                print("ID:", t["_id"])
                print("Titulo:", t.get("titulo", "Sin titulo"))
                print("Descripcion:", t.get("descripcion", ""))
                print("Curso:", t.get("curso_id", ""))
                print("-------------------")

            tarea_id = input("ID de la tarea ObjectId en texto: ").strip()
            curso_id = input("ID del curso ObjectId en texto: ").strip()
            alumno_id = str(usuario["_id"])

            print("\nTipo de entrega")
            print("1) Texto")
            print("2) Link")
            tipo = input("Selecciona una opcion: ").strip()

            if tipo == "1":
                contenido_tipo = "texto"
                contenido = input("Escribe tu entrega: ").strip()
            elif tipo == "2":
                contenido_tipo = "link"
                contenido = input("Ingresa el enlace: ").strip()
            else:
                print("Opcion invalida")
                continue

            fecha_entrega = datetime.datetime.now()
            calificacion = None

            try:
                resultado = im.insertar_entrega(db, tarea_id, curso_id, alumno_id, fecha_entrega, calificacion, contenido_tipo, contenido)
                print("\nEntrega registrada correctamente")
                print("ID de entrega:", resultado.inserted_id)
            except Exception as e:
                print("Error al registrar la entrega")
                print("Detalle del error", e)

        elif opcion == "6":
            print("\nCalificaciones y promedios")                  
            #alumno_id = input("Ingresa tu ID de alumno ObjectId en texto: ").strip()
            alumno_id = str(usuario["_id"])
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
            dq.cometarios_de_actividad10(client_dg) #dgraph req 10

        elif opcion == "8":
            print("\n--- Mis Mensajes ---")
            #uuid_in = get_uuid_input()
            uuid_in = str(usuario.get("uuid"))
            modelC.get_mensajes(session, uuid_in)

        elif opcion == "9": 
            print("\n--- Mis Notificaciones ---")
            #uuid_in = get_uuid_input()
            uuid_in = str(usuario.get("uuid"))
            modelC.get_notificaciones(session, uuid_in)

        elif opcion == "10": 
            print("\n--- Historial Académico ---")
            #uuid_in = get_uuid_input()
            uuid_in = str(usuario.get("uuid"))
            stmt = session.prepare(modelC.SELECT_HISTORIAL_ACAD_BY_USER)
            rows = session.execute(stmt, [modelC.to_uuid(uuid_in)])
            for row in rows:
                print(f"[{row.fecha_evento}] {row.nombre_curso}: {row.estado}")

        elif opcion == "11": 
            print("\n--- Mis Asesorías ---")
            #uuid_in = get_uuid_input()
            uuid_in = str(usuario.get("uuid"))
            stmt = session.prepare(modelC.SELECT_ASESORIAS_BY_ALUMNO)
            rows = session.execute(stmt, [modelC.to_uuid(uuid_in)])
            for row in rows:
                print(f"[{row.fecha_asesoria}] Con: {row.nombre_profesor} | Tema: {row.tema}")

        elif opcion == "12":
            print("\n--- Registro de Asistencia ---")
            #uuid_in = get_uuid_input()
            uuid_in = str(usuario.get("uuid"))
            modelC.get_asistencia_alumno(session, uuid_in)
        elif opcion == "13":
            print("\nCompañeros relacionados por cursos")
            nom = usuario.get("nombre")
            if nom is None:
                print("No se encontro un nombre registrado en tu perfil")
            else:
                dq.companeros_de_alumno(client_dg, nom) #dgraph req 12

        elif opcion == "14":
            print("\nLos cursos de una materia")
            dq.cursos_de_materia3(client_dg) #dgraph req 3

        elif opcion == "15":
            print("\nLas materias con prerequisito")
            dq.materias_prerequisito7(client_dg) #dgraph req 7

        elif opcion == "16":
            print("\nLos profesores de un curso")
            dq.profesores_de_curso5_2(client_dg) #dgraph req 5.2

        elif opcion == "0":
            user_uuid = usuario.get("uuid")
            modelC.insert_log_usuario(session, user_uuid, "cierre de sesion")
            print("\nCerrando sesion\n")
            break
        else:
            print("Opcion invalida")


# simulacion del menu del profesor
def menu_maestro(usuario, session, client_dg):
    corr = usuario.get("correo", None)
    uid = usuario.get("uuid", None)
    mongo_id = usuario.get("_id", None)


    print("ID de usuario")
    print("  correo: ", corr)
    #print("  uuid:", uid)
    #print("  id de mongo:", mongo_id)

    while True:
        print("\n=== Menu Maestro ===")
        print("\n----- Registro y configuracion -----")
        print("1) Registrar usuario")
        print("2) Registrar carrera")
        print("3) Registrar materia")

        print("\n----- Gestion de cursos y actividades -----")
        print("4) Creacion de cursos")
        print("5) Ver cursos que imparto")
        print("6) Crear tarea")

        print("\n----- Entregas, calificaciones y comentarios -----")
        print("7) Ver entregas por alumno")
        print("8) Ver promedio del curso")
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
        print("21) Ver alumnos de un curso")
        print("22) Ver alumnos de una carrera")
        print("23) Ver actividades de un curso")

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
            expediente = input("Expediente del usuario: ").strip()
            progreso_carrera = None
            try:
                resultado = im.insertar_usuario(db, nombre, correo, password, rol, carrera_id, progreso_carrera)
                print(f"Usuario registrado con ID {resultado.inserted_id}")

                if rol.lower() == "alumno":
                    md.insertar_alumno_dg(client_dg, nombre, correo, expediente)
                elif rol.lower() == "maestro":
                    md.insertar_profesor_dg(client_dg, nombre, correo)

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
                md.insertar_carrera_dg(client_dg, nombre, descripcion)
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
                md.insertar_materia_dg(client_dg, codigo, nombre, categoria)
            except Exception as e:
                print("Error al registrar materia")
                print(f"Detalle del error {e}")
            
        elif opcion == "4":
            print("\nCursos")
            codigo = input("Codigo del curso: ").strip()
            nombre = input("Nombre del curso: ").strip()
            descripcion_curso = input("Descripcion del curso: ").strip()
            periodo = input("Periodo por ejemplo 2025A: ").strip()
            estado = input("Estado inicial por ejemplo activo inactivo finalizado: ").strip()
            id_profesor = input("ID del profesor ObjectId en texto: ").strip()
            id_materia = input("ID de la materia ObjectId en texto: ").strip()
            creditos = input("Creditos del curso como numero entero: ").strip()
            try:
                resultado = im.insertar_curso(db, codigo, nombre, periodo, estado, id_profesor, id_materia)
                print(f"Curso creado con ID {resultado.inserted_id}")
                md.insertar_curso_dg(client_dg, codigo, nombre, descripcion_curso, creditos)
            except Exception as e:
                print("Error al crear el curso")
                print(f"Detalle del error {e}")

        elif opcion == "5":
            print("\nCursos que imparto")
            correo = usuario.get("correo")
            if correo is None:
                print("No se encontro correo en tu perfil")
            else:
                dq.cursos_de_profesor5(client_dg, correo) #dgraph req 5

        elif opcion == "6":
            print("\nCrear tarea")
            curso_id = input("ID del curso ObjectId en texto: ").strip()
            codigo_act = input("Codigo de la actividad para Dgraph: ").strip()
            titulo = input("Titulo de la tarea: ").strip()
            descripcion = input("Descripcion: ").strip()
            fecha_limite = input("Fecha limite formato AAAA-MM-DD: ").strip()
            puntuacion_maxima = input("Puntuacion maxima: ").strip()
            try:
                resultado = im.insertar_tarea(db, curso_id, titulo, descripcion, fecha_limite, puntuacion_maxima)
                print(f"Tarea creada con ID {resultado.inserted_id}")
                md.insertar_actividad_dg(client_dg, codigo_act, titulo, descripcion, fecha_limite)
            except Exception as e:
                print("Error al crear la tarea")
                print(f"Detalle del error {e}")

        elif opcion == "7":
            print("\nVer entregas por alumno")
            alumno_id = input("ID del alumno: ")
            curso_id = input("ID del curso: ")
            pipeline = mm.pipeline_entregas_por_alumno_curso(alumno_id, curso_id)
            resultados = mm.ejecutar_pipeline(db, pipeline)
            if not resultados:
                print("No hay entregas registradas para este alumno en este curso")
            else:
                for doc in resultados:
                    promedio = doc.get("promedio", 0)
                    print("\nEntregas encontradas")
                    print("Promedio del alumno en el curso:", promedio)
                    print("---------------")
                    for entrega in doc.get("entregas", []):
                        print("ID de entrega:", entrega.get("_id"))
                        print("ID tarea:", entrega.get("tarea_id"))
                        print("ID curso:", entrega.get("curso_id"))
                        print("Fecha de entrega:", entrega.get("fecha_entrega"))
                        print("Calificacion:", entrega.get("calificacion"))
                        print("Tipo de contenido:", entrega.get("contenido_tipo"))
                        print("Contenido:", entrega.get("contenido"))
                        print("---------------")
                        
        elif opcion == "8":
            print("\nVer promedio del curso")
            #profesor_id = input("ID del profesor: ")
            profesor_id = str(usuario["_id"])
            pipeline = mm.pipeline_promedio_cursos_profesor(profesor_id)
            resultados = mm.ejecutar_pipeline(db, pipeline)
            if not resultados:
                print("No hay calificaciones registradas para los cursos de este profesor")
            else:
                print("Promedios por curso")
                print("---------------")
                for doc in resultados:
                    print("ID del curso:", doc.get("curso_id"))
                    print("Nombre del curso:", doc.get("nombre_curso"))
                    print("Promedio del curso:", doc.get("promedio_curso"))
                    print("---------------")
        
        elif opcion == "9":
            print("\nPromedio por materia")
            pipeline = mm.pipeline_promedio_general_por_materia()
            resultados = mm.ejecutar_pipeline(db, pipeline)
            for doc in resultados:
                nombre = doc.get("nombre_materia", "")
                promedio = doc.get("promedio_materia", "")
                materia_id = str(doc.get("materia_id", ""))
                print("Materia:", nombre)
                print("Promedio:", promedio)
                print("ID:", materia_id)
                print("------------------------------------")

        elif opcion == "10":
            print("\nComentarios de una tarea")
            dq.cometarios_de_actividad10(client_dg) #dgraph req 10

        elif opcion == "11":
            print("\nComentarios del usuario en un curso")
            usuario_id = str(usuario["_id"])
            curso_id = input("ID del curso ObjectId en texto: ").strip()
            resultados = mm.comentarios_usuario_curso(db, usuario_id, curso_id)
            if not resultados:
                print("No se encontraron comentarios para ese usuario en ese curso")
            else:
                print("Comentarios encontrados")
                print("------------------------------------")
                for com in resultados:
                    texto = com.get("texto", "")
                    fecha = com.get("fecha", "")
                    print("Comentario:", texto)
                    print("Fecha:", fecha)
                    print("------------------------------------")

        elif opcion == "12":
            print("\n--- Enviar Mensaje ---")
            #emisor = get_uuid_input("Tu ID (UUID): ")
            emisor = str(usuario.get("uuid"))
            receptor = get_uuid_input("ID Destinatario (UUID): ")
            msg = input("Mensaje: ")
            modelC.enviar_mensaje(session, emisor, usuario['nombre'], receptor, msg)

        elif opcion == "13": 
            print("\n--- Historial Asesorías ---")
            #uuid_in = get_uuid_input("Tu ID de Profesor (UUID): ")
            uuid_in = str(usuario.get("uuid"))
            modelC.get_asesorias_profesor(session, uuid_in)

        elif opcion == "14": 
            print("\n--- Bitácora de Clases ---")
            #uuid_in = get_uuid_input("Tu ID de Profesor (UUID): ")
            uuid_in = str(usuario.get("uuid"))
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
            #uuid_in = get_uuid_input("Tu ID de Profesor (UUID): ")
            uuid_in = str(usuario.get("uuid"))
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
            print("\nProfesores por carrera")
            dq.profesores_de_carrera11(client_dg) #dgraph req 11
        
        elif opcion == "20":
            print("\nAlumnos a los que les he dado clase")
            correo = usuario.get("correo")
            if correo is None:
                print("No se encontro correo en tu perfil")
            else:
                dq.alumnos_de_profesor8(client_dg, correo) #dgraph req 8

        elif opcion == "21":
            print("\nAlumnos de un curso")
            dq.alumnos_de_curso2_2(client_dg) #dgraph req 2.2

        elif opcion == "22":
            print("\nAlumos de una carrera")
            dq.alumnos_de_carrera6(client_dg) #dgraph req 6

        elif opcion == "23":
            print("\nActividades de un curso")
            dq.actividades_de_curso4(client_dg) #dgraph req 4

        elif opcion == "0":
            print("\nCerrando sesion\n")
            user_uuid = usuario.get("uuid")
            modelC.insert_log_usuario(session, user_uuid, "cierre de sesion")
            break
        else:
            print("Opcion invalida")

# punto de entrada
if __name__ == '__main__':
    main()
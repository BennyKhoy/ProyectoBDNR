#menu que muestra lo que se podria hacer
from connect import mongo_conexion, mongo_cerrar
import Mongo.insertsM as im
import Mongo.modelM as mm

# variables globales de conexion
client = None
db = None

# simulacion de login
def login():
    print("\nInicio de sesión")
    usuario = input("Usuario: ")
    password = input("Contraseña: ")

    while True:
        print("\n¿Eres alumno o maestro?")
        print("1) Alumno")
        print("2) Maestro")
        print("0) Salir")

        opcion = input("Opcion: ").strip()

        if opcion == "1":
            rol = "alumno"
            break
        elif opcion == "2":
            rol = "maestro"
            break
        elif opcion == "0":
            print("\nSaliendo del sistema...\n")
            return None
        else:
            print("Opción inválida, intenta de nuevo.\n")

    
    print(f"\nInicio de sesión\n")
    return rol


def main():

    while True:
        rol = login()

        if rol is None:
            # el usuario eligio salir
            break

        if rol == "alumno":
            menu_alumno()
        elif rol == "maestro":
            menu_maestro()


# menu del alumno no funcional aun
def menu_alumno():
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
                print("No se encontraron calificaciones para este alumno")
            else:
                print("Promedios por curso")
                for doc in resultados:
                    nombre_curso = doc.get("nombre_curso", "Curso sin nombre")
                    promedio = doc.get("promedio", 0)
                    print(f"- {nombre_curso}: {promedio}")


        elif opcion == "7":
            print("\nComentarios en tareas")
        elif opcion == "8":
            print("\nMensajes directos")
        elif opcion == "9":
            print("\nNotificaciones")
        elif opcion == "10":
            print("\nHistorial academico")
        elif opcion == "11":
            print("\nAsesorias academicas")
        elif opcion == "12":
            print("\nRegistro de asistencia")
        elif opcion == "13":
            print("\nCompañeros relacionados por cursos")
        elif opcion == "0":
            print("\nCerrando sesion\n")
            break
        else:
            print("Opcion invalida")



# simulacion del menu del profesor
def menu_maestro():
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
        elif opcion == "2":
            print("\nRegistro de carrera")
        elif opcion == "3":
            print("\nRegistro de materia")
        elif opcion == "4":
            print("\nGestion de cursos (creacion de cursos y cambiar su estado)")
        elif opcion == "5":
            print("\nCursos que imparto")
        elif opcion == "6":
            print("\nCrear tarea")

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
            print("\nEnviar mensaje a alumno")
        elif opcion == "13":
            print("\nHistorial de asesorias")
        elif opcion == "14":
            print("\nBitacora de clases")
        elif opcion == "15":
            print("\nEstados del curso a lo largo del tiempo")
        elif opcion == "16":
            print("\nHistorial de inscripciones al curso")
        elif opcion == "17":
            print("\nHistorial de movimientos")
        elif opcion == "18":
            print("\nRegistros de inicio y cierre de sesion")
        elif opcion == "19":
            print("\nProfesores que imparten cursos en la carrera")
        elif opcion == "20":
            print("\nAlumnos a los que les he dado clase")
        elif opcion == "0":
            print("\nCerrando sesion\n")
            break
        else:
            print("Opcion invalida")


# punto de entrada
if __name__ == "__main__":
    client, db = mongo_conexion()

    try:
        main()
    finally:
        mongo_cerrar(client)
        print("Conexion cerrada correctamente")
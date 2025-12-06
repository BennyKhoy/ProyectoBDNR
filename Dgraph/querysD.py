import pydgraph
import json

def imprimir_resultado(res):
    print('Resultado:')
    datos = json.loads(res.json)
    print(json.dumps(datos, indent=4))



def materias_de_carrera1(client):
    nombre = input('ingresa el nombre de la carrera: ')
    query = f"""
    {{
        materias_by_carrera(func: type(Carrera)) @filter(allofterms(nombre, "{nombre}")) {{
            nombre
            codigo
            tiene_materias {{
                nombre
                codigo
                departamento
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def cursos_de_alumno2(client):
    expediente = int(input('ingresa el expediente del alumno: '))
    query = f"""
    {{
        cursos_by_alumno(func: type(Alumno)) @filter(eq(expediente, {expediente})) {{
            nombre
            expediente
            inscrito_en {{
                nombre
                descripcion
                creditos
                codigo
            }}
        }}

    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def alumnos_de_curso2_2(client):
    nombre = input('ingresa el nombre del curso: ')
    query = f"""
    {{
        alumnos_by_curso(func: type(Curso)) @filter(allofterms(nombre, "{nombre}")) {{
            nombre
            creditos
            ~inscrito_en {{
                nombre
                correo
                expediente
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def cursos_de_materia3(client):
    nombre = input('ingresa el nombre la materia: ')
    query = f"""
    {{
        curso_by_materia(func: type(Materia)) @filter(allofterms(nombre, "{nombre}")){{
            nombre
            departamento
            tiene_cursos {{
                nombre
                codigo
                descripcion
                creditos
            }}
        }}

    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def actividades_de_curso4(client):
    nombre = input('ingresa el nombre del curso: ')
    query = f"""
    {{
        actividades_by_curso(func: type(Curso)) @filter(allofterms(nombre, "{nombre}")){{
            nombre
            codigo
            tiene_actividades {{
                titulo
                codigo
                descripcion
                fecha_limite
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def cursos_de_profesor5(client):
    correo = input('ingresa el correo del profesor: ')
    query = f"""
    {{
        cursos_by_profesor(func: type(Profesor)) @filter(eq(correo, "{correo}")){{
            nombre
            correo
            profesor_curso {{
                nombre
                codigo
                descripcion
                creditos
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def profesores_de_curso5_2(client):
    nombre = input('ingresa el nombrel curso: ')
    query = f"""
    {{
        profesores_by_curso(func: type(Curso)) @filter(allofterms(nombre, "{nombre}")){{
            nombre
            codigo
            ~profesor_curso {{
                nombre
                correo
            }}

        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def alumnos_de_carrera6(client):
    nombre = input('Ingresa el nombre de la carrera: ')
    query = f"""
    {{
        alumnos_by_carrera(func: type(Carrera)) @filter(allofterms(nombre, "{nombre}")){{
            nombre
            codigo
            contiene_alumnos{{
                nombre
                correo
                expediente
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def materias_prerequisito7(client):
    nombre = input('ingresa el nombre de la materia: ')
    query = f"""
    {{
        materias_prerrequistos(func: type(Materia)) @filter(allofterms(nombre, "{nombre}")) {{
            nombre
            codigo
            tiene_prerequisito {{
                nombre
                codigo
                departamento
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def alumnos_de_profesor8(client):
    correo = input('ingresa el correo del profesor: ')
    query = f"""
    {{
        alumnos_by_profesor(func: type(Profesor)) @filter(eq(correo, "{correo}")){{
            nombre
            correo
            tiene_alumnos{{
                nombre
                expediente
                correo
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def actividades_de_alumno9(client):
    expediente = int(input('ingresa el expediente del alumno: '))
    query = f"""
    {{
        actividades_by_alumno(func: type(Alumno)) @filter(eq(expediente, {expediente})) {{
            nombre
            expediente
            tiene_asignado {{
                titulo
                codigo
                descripcion
                fecha_limite
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def cometarios_de_actividad10(client):
    titulo = input('ingresa el titulo de la actividad: ')
    query = f"""
    {{
        comentarios_by_actividad(func: type(Actividad)) @filter(allofterms(titulo, "{titulo}")){{
            titulo
            codigo
            tiene_comentarios {{
                fecha
                cuerpo
                codigo
                escrito_por {{
                    nombre
                    expediente
                }}
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def profesores_de_carrera11(client):
    nombre = input('ingresa el nombre de la carrera: ')
    query = f"""
    {{
        cursos_by_carrera(func: type(Carrera)) @filter(allofterms(nombre, "{nombre}")){{
            nombre
            codigo
            tiene_materias {{
                nombre
                tiene_cursos {{
                    nombre
                    creditos
                    ~profesor_curso {{
                        nombre
                        correo
                    }}
                }}
            }}

        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)


def companeros_de_alumno(client):
    nombre = input('ingresa el nombre del alumno: ')
    query = f"""
    {{
        companeros(func: type(Alumno)) @filter(allofterms(nombre, "{nombre}")){{
            nombre
            expediente
            inscrito_en{{
                nombre
                ~inscrito_en{{
                    nombre
                }}
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    imprimir_resultado(res)







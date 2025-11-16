# ProyectoBDNR
# Plataforma educativa en lìnea

## Descripción del Proyecto

Repositorio para el desarrollo de una Plataforma de Educación en Línea funcional que simula un Sistema de Gestión de Aprendizaje (LMS) moderno. El objetivo principal es gestionar cursos, usuarios, contenidos y actividades, aprovechando la arquitectura de tres bases de datos NoSQL diferentes.

## ¿Qué hará?
La plataforma gestionará cursos, usuarios, contenidos y las distintas actividades. Permitirá a un usuario alumno ver sus materias inscritas e inscribirse en nuevas, y a un usuario profesor ver las materias que imparte.

### Tecnologías NoSQL Utilizadas
**MongoDB:** Datos transaccionales y contenido (Usuarios, Cursos, Tareas, etc.).
**Cassandra:** Datos de series de tiempo y logging (Registro de sesiones, entregas de tareas, etc.).
**Dgraph:** Modelado de relaciones de grafo (Inscripciones, Prerrequisitos, etc.)[cite: 91].


 **Integrantes**
 Benjamín Rodríguez Cuevas
 Alan Alejandro Rodríguez Avalos 
 Diego Alonso Gómez Yáñez 

## Flujo de Trabajo y Setup

### 1. Configuración del Entorno Virtual (Python)

Configura e instala las dependencias del proyecto usando un entorno virtual:

```bash
# Instalar y activar virtual env (Linux/MacOS)
python3 -m pip install virtualenv
python3 py -3.11 -m venv venv
source ./venv/bin/activate

# O Instalar y activar virtual env (Windows)
python3 -m pip install virtualenv
python3 py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1

# Instalar requisitos del proyecto (incluye drivers de BD)
pip install cassandra-driver pymongo pydgraph requests
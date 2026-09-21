import sqlite3
import threading

class DatabaseManager:
    def __init__(self, db_file="evcharging.db"):
        self.db_file = db_file
        self.lock = threading.Lock() # lock propio para esta instancia

    def get_connection(self): # inicia la conexion
        conexion = sqlite3.connect(self.db_file, check_same_thread=False)
        conexion.execute("PRAGMA foreign_keys = ON;")
        return conexion
    
    def crear_tablas(self):
        with self.lock:
            conexion = None
            try:
                conexion = self.get_connection() #se conecta con la bd, si no existe se crea
                cursor = conexion.cursor() #para ejecutar comandos sql

                # tabla para guardar la informacion de los puntos de recarga
                cursor.execute("""CREATE TABLE IF NOT EXISTS puntos_recarga (
                cp_id TEXT PRIMARY KEY,
                ubicacion TEXT,
                precio_kwh REAL,
                estado TEXT,
                token TEXT,
                clave_cifrado TEXT)
                """)

                #tabla para guardar la informacion de los conductores
                cursor.execute("""CREATE TABLE IF NOT EXISTS conductor (
                driver_id TEXT PRIMARY KEY,
                nombre TEXT)
                """)

                #tabla para guardar la información de carga
                cursor.execute("""CREATE TABLE IF NOT EXISTS cargas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cp_id TEXT,
                driver_id TEXT,
                energia REAL,
                coste REAL,
                fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (driver_id) REFERENCES conductor (driver_id),
                FOREIGN KEY (cp_id) REFERENCES puntos_recarga (cp_id))
                """)
                
                conexion.commit()
                print("Las tablas se han creado correctamente!")
            except sqlite3.Error as e:
                print(f"Error de SQLite al crear tablas: {e}")
            except Exception as e:
                print(f"Error inesperado al crear tablas: {e}")
            finally:
                if conexion:
                    conexion.close()

    def desconectar_cps(self):
        with self.lock:
            conexion = None
            try:
                conexion = self.get_connection()
                cursor = conexion.cursor()
            
                # pone todos los cps como desc al principio
                cursor.execute("UPDATE puntos_recarga SET estado = 'DESCONECTADO'")
                
                conexion.commit()
            
                print("Estado inicial de todos los CPs DESCONECTADO.")
            except sqlite3.Error as e:
                print(f"Error de SQLite al poner como desconectados los CP: {e}")
            except Exception as e:
                print(f"Error inesperado al poner como desconectado los CP: {e}")
            finally:
                if conexion:
                    conexion.close()

    def registrar_cp(self, cp_id, ubicacion, precio_kwh):
        with self.lock:
            conexion = None
            estado = "ACTIVADO"
            try:
                conexion = self.get_connection()
                cursor = conexion.cursor()

                #inserta los valores del CP en la BD
                cursor.execute("INSERT OR REPLACE INTO puntos_recarga (cp_id, ubicacion, precio_kwh, estado) VALUES (?, ?, ?, ?)", (cp_id, ubicacion, precio_kwh, estado))

                conexion.commit()

                print(f"Se ha registrado un punto de carga: {cp_id}, {estado}")
            except sqlite3.Error as e:
                print(f"Error de SQLite al registrar CP: {e}")
            except Exception as e:
                print(f"Error inesperado al registrar CP: {e}")
            finally:
                if conexion:
                    conexion.close()

    def registrar_cp_token(self, cp_id, ubicacion, token, clave):
        with self.lock:
            conexion = None
            try:
                conexion = self.get_connection()
                cursor = conexion.cursor()
                cursor.execute("""INSERT INTO puntos_recarga (cp_id, ubicacion, precio_kwh, estado, token, clave_cifrado) 
                VALUES (?, ?, 0.50, 'DESCONECTADO', ?, ?)
                ON CONFLICT(cp_id) DO UPDATE SET 
                token=excluded.token,
                clave_cifrado=excluded.clave_cifrado,
                ubicacion=excluded.ubicacion""", (cp_id, ubicacion, token, clave))

                conexion.commit()
                print(f"Se ha registrado el Token para {cp_id} en la BD")
            except Exception as e:
                print(f"Error guardando token: {e}")
            finally:
                if conexion: 
                    conexion.close()

    def actualizar_estado_cp(self, cp_id, estado):
        with self.lock:
            conexion = None
            try:
                conexion = self.get_connection()
                cursor = conexion.cursor()
                
                #actualiza el estado del CP en la BD
                cursor.execute("UPDATE puntos_recarga SET estado = ? WHERE cp_id = ?", (estado, cp_id))
                
                conexion.commit()

                print(f"Se ha actualizado el estado de {cp_id} a {estado}")
            except sqlite3.Error as e:
                print(f"Error de SQLite al actualizar CP: {e}")
            except Exception as e:
                print(f"Error inesperado al actualizar CP: {e}")
            finally:
                if conexion:
                    conexion.close()

    def seleccionar_cp(self, cp_id): #devuelve (cp_id, ubicacion, estado, precio, token, clave)
        with self.lock:    
            conexion = None
            fila = None
            try:             
                conexion = self.get_connection()
                cursor = conexion.cursor()
                
                cursor.execute("SELECT cp_id, ubicacion, estado, precio_kwh, token, clave_cifrado FROM puntos_recarga WHERE cp_id = ?", (cp_id,))
                
                fila = cursor.fetchone()
            except sqlite3.Error as e:
                print(f"Error de SQLite al hacer select CP: {e}")
            except Exception as e:
                print(f"Error inesperado al hacer select CP: {e}")
            finally:
                if conexion:
                    conexion.close()
            return fila

    def seleccionar_conductor(self, driver_id):
        with self.lock:
            conexion = None
            fila = None
            try:
                conexion = self.get_connection()
                cursor = conexion.cursor()
                
                cursor.execute("SELECT driver_id, nombre FROM conductor WHERE driver_id = ?", (driver_id,))
                
                fila = cursor.fetchone()
            except sqlite3.Error as e:
                print(f"Error de SQLite al hacer select conductor: {e}")
            except Exception as e:
                print(f"Error inesperado al hacer select conductor: {e}")
            finally:
                if conexion:
                    conexion.close()
            return fila

    def registrar_carga(self, cp_id, driver_id, energia, coste):
        with self.lock:
            conexion = None
            try:
                conexion = self.get_connection()
                cursor = conexion.cursor()

                #inserta informacion sobre una carga en la BD
                cursor.execute("""INSERT INTO cargas (cp_id, driver_id, energia, coste) VALUES (?, ?, ?, ?)""", (cp_id, driver_id, energia, coste))
                
                conexion.commit()

                print(f"Se ha registrado una carga de suministro: {cp_id}, {driver_id}, {energia}, {coste}")
            except sqlite3.Error as e:
                print(f"Error de SQLite al registrar carga CP: {e}")
            except Exception as e:
                print(f"Error inesperado al registrar carga CP: {e}")
            finally:
                if conexion:
                    conexion.close()

    def seleccionar_todos_cps(self):
        with self.lock:
            conexion = None
            filas = []
            try:
                conexion = self.get_connection()
                cursor = conexion.cursor()
                
                cursor.execute("SELECT cp_id, ubicacion, estado, precio_kwh, token FROM puntos_recarga")
                
                filas = cursor.fetchall()
            except:
                pass
            finally:
                if conexion:
                    conexion.close()
            return filas

    def insertar_datos_prueba(self):
        
        cps_prueba = [
            ('ALC1', 'Calle-los-lirios', 0.5, 'DESCONECTADO'),
            ('ALC2', 'Calle-Politecnica', 0.75, 'DESCONECTADO'),
            ('CP001', 'Calle-SanVicente', 0.45, 'DESCONECTADO'), 
            ('CP002', 'Calle-LasMaromas', 0.85, 'DESCONECTADO'),
            ('CP003', 'Calle-Benimar', 0.90, 'DESCONECTADO')]
        
        conductores_prueba = [
            ('D001', 'Danil'),
            ('D002', 'Driver2'),
            ('D003', 'Driver3'),
            ('D004', 'Driver4'),
            ('D005', 'Driver5'),
            ('D006', 'Driver6'),
            ('D007', 'Driver7')]

        with self.lock:
            conexion = None
            try:
                conexion = self.get_connection()
                cursor = conexion.cursor()
                
                cursor.executemany("INSERT OR IGNORE INTO conductor (driver_id, nombre) VALUES (?, ?)", conductores_prueba)
                cursor.executemany("INSERT OR IGNORE INTO puntos_recarga (cp_id, ubicacion, precio_kwh, estado) VALUES (?, ?, ?, ?)", cps_prueba)
                
                conexion.commit()
                print(f"Puntos de carga y conductores de prueba insertados.")
                
            except sqlite3.Error as e:
                print(f"Error de SQLite al insertar datos de prueba: {e}")
            finally:
                if conexion:
                    conexion.close()


    def revocar_permisos_cp(self, cp_id):
        with self.lock:
            conexion = None
            try:
                conexion = self.get_connection()
                cursor = conexion.cursor()
                # ponemos token y clave a NULL con estado no autorizdo
                cursor.execute("UPDATE puntos_recarga SET token=NULL, clave_cifrado=NULL, estado='NO_AUTORIZADO' WHERE cp_id = ?", (cp_id,))
                conexion.commit()
                print(f"Permisos revocados para {cp_id} en BD.")
            except Exception as e:
                print(f"Error revocando permisos: {e}")
            finally:
                if conexion: conexion.close()

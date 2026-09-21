import socket
import sys
import threading
import time
import os
from cryptography.fernet import Fernet
from kafka import KafkaConsumer, KafkaProducer

FORMAT = "utf-8"

estado_lock = threading.Lock() #para que dos hilos no manipulen la varibale global a la vez
estado_cp = "OK" #inicialmente el estado es OK, puede ser KO SUMINISTRANDO PARADO ...

CLAVE_CIFRADO = None

#funciones para el cifrado de mensaje por KAFKA
def cifrar(mensaje_str, clave):
    if not clave: 
        return mensaje_str.encode()
    try:
        f = Fernet(clave)
        return f.encrypt(mensaje_str.encode())
    except Exception as e:
        print(f"Error al cifrar mensaje: {e}")
        return mensaje_str.encode()

def descifrar(mensaje_bytes, clave):
    if not clave: 
        return mensaje_bytes.decode()
    try:
        f = Fernet(clave)
        return f.decrypt(mensaje_bytes).decode()
    except Exception as e:
        print(f"Error al descifrar mensaje: {e}")
        return None


def set_estado(nuevo_estado):
    global estado_cp #para poder modificar la variable global
    with estado_lock:
        if estado_cp != nuevo_estado:
            estado_cp = nuevo_estado
            print(f"Estado cambiado a: {estado_cp}")

def get_estado():
    with estado_lock:
        return estado_cp

# ------ Funciones para manejar conexiones con central por Kafka, implementando securizacion------
def enviar_kafka_seguro(mensaje_plano, producer):
    if CLAVE_CIFRADO:
        try:
            contenido_cifrado = cifrar(mensaje_plano, CLAVE_CIFRADO).decode(FORMAT)
            mensaje_final = f"ENCRIPTADO,{cp_id},{contenido_cifrado}"
        except Exception as e:
            print(f"Error al cifrar mensaje kafka: {e}, enviando mensaje sin cifrar")
            mensaje_final = mensaje_plano
    else:
        print(f"Enviando sin cifrar porque no hay clave")
        mensaje_final = mensaje_plano
        
    producer.send('cp_central', value=mensaje_final.encode(FORMAT))
    producer.flush()

def kafka_engine_central(cp_id, broker):
    """Esta funcion maneja la conexion por kafka entre ENGINE y CENTRAL, engine espera alguna orden de central,
    como iniciar carga (START), parar o renaudar"""

    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                'central_cp',
                bootstrap_servers = [broker],
                group_id = f"engine-{cp_id}",
                auto_offset_reset="latest"
            )
    
            print("Consumidor Kafka conectado. Esperando ordenes desde central ...")

            for mensaje in consumer:
                try:
                    orden = mensaje.value.decode(FORMAT)
                    partes = orden.split(',')

                    contenido_descifrado = ""
                    if partes[0] == "ENCRIPTADO": #lo primero que hacemos al recibir un mensaje de central es descifrarlo
                        if len(partes) >= 3 and partes[1] == cp_id:
                            contenido_cifrado = partes[2]
                        
                            if CLAVE_CIFRADO:
                                contenido_descifrado = descifrar(contenido_cifrado.encode(), CLAVE_CIFRADO)
                                
                                if contenido_descifrado:
                                    print(f"Se ah descifrado la orden de Central: {contenido_descifrado}")
                                    partes = contenido_descifrado.split(',')
                                else:
                                    print("Error al descifrar orden de Central")
                                    continue
                            else:
                                print("Se ha recibido una orden cifrada pero Engine no tiene clave")
                                continue
                        else:
                            continue

                    #print(f"Se ha recibido la orden {contenido_descifrado}")

                    tipo_orden = partes[0]
                    if tipo_orden == "START":
                        if len(partes) >= 3:
                            if get_estado() == "OK": #si el CP esta Ok entonces se lanza un hilo que hace la simulacion de carga
                                driver_id = partes[1]
                                precio_kwh = partes[2]
                                cp_id_msg = partes[3]
                                threading.Thread(target=proceso_carga, args=(cp_id_msg, driver_id, precio_kwh, broker), daemon=True).start()
                            else:
                                print(f"Orden START recibida, pero CP está '{get_estado()}'")

                    elif tipo_orden == "PARAR_CP":
                        if len(partes) >= 2 and partes[1] == cp_id:
                            print(f"Orden de PARADA recibida de Central.")
                            set_estado("PARADO") # Ponemos el estado en PARADO
                    
                    elif tipo_orden == "REANUDAR_CP":
                        if len(partes) >= 2 and partes[1] == cp_id:
                            estado_actual = get_estado()
                            if estado_actual == "SUMINISTRANDO":
                                print(f"Orden de REANUDAR recibida de Central pero ignorada porque estamos SUMINISTRANDO.")
                            else:
                                print("Orden de REANUDAR recibida de CENTRAL")
                                set_estado("OK") # Vuelve a estar disponible
                except Exception as e:
                    print(f"Error procesando mensaje Kafka de CENTRAL: {e}")
        except Exception as e:
            print(f"Error en consumidor Kafka: {e}. Reiniciando en 5s...")
            if consumer:
                consumer.close() # Cierra el consumidor si existe
            time.sleep(5)

def proceso_carga(cp_id, driver_id, precio_kwh, broker):
    global CLAVE_CIFRADO

    precio_kwh = float(precio_kwh)

    if get_estado() != "OK": #comprobacion inicial de si esta activo el CP para realizar el proceso
        print(f"Carga para {driver_id} rechazada, CP no está OK.")
        return
    
    set_estado("SUMINISTRANDO")

    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=[broker])
    except Exception as e:
        print(f"Error al crear productor de Kafka: {e}")
        set_estado("OK") # Vuelve a OK si no se puede crear el producer
        return
    
    #simulacion de la carga
    energia_cargada = 0
    energia_completa = 20
    potencia_carga = 15

    print(f"\nIniciando carga para {driver_id} en {cp_id}...")

    try:
        while energia_cargada < energia_completa:
            estado_actual = get_estado()
            if estado_actual != "SUMINISTRANDO": #si de repente cambia el estado, notificamos a central
                print(f"\nCarga interrumpida. Estado: {estado_actual}!")
                try:
                    if estado_actual == "KO":
                        enviar_kafka_seguro(f"EVENT,{cp_id},{driver_id},AVERIADO", producer)
                    elif estado_actual == "PARADO":
                        enviar_kafka_seguro(f"EVENT,{cp_id},{driver_id},PARADO_CENTRAL", producer)
                except Exception as e:
                    print(f"Error al enviar EVENT de interrupción: {e}")
                return
            
            time.sleep(1)

            #si todo va bien seguimos con la simulacion
            energia_por_segundo = potencia_carga / 3600 
            energia_cargada += energia_por_segundo * 10 #Aceleracion de simulacion
            
            if energia_cargada > energia_completa:
                energia_cargada = energia_completa

            coste_actual = energia_cargada * precio_kwh
            progreso = (energia_cargada / energia_completa) * 100
            
            print(f"  Cargando... {progreso:5.1f}% | {energia_cargada:5.2f} kWh | {coste_actual:5.2f} €")

            #enviamos el estado a central
            mensaje_suministro = f"CARGANDO,{cp_id},{driver_id},{energia_cargada:.2f}kWh,{coste_actual:.2f}EUR"
            enviar_kafka_seguro(mensaje_suministro, producer)

        # termina la simulacion y calcula el coste final y enviamos el ticket
        coste_final = energia_cargada * precio_kwh
        print(f"\nCarga completada para {driver_id}")
        print(f"Coste total: {coste_final:.2f} €")

        mensaje_ticket = f"TICKET,{cp_id},{driver_id},{energia_cargada:.2f}kWh,{coste_final:.2f}EUR"
        enviar_kafka_seguro(mensaje_ticket, producer)
    
    except Exception as e:
        print(f"Error durante el proceso de carga: {e}")
        # Intentamos notificar a Central de un error
    finally:
        if producer:
            producer.close()
        if get_estado() == "SUMINISTRANDO":
            set_estado("OK") # Volvemos a estar disponibles

# ------ Funciones para manejar la comunicacion por sockets entre Monitor y Engine ------
def start_server(puerto, cp_id):
    global CLAVE_CIFRADO
    ADDR = ('127.0.0.1', puerto)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)    
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind(ADDR)
        server.listen()
    except OSError:
        print(f"Error la dirección {ADDR} ya esta en uso (alomejor hay otro engine corriendo)")
        os._exit(1)

    print(f"Esperando conexión de un Monitor en {ADDR}...")
    monitor_activo = None
    while True:
        conn = None
        try:
            conn, addr = server.accept()
            
            #comprobamos que no haya ya un monitor en esta conexion
            if monitor_activo:
                print(f"Rechazando un intento de conexion de otro monitor cuando ya hay uno en {ADDR}.")
                conn.close()
                continue
            
            monitor_activo = conn
            print(f"Se ha conectado un monitor en {ADDR}")

            #bulce para la comunicacion con el monitor que acaba de entrar
            while True:
                data = conn.recv(1024).decode(FORMAT)
                if not data:
                    break

                if data.startswith("SET_KEY"): #nuveo, para recibir la clave desde monitor
                    partes = data.split(',')
                    if len(partes) >= 2:
                        CLAVE_CIFRADO = partes[1]
                        print(f"Se ha recibido la clave de cifrado: {CLAVE_CIFRADO[:5]}")
                        conn.sendall(b"OK_KEY")
                    continue

                if data == "PING": #monitor quiere comprobar el estado
                    estado_actual = get_estado()
                    
                    respuesta = ""
                    if estado_actual == "KO":
                        respuesta = f"KO,{cp_id}"
                    else:
                        respuesta = f"OK,{cp_id}"
                    conn.sendall(respuesta.encode(FORMAT))
        except Exception as e:
            print(f"Error en la conexión con monitor: {e}")

        if monitor_activo:
            print("Limpiando la conexion con este monitor")
            monitor_activo.close()
            monitor_activo = None

if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Error: argumentos incorrectos")
        print("Uso: python3 EV_CP_E.py <broker_ip> <broker_puerto> <cp_id> <puerto>")
        sys.exit(1)

    broker_ip = sys.argv[1]
    broker_puerto = sys.argv[2]
    cp_id = sys.argv[3]
    puerto = int(sys.argv[4])

    broker = f"{broker_ip}:{broker_puerto}"
    
    #hilo para el servidor para el  monitor
    threading.Thread(target=start_server, args=(puerto, cp_id), daemon=True).start()
    
    #hilo para Consumidor de órdenes de Central
    threading.Thread(target=kafka_engine_central, args=(cp_id, broker), daemon=True).start()
    print(f"Iniciado ENGINE para {cp_id}")
    
    # hilo principal que controla el estado, podemos simular ko poniendo ko en terminal
    print("Escribe 'ko' para simular avería, 'ok' para restaurar.")
    try:
        while True:
            cmd = input("Simular AVERIA (ko) o ACTIVAR (ok) ").strip().lower()
            if cmd == "ko":
                set_estado("KO")
            elif cmd == "ok":
                # si no está en medio de una carga
                if get_estado() == "SUMINISTRANDO":
                    print("No se puede poner 'ok' mientras suministra. La carga debe finalizar.")
                else:
                    set_estado("OK")
            elif cmd == "":
                continue
            else:
                print("Comando no reconocido. Usa 'ok' o 'ko'.")
    except KeyboardInterrupt:
        print("\nFinalizando Engine por Ctrl+C...")
import sys
import os
import time
import threading
from kafka import KafkaProducer, KafkaConsumer

FORMAT = "utf-8"

# Evento para sincronizar el hilo principal (que pide) con el consumidor (que escucha)
suministro_terminado = threading.Event()

def peticion_suministro(productor, driver_id, cp_id):
    mensaje = f"REQUEST,{driver_id},{cp_id}"
    productor.send('driver_central', mensaje.encode(FORMAT))
    productor.flush()
    print(f"Solicitud de suministro enviada a central: {mensaje}")

def consumidor_driver(broker, driver_id):
    """Esta funcion maneja la conexion por kafka entre central y driver, esperando alguna respuesta de central
    de si el conductor esta autorizado, estado de carga, etc"""
    
    consumidor = None
    while True:
        try:
            consumidor = KafkaConsumer(
                'central_driver',
                bootstrap_servers=[broker],
                group_id=f"driver-{driver_id}",
                auto_offset_reset='latest'
            )
            print("Consumidor kafka esperando respuesta de CENTRAL...")
            
            for mensaje in consumidor:
                try:
                    texto = mensaje.value.decode(FORMAT)
                    partes = texto.split(',')

                    # solo queremos mensajes para este driver
                    if len(partes) < 3 or partes[1] != driver_id:
                        continue
                    
                    tipo_mensaje = partes[0]
                    cp_id_msg = partes[2]

                    if tipo_mensaje == "AUTORIZADO":
                        print(f"AUTORIZADO: Carga en {cp_id_msg} puede comenzar.")
                    
                    elif tipo_mensaje == "DENEGADO":
                        causa = partes[3] if len(partes) > 3 else "Desconocida"
                        print(f"DENEGADO: Carga en {cp_id_msg}. Causa: {causa}")
                        suministro_terminado.set() # Desbloquea el hilo principal

                    elif tipo_mensaje == "CARGANDO_DRIVER":  #informacion desde central de como va la carga una vez autorizado
                        kwh = partes[3]
                        coste = partes[4]
                        print(f"CARGANDO {driver_id} {cp_id_msg}: {kwh}, {coste}")

                    elif tipo_mensaje == "TICKET":
                        print(f"TICKET FINAL {cp_id_msg}: {', '.join(partes[3:])}")
                        suministro_terminado.set()
                    
                    elif tipo_mensaje == "ERROR_AVERIA" or tipo_mensaje == "ERROR_INTERRUMPIDO":
                        causa = partes[3] if len(partes) > 3 else "Desconocida"
                        print(f"ERROR: Carga en {cp_id_msg} interrumpida. Causa: {causa}")
                        suministro_terminado.set()

                except Exception as e:
                    print(f"Error procesando mensaje de Kafka: {e}")
        except Exception as e:
            print(f"Error en consumidor Kafka: {e}. Reiniciando...")
            if consumidor:
                consumidor.close()
            time.sleep(1)

def procesar_cp_manual(productor, driver_id):
    try:
        while True:
            print("Introduce ID del punto de carga o 'q' para salir: ")
            cp_id = input().strip()
            if not cp_id:
                continue
            if cp_id.lower() == 'q':
                break

            suministro_terminado.clear() # Resetea el evento
            peticion_suministro(productor, driver_id, cp_id)
            suministro_terminado.wait()

            print("Esperando 4s para una posible nueva carga...\n")
            time.sleep(4)
    except KeyboardInterrupt:
        print("\nSaliendo del modo manual...")

def procesar_cp_fichero(productor, driver_id, broker, fichero):
    try:
        with open(fichero, 'r') as f:
            peticiones = [linea.strip() for linea in f if linea.strip()]
            
    except FileNotFoundError:
        print(f"Error: No se encontró el fichero {fichero}")
        return
    except Exception as e:
        print(f"Error al leer el fichero: {e}")
        return
    
    print(f"Servicios a procesar del fichero: {peticiones}")
    
    try:
        for cp_id in peticiones:
            print(f"--- Procesando petición para {cp_id} ---")

            suministro_terminado.clear() # Resetea el evento antes de la petición
            peticion_suministro(productor, driver_id, cp_id)
            suministro_terminado.wait()

            print(f"--- Servicio en {cp_id} finalizado. Esperando 4 segundos... ---")
            time.sleep(4) 
    except KeyboardInterrupt:
        print("\nProcesamiento de fichero interrumpido.")

    print("--- Fichero de servicios completado. ---")

if __name__ == "__main__":
    broker_ip = os.environ.get("BROKER_HOST", "")
    broker_puerto = os.environ.get("BROKER_PORT", "")
    driver_id = os.environ.get("DRIVER_ID", "")

    # compatibilidad: si no hay env vars, leer de sys.argv
    if not broker_ip and len(sys.argv) >= 4:
        broker_ip = sys.argv[1]
        broker_puerto = sys.argv[2]
        driver_id = sys.argv[3]
    elif not broker_ip:
        print("Error: Argumentos incorrectos")
        print("Uso: python3 EV_Driver.py <ip_broker> <puerto_broker> <driver_id> [fichero_servicios]")
        print("  o configurar: BROKER_HOST, BROKER_PORT, DRIVER_ID")
        sys.exit(1)

    broker = f'{broker_ip}:{broker_puerto}'

    try:
        productor = KafkaProducer(bootstrap_servers=[broker])
    except Exception as e:
        print(f"Error al conectar productor Kafka: {e}")
        sys.exit(1)

    # Inicia el hilo consumidor
    threading.Thread(target=consumidor_driver, args=(broker, driver_id), daemon=True).start()

    print("Estableciendo conexión con Kafka...")
    time.sleep(2)

    try:
        fichero = os.environ.get("SERVICE_FILE", "")
        if not fichero and len(sys.argv) == 5:
            fichero = sys.argv[4]
        
        if fichero:
            print(f"Iniciando peticiones de fichero {fichero} para {driver_id}")
            procesar_cp_fichero(productor, driver_id, broker, fichero)
        else:
            print(f"Iniciando peticiones manuales para {driver_id}")
            procesar_cp_manual(productor, driver_id)
    except Exception as e:
        print(f"Error inesperado en el hilo principal: {e}")

    print("Finalizando aplicación del conductor.")
    productor.close()
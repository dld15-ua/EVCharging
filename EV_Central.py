import ssl
import os
import sys
import socket
import threading
from database_manager import DatabaseManager
import time
from kafka import KafkaConsumer, KafkaProducer
from cryptography.fernet import Fernet
from flask import Flask, jsonify, request, render_template
import datetime

DB_FILE = "evcharging.db" #fichero de la BD
db = DatabaseManager("evcharging.db")

historial_logs = [] # para guardar los logs importantes, con la funcion auditar evento, y poder mostrarlos en el front
lock_frontend = threading.Lock()

# funcion para la auditorias, para eventos importantes como registro de un monitor, averia, alerta de clima
def auditar_evento(origen, accion, descripcion):
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mensaje_log = f"[{fecha}] ORIGEN: {origen} | ACCION: {accion} | DESC: {descripcion}"
    print(f"AUDITORIA: {mensaje_log}")
    
    with lock_frontend:
        historial_logs.insert(0, mensaje_log) #ponemos el log mas nuevo arriba
        if len(historial_logs) > 20: #como maximo 20 logs, para ir mostrando los mas importantes en la web
            historial_logs.pop()

#para guardar,consumo en Kw, importe en € y Id del conductor y mostrar cuando se esta cargando suministro y no tener que preguntara a la BD
cp_datos_panel = {}
cp_datos_panel_lock = threading.Lock()

# --- Constantes para empaquetado de mensajes ---
STX = bytes([0x02])
ETX = bytes([0x03])
ACK = bytes([0X06])
NACK = bytes([0X15])

def calcular_lrc(data): #calcula la suma de control del paquete
    lrc = 0
    for b in data:
        lrc ^= b
    return lrc

FORMAT = "utf-8" #para encode y decode

#funciones para el cifrado de mensajes entre CP y CENTRAL
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
        return bytes(mensaje_bytes.decode(FORMAT))
    try:
        f = Fernet(clave)
        return f.decrypt(bytes(mensaje_bytes)).decode(FORMAT)
    except Exception as e:
        print(f"Error al descifrar mensaje: {e}")
        return None

#para el envio cifrado de los mensajes con kafka entre engine y central
def enviar_orden_engine(producer, cp_id, orden):
    try:
        info_cp = db.seleccionar_cp(cp_id)
        if not info_cp or not info_cp[5]:
            print(f"No se puede enviar orden a {cp_id}: Falta clave en BD.")
            return

        clave = info_cp[5]
        
        mensaje_plano = f"{orden},{cp_id}" 
        
        mensaje_cifrado = cifrar(mensaje_plano, clave).decode(FORMAT) #ciframos el mensaje
        mensaje_final = f"ENCRIPTADO,{cp_id},{mensaje_cifrado}" #menasje que se va a enviar
        
        producer.send('central_cp', mensaje_final.encode(FORMAT))
        producer.flush()
        print(f"Se ha enviado la orden cifrada a {cp_id}: {orden}")

    except Exception as e:
        print(f"Error enviando orden a Engine: {e}")

# --------------  Funcion para la comunicacion Kafa (CENTRAL-DRIVER) --------------
def kafka_central_driver(broker, producer):
    """Esta funcion maneja la comunicaion por kafka entre central y driver, recibe REQUEST de drivers, le responde, 
    y si todo va bien autoriza la carga mandando la orden START a ENGINE """

    try:
        consumidor = KafkaConsumer(
            'driver_central',
            bootstrap_servers = [broker],
            auto_offset_reset = 'latest',
            group_id = 'central-group-driver'
        )
        print("Iniciado consumidor KAFKA CENTRAL-DRIVER")
    except Exception as e:
        print(f"Error al iniciar consumidor KAFKA en central_driver: {e}")
        return
        
    for mensaje in consumidor:
        try:
            datos_str = mensaje.value.decode(FORMAT)
            partes = datos_str.split(',')

            if len(partes) != 3 or partes[0] != "REQUEST":
                print("Se ha recibido una petición del driver inválida")
                continue
        
            driver_id = partes[1]
            cp_id = partes[2]
            print(f"Petición recibida de {driver_id} para carga en {cp_id}")

            #valida si el conductor esta en la BD
            conductor = db.seleccionar_conductor(driver_id)
            if not conductor:
                print(f"DENEGADO: Conductor '{driver_id}' no registrado.")
                producer.send('central_driver', f"DENEGADO,{driver_id},{cp_id},CONDUCTOR_DESCONOCIDO".encode(FORMAT))
                producer.flush()
                continue

            #validar el cp_id recibido
            fila = db.seleccionar_cp(cp_id) #devuelve una tupla (cp_id, ubicacion, estado, precio)
            if not fila:
                producer.send('central_driver', f"DENEGADO,{driver_id},{cp_id},INEXISTENTE".encode(FORMAT))
                producer.flush()
                continue

            cp_id = fila[0]
            ubicacion = fila[1]
            estado_actual = fila[2]
            precio_kwh = fila[3]
            clave_cp = fila[5]

            #para validar si hay clave, si por ejemplo se revocan las claves se envia un fallo al driver para cortar el suministro
            if not clave_cp:
                print(f"Denegada la carga a {driver_id} en {cp_id}: Credenciales CP revocadas.")
                producer.send('central_driver', f"DENEGADO,{driver_id},{cp_id},FALLO_SEGURIDAD_CP".encode(FORMAT))
                producer.flush()
                continu

            if estado_actual != "ACTIVADO":
                print(f"Denegada la carga a {driver_id} en {cp_id}, Estado: {estado_actual}")
                producer.send('central_driver', f"DENEGADO,{driver_id},{cp_id},{estado_actual}".encode(FORMAT))
                producer.flush()
                continue
            
            # si esta todo bien autorizamos la carga
            print(f"Autorizando a {driver_id} en {cp_id}")
            
            #avisar al driver de que ha sido autorizado
            producer.send('central_driver', f"AUTORIZADO,{driver_id},{cp_id}".encode(FORMAT))
            
            #mensaje START para el engine, cifrado
            contenido_orden = f"START,{driver_id},{precio_kwh}"
            enviar_orden_engine(producer, cp_id, contenido_orden)
        except Exception as e:
            print(f"Error al procesar mensaje {datos_str} en kafka_central_driver: {e}")

# --------------  Funcion para la comunicacion Kafa entre CENTRAL y ENGINE --------------
def kafka_central_cp(broker, producer):
    """Esta funcion maneja la conexion por kafka entre la central y el engine, para el funcionamiento del suministro de carga.
    Espera que engine le mande mensajes del estado de carga (CARGANDO, TICKER, o EVENT (que puede ser de AVERIA o de que se ha PARADO cp por la central))"""
    
    try:
        consumidor = KafkaConsumer(
            'cp_central',
            bootstrap_servers=[broker],
            auto_offset_reset='latest',
            group_id='central-group-cp'
        )
        print("Iniciado consumidor KAFA cp-central")
    except Exception as e:
        print(f"Error al iniciar consumidor KAFKA cp_central: {e}")
        return

    for mensaje in consumidor:
        try:
            datos_str = mensaje.value.decode(FORMAT)
            partes = datos_str.split(',')

            # lo primero qeu hacemos cuando llega un mensaje es descifrarlo
            if partes[0] == "ENCRIPTADO":
                if len(partes) >= 3:
                    cp_origen = partes[1]
                    contenido_cifrado = partes[2]
                    
                    info_cp = db.seleccionar_cp(cp_origen) #buscamos la clave del CP en espeficio en la BD
                    
                    if info_cp and info_cp[5]: #si tiene clave
                        clave = info_cp[5]
                        texto_descifrado = descifrar(contenido_cifrado.encode(), clave)
                        
                        if texto_descifrado:
                            print(f"Mensaje Kafka descifrado de {cp_origen}: {texto_descifrado}")

                            datos_str = texto_descifrado
                            partes = datos_str.split(',') 
                        else:
                            print(f"Error al descifrar mensaje de {cp_origen}")
                            continue
                    else: #si se revoca las claves durante la carga
                        print(f"Se ha recibido un mensaje de {cp_origen} pero NO hay claves en la BD.")
                        
                        # es este caso de envia orden de parada al cp
                        msg_emergencia = f"PARAR_CP,{cp_origen}"
                        producer.send('central_cp', msg_emergencia.encode(FORMAT))
                        producer.flush()

                        with cp_datos_panel_lock:
                            if cp_origen in cp_datos_panel:
                                driver_afectado = cp_datos_panel[cp_origen]['driver_id']
                                msg_driver = f"ERROR_INTERRUMPIDO,{driver_afectado},{cp_origen},FALLO_SEGURIDAD_REVOCADO"
                                producer.send('central_driver', msg_driver.encode(FORMAT))
                                producer.flush()
                                del cp_datos_panel[cp_origen]
                        
                        continue

            tipo_mensaje = partes[0]

            if tipo_mensaje == "CARGANDO":
                if len(partes) >= 5: #el mensaje esperado durante el suministro de carga es "CARGANDO,{cp_id},{driver_id},{energia_cargada:.2f}kWh,{coste_actual:.2f}EUR"
                    cp_id = partes[1]
                    driver_id = partes[2]
                    kwh = partes[3]
                    coste = partes[4]
                    
                    fila_db = db.seleccionar_cp(cp_id)
                    if fila_db:
                        estado_actual_db = fila_db[2]
                    else:
                        estado_actual_db = fila_db[2]

                    if estado_actual_db == "DESCONECTADO":
                        print("Se ha recibido una orden de carga pero el MONITOR se ha desconectado, terminando carga...")

                        enviar_orden_engine(producer, cp_id, "PARAR_CP")
                    
                        msg_error = f"ERROR_INTERRUMPIDO,{driver_id},{cp_id},FALLO_MONITOR" #y avisamos al driver
                        kafka_producer.send('central_driver', msg_error.encode(FORMAT))
                        
                        with cp_datos_panel_lock:
                            if cp_id in cp_datos_panel:
                                del cp_datos_panel[cp_id]

                        continue
                    
                    if estado_actual_db != "PARADO":
                        db.actualizar_estado_cp(cp_id, "SUMINISTRANDO") #si engine envia cargando y esta todo correcto, estado SUMINISTRANDO energia

                    #guardamos los datos para mostrarlos en tiempo real
                    with cp_datos_panel_lock:
                        cp_datos_panel[cp_id] = {"kwh": kwh, "coste": coste, "driver_id": driver_id}

                    # avisamos al driver que estamos cargando
                    datos_cargando = ','.join(partes[3:]) #esto crea une una lista de palabras de una lista usando la coma como separador
                    mensaje_driver = f"CARGANDO_DRIVER,{driver_id},{cp_id},{datos_cargando}"
                    producer.send('central_driver', mensaje_driver.encode(FORMAT))
                    producer.flush()
                else:
                    print("Mensaje CARGANDO mal formado:", datos_str)

            elif tipo_mensaje == "TICKET":
                if len(partes) >= 5:
                    cp_id = partes[1]
                    driver_id = partes[2]
                    energia = float(partes[3].replace("kWh", ""))
                    coste = float(partes[4].replace("EUR", ""))
                    
                    db.registrar_carga(cp_id, driver_id, energia, coste)

                    fila_db = db.seleccionar_cp(cp_id)
                    if fila_db and fila_db[2] != "PARADO":
                        db.actualizar_estado_cp(cp_id, "ACTIVADO") #volvemos a activar el cp cuando ha termiado la carga

                    #limpiamos los datos en tiempo real
                    with cp_datos_panel_lock:
                        if cp_id in cp_datos_panel:
                            del cp_datos_panel[cp_id]
                    
                    # reenviar ticket al driver
                    mensaje_driver = f"TICKET,{driver_id},{cp_id},{energia:.2f}kWh,{coste:.2f}EUR"
                    producer.send('central_driver', mensaje_driver.encode(FORMAT))
                    producer.flush()
                else:
                    print("Mensaje TICKET mal formado:", datos_str)

            elif tipo_mensaje == "EVENT": #eventos que vienen del engine
                if len(partes) < 4:
                    print("Mensaje EVENT mal formado:", datos_str)
                    continue

                cp_id = partes[1]
                driver_id = partes[2]
                tipo_evento = partes[3]

                print(f"Se ha recibido Evento de Engine: {tipo_evento} en {cp_id} para {driver_id}")
                auditar_evento(f"CP_{cp_id}", "FALLO_ENGINE", f"El engine ha reportado: {tipo_evento}")

                # actualizamos estado en BD
                if tipo_evento == "AVERIADO":
                    db.actualizar_estado_cp(cp_id, "AVERIADO")
                
                driver_afectado = None
                with cp_datos_panel_lock:
                    if cp_id in cp_datos_panel:
                        driver_afectado = cp_datos_panel[cp_id]['driver_id']
                        del cp_datos_panel[cp_id]

                if not driver_id and driver_afectado:
                    driver_id = driver_afectado

                # notifica al dirver de la averia
                print(f"Notificando a {driver_id} de la interrupción en {cp_id}...")
                mensaje_driver = f"ERROR_INTERRUMPIDO,{driver_id},{cp_id},{tipo_evento}"
                producer.send('central_driver', mensaje_driver.encode(FORMAT))
                producer.flush()

        except Exception as e:
            print(f"Error en kafka_central_cp al procesar '{datos_str}': {e}")

# -------------- Funciones para manejar la comunicacion con monitores --------------
def handle_client(conn, addr, producer):
    """Esta funcion maneja la conexion entre central y monitor, central espera un mensaje AUT, y este le verifica con un ACK si es correcto.
    Tambien maneja el mensaje STAT de monitor que es el estado del CP y EVENT del monitor que es si el CP sufre una averia"""

    buffer = bytearray()
    cp_id_conectado = None
    clave_cifrado = None
    try:
        conn.settimeout(5.0) #si cualquier funcion que se queda esperando tarda mas de 5s lanza una excepcion

        conectado = True
        while conectado:
            mensaje_empaquetado = conn.recv(1024)
            if not mensaje_empaquetado:
                conectado = False
                break

            buffer.extend(mensaje_empaquetado) #metemos los bytes recibidos al buffer

            while True: #proceso todos los mensajes completos que se encuentre
                stx_pos = buffer.find(STX)
                etx_pos = buffer.find(ETX, stx_pos)

                if stx_pos == -1 or etx_pos == -1: #si find no encontro algunos de los bytes del protocolo de empaquetado
                    break

                if len(buffer) < etx_pos+2: #por si lrc no ha llegado
                    break
                
                mensaje_bytes = buffer[stx_pos + 1 : etx_pos] #quitamos stx y ext-lrc para decficrar el mensaje en si
                
                lrc_recibido = buffer[etx_pos + 1] 
                if calcular_lrc(mensaje_bytes) != lrc_recibido: #valida el lrc (suma de control) recibida
                    print(f"Error de LRC, enviando NACK")
                    conn.sendall(NACK)
                    buffer = buffer[etx_pos + 2:]
                    continue

                if cp_id_conectado: # solo si ya esta conectado, comprobamos las credenciales
                    check_cp = db.seleccionar_cp(cp_id_conectado)
                    
                    if not check_cp or not check_cp[4] or not check_cp[5]:
                        print(f"Credenciales revocadas para {cp_id_conectado}. Cortando conexión.")
                        conectado = False 
                        break # desconectamos el monitor
                    
                    clave_cifrado = check_cp[5] #actualiza la clave

                mensaje_str = ""
                if clave_cifrado: # desficrar el mensaje de monitor
                    descifrado = descifrar(mensaje_bytes, clave_cifrado)
                    if descifrado:
                        mensaje_str = descifrado
                        print(f"Se ha descifrado el mensaje: {mensaje_str}")
                    else:
                        print("No se ha podido descifrar el mensaje.")
                        buffer = buffer[etx_pos + 2:] #limpiamos el buffer
                        continue
                else: # si no tenemos clave puede que sea porque es la primera vez que se conecta MONITOR
                    mensaje_str = mensaje_bytes.decode(FORMAT)

                partes = mensaje_str.split(',')
                tipo_mensaje = partes[0]

                if tipo_mensaje == "AUT": #se ha recibito una peticion de autenticacion de MONITOR, ahora autenticamos si se registrado en registry
                    if len(partes) < 3:
                        print(f"Mensaje AUT mal formado: {mensaje_str}")
                        conn.sendall(NACK)
                        conectado = False
                    else:
                        cp_id = partes[1]
                        token_recibido = partes[2]

                        fila_db = db.seleccionar_cp(cp_id)
                        if fila_db and fila_db[4] == token_recibido: # en la posicion 4 de la tupla esta el token generado
                            conn.sendall(ACK) #si todo es correcto y existe le enviamos confirmacion ack al monitor
                            cp_id_conectado = cp_id
                            clave_cifrado = fila_db[5]
                            print(f"Monitor para {cp_id} autenticado con socket seguros. Token validado")
                            auditar_evento(f"MONITOR_{cp_id}", "CONEXION", "Monitor registrado con token")

                            estado_actual_db = fila_db[2]

                            #sincronizamos el engine
                            if estado_actual_db == "PARADO":
                                if producer:
                                    print(f"Enviando orden de PARADA a {cp_id_conectado}")
                                    enviar_orden_engine(producer, cp_id_conectado, "PARAR_CP")
                            elif estado_actual_db in ["DESCONECTADO", "AVERIADO", "NO_AUTORIZADO"] :
                                db.actualizar_estado_cp(cp_id, "ACTIVADO")
                                if producer:
                                    print(f"Enviando orden de REANUDAR a {cp_id}")
                                    enviar_orden_engine(producer, cp_id_conectado, "REANUDAR_CP")
                        else:
                            conn.sendall(NACK)
                            print(f"Monitor de {cp_id} se intentó conectar, pero no existe en la BD o el token es incorrecto.")
                            conectado = False

                elif tipo_mensaje == "STAT":
                    conn.sendall(ACK)
                    if len(partes) >= 3:
                        cp_id = partes[1]
                        estado_monitor = partes[2]
                        
                        fila_db = db.seleccionar_cp(cp_id)
                        estado_actual_db = fila_db[2]

                        if estado_monitor == "OK":
                            if estado_actual_db == "PARADO":
                                if producer:
                                    # le decimos al Engine que se pare (por si se ha encendido y apagado)
                                    enviar_orden_engine(producer, cp_id_conectado, "PARAR_CP")

                            elif estado_actual_db == "DESCONECTADO" or estado_actual_db == "AVERIADO":
                                db.actualizar_estado_cp(cp_id, "ACTIVADO")
                                #sincronizamos el engine
                                if producer:
                                    enviar_orden_engine(producer, cp_id_conectado, "REANUDAR_CP")
                                auditar_evento(f"MONITOR_{cp_id_conectado}", "ENGINE_CONECTADO", f"Se ha conectado el ENGINE")
                        else: #ko
                            db.actualizar_estado_cp(cp_id, "AVERIADO")

                elif tipo_mensaje == "EVENT":
                    try:
                        conn.sendall(ACK)
                    except:
                        pass

                    if len(partes) >= 3:
                        cp_id = partes[1]
                        evento = partes[2]
                        print(f"Se ha recibido un evento de {cp_id}: {evento}")
                        auditar_evento(f"MONITOR_{cp_id}", "EVENTO", "Se ha recibido una AVERIA")

                        fila_db = db.seleccionar_cp(cp_id)
                        estado_actual_db = fila_db[2]

                        if evento == "AVERIADO":
                            if estado_actual_db == "PARADO":
                                print("Ignorando cambio a averiado porque se ha parado manualmente")
                            else:
                                db.actualizar_estado_cp(cp_id, "AVERIADO")

                                #solo intenta avisar al driver si hay una averia de verdad            
                                try:
                                    driver_afectado = None
                                    with cp_datos_panel_lock:
                                        if cp_id in cp_datos_panel:
                                            driver_afectado = cp_datos_panel[cp_id]['driver_id']
                                            del cp_datos_panel[cp_id] 

                                    if driver_afectado and kafka_producer:
                                        print(f"Notificando interrupción a driver {driver_afectado}")
                                        msg = f"ERROR_AVERIA,{driver_afectado},{cp_id},FALLO_ENGINE"
                                        kafka_producer.send('central_driver', msg.encode(FORMAT))
                                        kafka_producer.flush()
                                    else:
                                        print("No se encontró driver activo para notificar.")
                                except Exception as e:
                                    print(f"Error: No se pudo avisar al driver: {e}")
                else:
                    print(f"Tipo de mensaje del MONITOR desconocido: {tipo_mensaje}")
                    conn.sendall(ACK) #si todo es correcto y existe le enviamos confirmacion ack al monitor

                buffer = buffer[etx_pos + 2:] #limpia el buffer

    except socket.timeout:
        print(f"Se perdio la conexion con monitor: {cp_id_conectado}")
    except Exception as e:
        print(f"Se perdio la conexion con monitor {addr}: {e}")
    finally: #si monitor termina por alguna razon, se pone a deconectado (si no estaba parado manualmente o por clima)
        if cp_id_conectado:
            print(f"Monitor {cp_id_conectado} se ha desconectado.")
            auditar_evento(f"MONITOR_{cp_id_conectado}", "DESCONEXION", f"Se ha perdido la conexion con monitor")

            fila_db = db.seleccionar_cp(cp_id_conectado)
            if fila_db:
                estado_actual = fila_db[2]
                if estado_actual in ["PARADO", "NO_AUTORIZADO"]:
                    print(f"Manteniendo estado: {estado_actual}")
                else:
                    db.actualizar_estado_cp(cp_id_conectado, "DESCONECTADO")
            
            with cp_datos_panel_lock:
                if cp_id_conectado in cp_datos_panel:
                    if estado_actual == "NO_AUTORIZADO":
                        pass
                    else:
                        del cp_datos_panel[cp_id_conectado]
        else:
            print(f"Monitor {addr} se desconectó sin autenticarse.")
        conn.close()

def start_server(puerto, producer):
    contexto_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        contexto_ssl.load_cert_chain(certfile='server_cert.pem', keyfile='server_key.pem')
    except Exception as e:
        print(f"Error cargando certificados SSL: {e}")
        return # si no hay certificados central no arranca

    server = None
    try:
        #crea el socket
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #permite reusar el puerto si esta en timeout

        server.bind(('0.0.0.0', puerto))
        server.listen()
        server.settimeout(1.0)  # 1 segundo de espera para accept()
        print(f"Servidor CENTRAL escuchando en 0.0.0.0:{puerto}\n")

        while True:
            try:
                conn, addr = server.accept() #se conecta un monitor

                conn_ssl = contexto_ssl.wrap_socket(conn, server_side=True) #convierte el socket normal en seguro
                
                print(f"Monitor conectado: {addr}")
                
                threading.Thread(target=handle_client, args=(conn_ssl, addr, producer), daemon=True).start()
            except socket.timeout:
                continue #no llego ninguna conexion pero volvemos a intenatr
            except KeyboardInterrupt:
                print("Saliendo por Ctlr+C...")
                break
            except Exception as e:
                print(f"Error al aceptar la conexion con el monitor: {e}")

    except KeyboardInterrupt:
        print("Deteniendo servidor CENTRAL por Ctrl+C")
    finally:
        if server:
            server.close()

# --- Configuracion deL API Rest de CENTRAL para la comunicacion con el modulo de weather y el front ---
app = Flask(__name__)

#ruta para el front
@app.route('/')
def home():
    return render_template('index.html')

# ruta para gurdar los datos que le llegan del registry en la bd
@app.route('/api/internal/registro-cp', methods=['POST'])
def registro_interno_cp():
    try:
        datos = request.json
        cp_id = datos.get('cp_id')
        ubi = datos.get('ubicacion')
        token = datos.get('token')
        clave = datos.get('clave_cifrado')
        
        print(f"Se ha recibido peticion de Registry para {cp_id}")
        
        db.registrar_cp_token(cp_id, ubi, token, clave)
        
        return jsonify({"status": "OK"}), 201
    except Exception as e:
        print(f"Error en registro interno: {e}")
        return jsonify({"error": str(e)}), 500

#ruta para consultar el estado de todos los CP
@app.route('/cps', methods=['GET'])
def listar_cps():
    try:
        filas = db.seleccionar_todos_cps() 
        lista = []
        for fila in filas:
            cp_id = fila[0]
            estado = fila[2]
            
            info_extra = {}
            if estado == "SUMINISTRANDO":
                with cp_datos_panel_lock:
                    info_extra = cp_datos_panel.get(cp_id, {})

            lista.append({
                "cp_id": cp_id,
                "ubicacion": fila[1],
                "estado": estado,
                "precio": fila[3],
                "token": fila[4],
                "info_carga": info_extra
            })
        return jsonify(lista), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#ruta para recibir la info de la api de clima
@app.route('/api/weather', methods=['POST'])
def recibir_alerta_clima():
    try:
        datos = request.json
        cp_id = datos.get('cp_id')
        estado_clima = datos.get('estado_clima')

        ip_origen = request.remote_addr #origen para la auditoria
        auditar_evento(f"EV_W ({ip_origen})", "CAMBIO_CLIMA", f"CP: {cp_id}, Estado: {estado_clima}")

        #miramos en la base de datos, para actualizar el estado correctamente
        fila = db.seleccionar_cp(cp_id)
        if not fila or not fila[4]:
            return jsonify({"error": "CP no registrado"}), 404
            
        estado_actual = fila[2] 

        if estado_actual == "DESCONECTADO":
            print(f"Alerta de clima rechazada para {cp_id} por estar DESCONECTADO.")
            return jsonify({"error": "CP desconectado temporalmente"}), 503 #para que el modulo de clima lo siga intentado, hasta que no este desconectado

        if estado_clima == "alerta":
            if estado_actual in ["ACTIVADO", "SUMINISTRANDO", "AVERIADO"]: # en estos casos el cp se parara
                print(f"Alerta de clima en {cp_id}. Parando CP...")
                db.actualizar_estado_cp(cp_id, "PARADO")
                enviar_orden_engine(kafka_producer, cp_id, "PARAR_CP")
            else:
                print(f"Alerta de clima recibida para {cp_id}, pero se mantiene estado: {estado_actual}") # en caso de a ver revocado las claves no, porque si luego le doy reanudar cp no le veo el sentido
            
        elif estado_clima == "normal":
            if estado_actual == "PARADO": #solo reactivamos el cp si estaba parado
                print(f"Clima normalizado en {cp_id}. Reanudando CP...")
                db.actualizar_estado_cp(cp_id, "ACTIVADO")
                enviar_orden_engine(kafka_producer, cp_id, "REANUDAR_CP")
            else:
                print(f"Clima normal en {cp_id}, pero no se reactiva porque su estado es: {estado_actual}")

        return jsonify({"status": "OK", "msg": f"Estado {estado_clima}"}), 200

    except Exception as e:
        print(f"Error procesando alerta clima: {e}")
        return jsonify({"error": str(e)}), 500

# para el panel de logs en front
@app.route('/api/logs', methods=['GET'])
def obtener_logs():
    with lock_frontend:
        return jsonify(historial_logs)

# boton de parar en el front
@app.route('/cps/<cp_id>/parar', methods=['POST'])
def parar_cp(cp_id):
    try:
        cp_id = cp_id.upper()
        db.actualizar_estado_cp(cp_id, "PARADO")
        enviar_orden_engine(kafka_producer, cp_id, "PARAR_CP")
        return jsonify({"status": "OK", "msg": f"Orden de PARADA enviada a {cp_id}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# boton de parar en el front
@app.route('/cps/<cp_id>/reanudar', methods=['POST'])
def reanudar_cp(cp_id):
    try:
        cp_id = cp_id.upper()

        info = db.seleccionar_cp(cp_id)

        if not info or not info[4] or not info[5]:
            print(f"Se ha intentado reanudar {cp_id} sin credenciales.")
            return jsonify({"error": "DENEGADO: CP no tiene credenciales, hay que autenticarse otra vezdesde el Monitor."}), 403

        db.actualizar_estado_cp(cp_id, "ACTIVADO")
        enviar_orden_engine(kafka_producer, cp_id, "REANUDAR_CP")
        return jsonify({"status": "OK", "msg": f"Orden de REANUDAR enviada a {cp_id}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/cps/<cp_id>/revocar', methods=['POST'])
def revocar_credenciales(cp_id): # implementacion de la funcionalidad de revocar credenciales del cp
    try:
        cp_id = cp_id.upper()
        db.revocar_permisos_cp(cp_id)
        
        auditar_evento("CENTRAL", "SEGURIDAD", f"Credenciales revocadas para {cp_id}")
        
        return jsonify({"status": "OK", "msg": f"Credenciales de {cp_id} revocadas. CP fuera de servicio."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Error: argumentos incorrectos")
        print("Uso: python3 EV_CP_Central.py <puerto_escucha> <broker_ip> <broker_puerto>")
        sys.exit(1)

    puerto = int(sys.argv[1])
    broker_ip = sys.argv[2]
    broker_puerto = sys.argv[3]
    
    broker = f"{broker_ip}:{broker_puerto}"
    
    #creamos las tablas, y ponemos los puntos de recarga desconectado hasta que se conecte un monitor
    db.crear_tablas()
    db.insertar_datos_prueba()
    db.desconectar_cps()

    # creamos el productor
    try:
        kafka_producer = KafkaProducer(bootstrap_servers=[broker])
    except Exception as e:
        print(f"Error al crear Kafka Producer: {e}")
        sys.exit(1)

    # hilo para la escucha de peticiones de drivers
    threading.Thread(target=kafka_central_driver, args=(broker, kafka_producer), daemon=True).start()
    
    # hilo para la escucha estado de los engines
    threading.Thread(target=kafka_central_cp, args=(broker, kafka_producer), daemon=True).start()
    
    # hilo para el servidor para monitor
    threading.Thread(target=start_server, args=(puerto, kafka_producer), daemon=True).start()
    
    contexto_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        contexto_ssl.load_cert_chain('server_cert.pem', 'server_key.pem')
        print("Se han cargado correctamente los certificados SSL para la API CENTRAL.")
    except Exception as e:
        print(f"Error al cargar certificados para la API CENTRAL: {e}")
        sys.exit(1)

    print(f"API CENTRAL escuchando en https://0.0.0.0:5001")
    app.run(host='0.0.0.0', port=5001, ssl_context=contexto_ssl, debug=False)
import ssl
import requests
import socket
import sys
import time
import threading
from cryptography.fernet import Fernet

# ---- Constantes de protocolo de envio de mensajes ----
STX = bytes([0x02])
ETX = bytes([0x03])
ACK = bytes([0x06])
NACK = bytes([0X15])

FORMAT = "utf-8"

monitor_running = False
token_actual = None
clave_actual = None

def calcular_lrc(data):
    lrc = 0
    for b in data:
        lrc ^= b
    return lrc

def construir_empaquetado(mensaje_str): #construye el mensaje empaqueta para la CENTRAL
    mensaje_bytes = mensaje_str.encode(FORMAT)
    
    mensaje_empaquetado = bytearray()
    mensaje_empaquetado.append(STX[0])
    mensaje_empaquetado.extend(mensaje_bytes) #extend ya que es mas de un byte
    mensaje_empaquetado.append(ETX[0])
    mensaje_empaquetado.append(calcular_lrc(mensaje_bytes))
    
    return mensaje_empaquetado

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
        return mensaje_bytes.decode()
    try:
        f = Fernet(clave)
        return f.decrypt(mensaje_bytes).decode()
    except Exception as e:
        print(f"Error al descifrar mensaje: {e}")
        return None

# funcion para el registro en Registry
def registrar_cp_registry(cp_id, url_registry, ubicacion_cp):
    print(f"Conectando con Registry en {url_registry}...")
    
    datos_post = {
        "cp_id": cp_id,
        "ubicacion": ubicacion_cp
    }
    
    try:
        # hacemos la petición POST con verify puesto a False para que confie en certificado que hemos creado
        respuesta = requests.post(f"{url_registry}/registro", json=datos_post, verify=False, timeout=5)
        
        if respuesta.status_code == 201:
            datos = respuesta.json()
            token = datos.get("token")
            clave = datos.get("clave_cifrado")
            print(f"Se ha completado el registro con token recibido: {token[:5]}")
            return token, clave
        else:
            print(f"Registry ha denegado el registro: {respuesta.status_code}-{respuesta.text}")
            return None, None

    except Exception as e:
        print(f"Error conectando con Registry: {e}")
        return None, None

# funciones prac 1
def conectar_con_central(ADDR_CENTRAL, cp_id, token):
    try:
        print("Intentando conectar con CENTRAL (SSL)...")

        contexto = ssl.create_default_context()
        contexto.check_hostname = False
        contexto.verify_mode = ssl.CERT_NONE

        socket_normal = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_normal.settimeout(5.0) #si espera durante 5s y central no responde no se ha podido conectar
        
        s = contexto.wrap_socket(socket_normal, server_hostname=ADDR_CENTRAL[0])
        s.connect(ADDR_CENTRAL)
        print(f"{cp_id} se ha conectado con CENTRAL con sockets seguros")

        #envia peticion de registro a la central, ahora le pasamos el token para que central valide si se ha registrado en registry
        print("Enviando mensaje a CENTRAL para autenticarse ...")
        mensaje_aut = f"AUT,{cp_id},{token}"
        mensaje_aut_empaquetado = construir_empaquetado(mensaje_aut)
        s.sendall(mensaje_aut_empaquetado)
        
        respuesta_central = s.recv(1)
        if  respuesta_central == ACK:
            print(f"CENTRAL ha autorizado al cp {cp_id}")
            return s
        else:
            print(f"CENTRAL ha rechazado el registro de {cp_id} (alomejor no esta registrado el cp en la BD)")
            s.close()
            return None
    except Exception as e:
        print(f"Error al crear el socket seguro: {e}")
        return None

def monitorizacion(cp_id, ADDR_CENTRAL, ADDR_ENGINE):
    global token_actual, clave_actual, monitor_running
    socket_central = None
    socket_engine = None

    ultima_clave_enviada = None
    #bucle para conexion con engine y mandar estado a CENTRAL
    try:
        while monitor_running:
            mensaje_a_central = ""
            
            try:
                if socket_engine is None:
                    print("Intentando conectar con Engine...")
                    socket_engine = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    socket_engine.settimeout(2.0)
                    socket_engine.connect(ADDR_ENGINE)
                    print("ENGINE conetado")                
                    ultima_clave_enviada = None

                # sincronizamos la clave de cifrado para engine
                if clave_actual and (clave_actual != ultima_clave_enviada):
                    #al conectar con el engine le pasamos la clave de cifrado
                    msg_clave = f"SET_KEY,{clave_actual}"
                    socket_engine.sendall(msg_clave.encode(FORMAT))

                    resp_clave = socket_engine.recv(1024).decode(FORMAT)
                    if resp_clave == "OK_KEY":
                        print("El Engine guardo la clave")
                        ultima_clave_enviada = clave_actual
                    else:
                        print(f"El Engine no guardo la clave: {resp_clave}")

                socket_engine.sendall("PING".encode(FORMAT))
                resp = socket_engine.recv(1024).decode(FORMAT)
            
                if not resp:
                    raise Exception("Engine cerró la conexión")

                partes = resp.split(',')
                if len(partes) == 2:
                    estado_eng = partes[0]
                    eng_id = partes[1]

                    # validamos el id del engine que se quiere conectar (tiene que ser el del monitor ya que monitor lo valida con CENTRAL)
                    if eng_id != cp_id:
                        print(f"Error en el id del cp, en Monitor es {cp_id} y en Engine {eng_id}.")
                        raise Exception("ID Incorrecto") #lanzamos una excepcion
                    
                    if estado_eng == "OK":
                        mensaje_a_central = f"STAT,{cp_id},OK"
                    else:
                        mensaje_a_central = f"EVENT,{cp_id},AVERIADO"
                else:
                    mensaje_a_central = f"EVENT,{cp_id},AVERIADO"

            except Exception as e:
                mensaje_a_central = f"EVENT,{cp_id},AVERIADO" #si ha fallado algo mandamos una averia
                if socket_engine:
                    socket_engine.close()
                socket_engine = None 

            if not token_actual:
                print("No se puede enviar estado a CENTRAL porque no hay token valido")
                time.sleep(2)
                continue

            # despues de comprobar el estado, monitor se lo envia a central
            if socket_central is None:
                socket_central = conectar_con_central(ADDR_CENTRAL, cp_id, token_actual)
        
            if socket_central:
                try:
                    print(f"Enviando mensaje a Central: {mensaje_a_central}")

                    #ciframos el mensaje con la clave del registry
                    mensaje_cifrado = cifrar(mensaje_a_central, clave_actual).decode()
                    mensaje_empaquetado = construir_empaquetado(mensaje_cifrado)
                    socket_central.sendall(mensaje_empaquetado)
                    
                    # esperamos confirmación de central
                    socket_central.settimeout(2.0)
                    ack = socket_central.recv(1)
                    if ack != ACK:
                        print("No se ha recibido un ACK de CENTRAL")
                except Exception as e:
                    print(f"Conexion perdida con CENTRAL: {e}")
                    socket_central.close()
                    socket_central = None
            else:
                print("Vigilando ENGINE sin CENTRAL")
            time.sleep(1) #espera un segundo
    except KeyboardInterrupt:
        print("\nMonitor finalizado manualmente (Ctrl+C).")
    finally:
        if socket_central:
            socket_central.close()
        if socket_engine: 
            socket_engine.close()

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Error: Argumentos incorrectos")
        print("Uso: python3 EV_CP_M.py <central_ip> <central_puerto> <engine_ip> <engine_puerto> <cp_id> <ubicacion_cp>")
        sys.exit(1)

    central_ip = sys.argv[1]
    central_puerto = int(sys.argv[2])
    engine_ip = sys.argv[3]
    engine_puerto = int(sys.argv[4])
    cp_id = sys.argv[5]
    ubicacion = sys.argv[6]
    
    ADDR_CENTRAL = (central_ip, central_puerto)
    ADDR_ENGINE = (engine_ip, engine_puerto)
    
    URL_REGISTRY = f"https://192.168.56.110:5000" #la maquina donde esta dirvers y registry
    
    monitor_running = True
    threading.Thread(target=monitorizacion, args=(cp_id, ADDR_CENTRAL, ADDR_ENGINE), daemon=True).start()
    try:
        while True:
            input("Pulsa ENTER para renovar credenciales: ")
            
            print("\nSolicitando nuevas credenciales al Registry...")
            nuev_tok, nuev_clav = registrar_cp_registry(cp_id, URL_REGISTRY, ubicacion)
            
            if nuev_tok:
                token_actual = nuev_tok
                clave_actual = nuev_clav
                print("Credenciales actualizadas. El monitor intentará reconectar automáticamente.")
            else:
                print("Error al renovar credenciales")
                
    except KeyboardInterrupt:
        monitor_running = False
        print("\nSaliendo...")
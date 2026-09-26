from flask import Flask, request, jsonify
import requests
import ssl
import os
import secrets
from cryptography.fernet import Fernet
import sys

URL_CENTRAL = os.environ.get("CENTRAL_URL", "https://192.168.56.1:5001")

app = Flask(__name__)

#ruta para recibir peticiones de registro de monitor
@app.route('/registro', methods=['POST'])
def registrar_cp():
    try:
        datos = request.json
        cp_id = datos.get('cp_id')
        ubi = datos.get('ubicacion')

        print(f"Se ha recibido una solicitud de registro: {cp_id}")

        if not cp_id:
            return jsonify({"error": "Falta cp_id"}), 400

        # generamos las credenciales credenciales
        token = secrets.token_hex(16) # para autenticarse en Central
        clave_cifrado = Fernet.generate_key().decode() # para cifrar mensajes
        
        payload = {
            "cp_id": cp_id,
            "ubicacion": ubi,
            "token": token,
            "clave_cifrado": clave_cifrado
        }
        
        try:
            # enviamos los datos a la API CENTRAL para actualizar la BD
            resp = requests.post(f"{URL_CENTRAL}/api/internal/registro-cp", json=payload, verify=False, timeout=5)
            
            if resp.status_code == 201:
                print(f"CP {cp_id} sincronizado con Central correctamente.")
                # respondemos a monitor con las credenciales
                return jsonify({
                    "mensaje": "Registro OK",
                    "token": token,
                    "clave_cifrado": clave_cifrado
                }), 201
            else:
                raise Exception(f"Central rechazó el guardado: {resp.text}")
                
        except Exception as e:
            print(f"Error conectando con Central para guardar datos: {e}")
            return jsonify({"error": "Fallo de comunicación interna Registry-Central"}), 500

    except Exception as e:
        print(f"Error durante registro: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    contexto_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert_file = os.environ.get('CERT_FILE', 'registry_cert.pem')
    key_file = os.environ.get('KEY_FILE', 'registry_key.pem')
    try:
        contexto_ssl.load_cert_chain(cert_file, key_file) #cargamos el certificado del registry
        print("Se han cargado los Certificados SSL.")
    except Exception as e:
        print(f"Error al cargar los certificados: {e}")
        sys.exit(1)
    
    registry_port = int(os.environ.get('REGISTRY_PORT', '5000'))
    print(f"Iniciando EV_Registry con securizacion en https://0.0.0.0:{registry_port}")

    # ponemos 0.0.0.0 para la maquina donde esta el monitor pueda pedir el registro
    app.debug = False
    app.run(host='0.0.0.0', port=registry_port, ssl_context=contexto_ssl, debug=False)
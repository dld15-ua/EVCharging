from flask import Flask, render_template
import os
import ssl

app = Flask(__name__)

# Configurar el puerto y la URL de la API desde variables de entorno
frontend_port = int(os.environ.get('FRONTEND_PORT', 5002))
central_api_url = os.environ.get('CENTRAL_API_URL', 'https://localhost:5001')

@app.route('/')
def home():
    # Inyectamos la URL de la API al frontend
    return render_template('index.html', api_base=central_api_url)

if __name__ == '__main__':
    # Configurar SSL para HTTPS (obligatorio en el proyecto)
    contexto_ssl = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert_path = os.environ.get('SSL_CERT_PATH', '/app/certs/server_cert.pem')
    key_path = os.environ.get('SSL_KEY_PATH', '/app/certs/server_key.pem')
    
    try:
        contexto_ssl.load_cert_chain(cert_path, key_path)
    except Exception as e:
        print(f"Error cargando certificados SSL en Frontend: {e}")
        contexto_ssl = None

    print(f"Iniciando EV_Frontend (UI) en https://0.0.0.0:{frontend_port}")
    print(f"Conectado a la API de Central en: {central_api_url}")
    
    app.run(host='0.0.0.0', port=frontend_port, ssl_context=contexto_ssl, debug=False)

import time
import os
import requests
import sys

API_KEY = os.environ.get("OPENWEATHER_API_KEY")

# formato env var: "Alicante:ALC1,Madrid:ALC2"
def parsear_ciudades(env_str):
    ciudades = {}
    if env_str:
        for par in env_str.split(','):
            partes = par.strip().split(':')
            if len(partes) == 2:
                ciudades[partes[0].strip()] = partes[1].strip()
    return ciudades

_ciudades_env = os.environ.get("CIUDADES", "")
CIUDADES = parsear_ciudades(_ciudades_env) if _ciudades_env else {
    "Alicante": "ALC1",
    "Madrid": "ALC2"
}

CENTRAL_URL = os.environ.get("CENTRAL_URL", "https://192.168.56.1:5001")

LIMITE_TEMP = int(os.environ.get("LIMITE_TEMP", "20"))

estados_clima = {} #para mantener el estado del clima

def obtener_temperatura(ciudad):
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={API_KEY}&units=metric"
        respuesta = requests.get(url)
        data = respuesta.json()
        if respuesta.status_code == 200:
            return data['main']['temp']
    except Exception as e:
        print(f"Error al obtener temperatura: {e}")
    return None

def notificar_central(cp_id, accion):
    ruta = f"{CENTRAL_URL}/api/weather"
    payload = {"cp_id": cp_id, "estado_clima": accion}
    try:
        respuesta = requests.post(ruta, json=payload, verify=False, timeout=5) 
        if respuesta.status_code == 200:
            print(f"Notificado a central, poniendo {cp_id} en {accion}")
            return True
        else:
            print(f"Error al notificar a Central: {respuesta.status_code}")
            return False
    except Exception as e:
        print(f"Error conectando con Central: {e}")
        return False

if __name__ == "__main__":

    for ciudad, cp_id in CIUDADES.items():
        estados_clima[ciudad] = None

    ultimo_sync = time.time()
    
    while True:
        try:
            print("Consultando la API de OpenWeather para obtener temperatura...")
            for ciudad, cp_id in CIUDADES.items():
                temp = obtener_temperatura(ciudad)
                if temp is not None:
                    print(f"{ciudad}: {temp}ºC")
                    estado = "normal"
                    if temp < LIMITE_TEMP:
                        estado = "alerta"

                    estado_anterior = estados_clima[ciudad] #sacamos el estado guradado anterior para ver si hace falta actualizarlo 
                    
                    # Sincronizamos si hay un cambio, o de forma forzada cada 60 segundos para asegurar que Central no lo olvide al reiniciarse
                    forzar_sync = (time.time() - ultimo_sync) > 60
                    
                    if estado != estado_anterior or forzar_sync:
                        if estado != estado_anterior:
                            print(f"Se ha detectado un cambio en el estado del clima en {ciudad}")
                        else:
                            print(f"Sincronización rutinaria del clima en {ciudad}")

                        if notificar_central(cp_id, estado):
                            estados_clima[ciudad] = estado
                            if forzar_sync: 
                                ultimo_sync = time.time()
                        else:
                            print(f"Fallo al notificar estado de clima")
                    else:
                        print("Sin cambios en el estado")
            time.sleep(4)
        except KeyboardInterrupt:
            print("Saliendo por Ctrl+C")
            sys.exit(1)
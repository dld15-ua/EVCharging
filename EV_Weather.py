import time
import requests
import sys

API_KEY = "bfeda57348ea4fb6d487399245c47a30"

CIUDADES = { #diccionario con las localidades a las que pedira el clima la api, con su CP
    "Alicante": "ALC1",
    "Madrid": "ALC2"
}

CENTRAL_URL = "https://192.168.56.1:5001" 

LIMITE_TEMP = 20

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
                    if estado != estado_anterior:
                        print(f"Se ha detectado un cambio en el estado del clima en {ciudad}")

                        if notificar_central(cp_id, estado):
                            estados_clima[ciudad] = estado
                            print(f"Se ha cambiado el estado en {ciudad} a {estado}")
                        else:
                            print(f"Fallo al notificar cambio de estado")
                    else:
                        print("Sin cambios en el estado")
            time.sleep(4)
        except KeyboardInterrupt:
            print("Saliendo por Ctrl+C")
            sys.exit(1)
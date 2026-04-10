# 1. Extração dos dados (E)

# Mudar aqui o período de dias!!
PAST_DAYS = 30


import json

import requests

url = "https://api.open-meteo.com/v1/forecast"

resposta = requests.get(
    url,
    params={
        "latitude": "-30.0328",
        "longitude": "-51.2302",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,sunrise,sunset,rain_sum,precipitation_sum,wind_speed_10m_max",
        "current": "temperature_2m,relative_humidity_2m,precipitation,rain,weather_code,wind_speed_10m",
        "timezone": "America/Sao_Paulo",
        "past_days": PAST_DAYS,
        "forecast_days": "1",
    },
)

print(resposta.text)

with open("dados_brutos.json", "w") as f:
    # f.write(resposta.text)
    json.dump(resposta.json(), f, indent=4, ensure_ascii=False)


# 2. Transformação dos dados (T)
with open("dados_brutos.json", "r") as f:
    dados = f.read()

# print(dados)
# print(type(dados))
dados_convertidos = json.loads(dados)
# print(type(dados_convertidos))


# print(dados_convertidos["latitude"])
# print(dados_convertidos["longitude"])
# print(dados_convertidos["generationtime_ms"])

dados_selecionados = {
    "elevation": dados_convertidos["elevation"],
    "current_time": dados_convertidos["current"]["time"],
    "current_temperature": dados_convertidos["current"]["temperature_2m"],
    "current_precipitation": dados_convertidos["current"]["precipitation"],
    "current_relative_humidity": dados_convertidos["current"]["relative_humidity_2m"],
    "current_rain": dados_convertidos["current"]["rain"],
    "current_wind_speed_10m": dados_convertidos["current"]["wind_speed_10m"],
}


# print(dados_convertidos["daily"]["time"][0])
# print(dados_convertidos["daily"]["temperature_2m_max"][7])
# print(json.dumps(dados_selecionados, indent=4))

dados_por_dia = []

# Acessar os dados por posição (index) e valor (item)
for index, item in enumerate(dados_convertidos["daily"]["time"]):
    dados_por_dia.append(
        {
            "date": item,
            "temperature_max": dados_convertidos["daily"]["temperature_2m_max"][index],
            "temperature_min": dados_convertidos["daily"]["temperature_2m_min"][index],
            "sunrise": dados_convertidos["daily"]["sunrise"][index],
            "sunset": dados_convertidos["daily"]["sunset"][index],
            "rain_sum": dados_convertidos["daily"]["rain_sum"][index],
            "precipitation_sum": dados_convertidos["daily"]["precipitation_sum"][index],
            "wind_speed_10m_max": dados_convertidos["daily"]["wind_speed_10m_max"][
                index
            ],
            **dados_selecionados,
        }
    )

print(json.dumps(dados_por_dia, indent=4))
# print(json.dumps(dados_selecionados, indent=4))


# 3. Carga dos dados (L)
with open("dados_tratados.json", "w") as f:
    json.dump(dados_por_dia, f, indent=4)

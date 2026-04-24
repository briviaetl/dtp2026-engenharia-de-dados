import requests


# Chave da API usada nos exemplos da aula.
API_KEY = "X7ENnmIUEmOmm4amhjKjbjXcN640Uzah"

# Endpoint que vamos consultar.
URL = "https://app.ticketmaster.com/discovery/v2/events.json"


def extrair_eventos_pais(codigo_pais, tamanho_pagina=3, max_paginas=1):
    # Aqui vamos guardar todos os eventos coletados.
    eventos = []

    # A API começa na página 0.
    pagina_atual = 0

    print(f"\nIniciando extração do país: {codigo_pais}")
    print(f"Tamanho da página: {tamanho_pagina}")
    print(f"Máximo de páginas que vamos buscar: {max_paginas}")

    # Enquanto ainda não atingimos o limite definido,
    # seguimos buscando novas páginas.
    while pagina_atual < max_paginas:
        # Estes são os parâmetros enviados para a API.
        # apikey autentica.
        # countryCode filtra o país.
        # size limita quantos eventos vêm por página.
        # page escolhe a página atual.
        parametros = {
            "apikey": API_KEY,
            "countryCode": codigo_pais,
            "size": tamanho_pagina,
            "page": pagina_atual,
        }

        print(f"Buscando página {pagina_atual} de {codigo_pais}...")

        # Faz a chamada HTTP para a API.
        resposta = requests.get(URL, params=parametros, timeout=30)

        # Se a API devolver erro HTTP, o script para aqui.
        resposta.raise_for_status()

        # Converte a resposta para dicionário Python.
        dados = resposta.json()

        # Os eventos ficam dentro de _embedded -> events.
        # Quando não há resultados, essa estrutura pode não existir.
        if "_embedded" in dados and "events" in dados["_embedded"]:
            eventos_pagina = dados["_embedded"]["events"]
        else:
            eventos_pagina = []

        print(f"Quantidade de eventos nesta página: {len(eventos_pagina)}")

        # Junta os eventos desta página com os anteriores.
        eventos.extend(eventos_pagina)

        # A própria API informa dados de paginação.
        if "page" in dados:
            total_paginas = dados["page"].get("totalPages", 0)
            total_elementos = dados["page"].get("totalElements", 0)
            print(f"Total de páginas informado pela API: {total_paginas}")
            print(f"Total de eventos informado pela API: {total_elementos}")

        # Se esta página veio vazia, não vale a pena continuar.
        if len(eventos_pagina) == 0:
            print("Nenhum evento encontrado nesta página. Encerrando este país.")
            break

        # Vai para a próxima página.
        pagina_atual = pagina_atual + 1

    print(f"Extração finalizada para {codigo_pais}.")
    print(f"Total de eventos coletados: {len(eventos)}")

    return eventos

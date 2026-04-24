from time import perf_counter

from extrator import extrair_eventos_pais


# Lista de países usada na comparação.
paises = ["BR", "US", "GB", "DE", "FR", "AU", "MX", "CA", "ES", "NL"]

# Vamos manter poucas chamadas para não gastar quota.
tamanho_pagina = 2
max_paginas = 1

print("Exemplo 02 - extração em sequência")
print("Aqui vamos buscar poucos eventos de 10 países, um por vez.")

# Marca o começo da medição de tempo.
inicio = perf_counter()

# Dicionário para guardar o resultado de cada país.
resultado_por_pais = {}

# Aqui o loop é sequencial.
# Um país só começa depois que o anterior termina.
for codigo_pais in paises:
    print("\n----------------------------------------")
    print(f"Agora é a vez do país: {codigo_pais}")

    # Busca os eventos daquele país.
    eventos = extrair_eventos_pais(
        codigo_pais=codigo_pais,
        tamanho_pagina=tamanho_pagina,
        max_paginas=max_paginas,
    )

    # Guarda o resultado na memória.
    resultado_por_pais[codigo_pais] = eventos

# Marca o fim da medição.
fim = perf_counter()

print("\nResumo final do exemplo 02")

# Mostra quantos eventos vieram em cada país.
for codigo_pais, eventos in resultado_por_pais.items():
    print(f"{codigo_pais}: {len(eventos)} eventos")

print(f"Tempo total da execução em sequência: {fim - inicio:.2f} segundos")

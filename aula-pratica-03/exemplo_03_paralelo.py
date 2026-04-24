from concurrent.futures import ThreadPoolExecutor, as_completed
from time import perf_counter

from extrator import extrair_eventos_pais

# Usamos os mesmos países do exemplo sequencial.
paises = ["BR", "US", "GB", "DE", "FR", "AU", "MX", "CA", "ES", "NL"]

# Mantemos poucas chamadas por segurança.
tamanho_pagina = 2
max_paginas = 1

# Quantidade máxima de execuções ao mesmo tempo.
max_trabalhadores = 7

print("Exemplo 03 - extração em paralelo")
print("Vamos usar poucas requisições por país para evitar sobrecarga da API.")
print(f"Quantidade máxima de execuções paralelas: {max_trabalhadores}")

print("\nPrimeiro: medindo a versão em sequência para comparar.")

# Mede o tempo da versão em sequência.
inicio_sequencia = perf_counter()
total_sequencia = 0

# Primeiro fazemos do jeito tradicional: um país por vez.
for codigo_pais in paises:
    eventos = extrair_eventos_pais(
        codigo_pais=codigo_pais,
        tamanho_pagina=tamanho_pagina,
        max_paginas=max_paginas,
    )
    total_sequencia = total_sequencia + len(eventos)

fim_sequencia = perf_counter()
tempo_sequencia = fim_sequencia - inicio_sequencia


print("\nAgora: medindo a versão em paralelo.")

# Mede o tempo da versão paralela.
inicio_paralelo = perf_counter()
resultado_por_pais = {}

# O ThreadPoolExecutor cria um pequeno grupo de trabalhadores.
# Cada trabalhador pode executar uma chamada ao mesmo tempo.
with ThreadPoolExecutor(max_workers=max_trabalhadores) as executor:
    tarefas = {}

    # Aqui disparamos uma tarefa para cada país.
    for codigo_pais in paises:
        tarefa = executor.submit(
            extrair_eventos_pais,
            codigo_pais,
            tamanho_pagina,
            max_paginas,
        )
        tarefas[tarefa] = codigo_pais

    # as_completed devolve as tarefas conforme elas terminam.
    for tarefa in as_completed(tarefas):
        codigo_pais = tarefas[tarefa]
        try:
            eventos = tarefa.result()
            resultado_por_pais[codigo_pais] = eventos
            print(f"Paralelo concluído para {codigo_pais}: {len(eventos)} eventos")
        except Exception as erro:
            # Se uma chamada falhar, guardamos lista vazia
            # para o script continuar e mostrar o erro.
            resultado_por_pais[codigo_pais] = []
            print(f"Paralelo com erro para {codigo_pais}: {erro}")

fim_paralelo = perf_counter()
tempo_paralelo = fim_paralelo - inicio_paralelo

# Soma o total final de eventos coletados em paralelo.
total_paralelo = 0
for eventos in resultado_por_pais.values():
    total_paralelo = total_paralelo + len(eventos)

print("\nResumo final do exemplo 03")
print(f"Tempo em sequência: {tempo_sequencia:.2f} segundos")
print(f"Tempo em paralelo: {tempo_paralelo:.2f} segundos")
print(f"Total de eventos em sequência: {total_sequencia}")
print(f"Total de eventos em paralelo: {total_paralelo}")

if tempo_paralelo > 0:
    print(f"Ganho aproximado: {tempo_sequencia / tempo_paralelo:.2f}x")

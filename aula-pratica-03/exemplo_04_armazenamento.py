import datetime as dt
import json
from pathlib import Path

import duckdb

from extrator import URL, extrair_eventos_pais


# Mesmo padrão simples do exemplo 01.
codigo_pais = "BR"
tamanho_pagina = 3
max_paginas = 1

# Informações usadas para montar o caminho no datalake.
nome_api = "ticketmaster"
nome_endpoint = "discovery_v2_events"
nome_arquivo = "events"
data_extracao = dt.date.today().isoformat()

print("Exemplo 04 - armazenamento na bronze")
print("Vamos extrair poucos eventos e salvar em JSON e PARQUET.")

# Reaproveita a mesma extração simples do exemplo 01.
eventos = extrair_eventos_pais(
    codigo_pais=codigo_pais,
    tamanho_pagina=tamanho_pagina,
    max_paginas=max_paginas,
)

# Antes de salvar, adicionamos a data e hora de extração.
for evento in eventos:
    evento["etl_data_hora_extracao"] = dt.datetime.now().isoformat()

print(f"\nTotal de eventos para salvar: {len(eventos)}")
print(f"Endpoint usado: {URL}")

# Caminho do JSON na bronze.
caminho_json = (
    Path("datalake")
    / "bronze"
    / nome_api
    / nome_endpoint
    / "json"
    / f"data_extracao={data_extracao}"
    / f"{nome_arquivo}.json"
)

# Caminho do parquet na bronze.
caminho_parquet = (
    Path("datalake")
    / "bronze"
    / nome_api
    / nome_endpoint
    / "parquet"
    / f"data_extracao={data_extracao}"
    / f"{nome_arquivo}.parquet"
)

# Cria as pastas automaticamente, se ainda não existirem.
caminho_json.parent.mkdir(parents=True, exist_ok=True)
caminho_parquet.parent.mkdir(parents=True, exist_ok=True)

# Salva o JSON com indentação para facilitar leitura humana.
caminho_json.write_text(
    json.dumps(eventos, indent=4, ensure_ascii=False),
    encoding="utf-8",
)

print(f"JSON salvo em: {caminho_json}")

# Gera o parquet a partir do JSON usando DuckDB.
# Isso mantém a estrutura aninhada e evita problemas do pandas
# com alguns campos vazios da API.
conexao = duckdb.connect()
sql_json = str(caminho_json).replace("'", "''")
sql_parquet = str(caminho_parquet).replace("'", "''")
conexao.execute(
    f"COPY (SELECT * FROM read_json_auto('{sql_json}')) TO '{sql_parquet}' (FORMAT PARQUET)"
)
conexao.close()

print(f"PARQUET salvo em: {caminho_parquet}")

import datetime as dt
from pathlib import Path

import duckdb


# Informações da origem bronze e do destino silver.
nome_api = "ticketmaster"
nome_endpoint = "discovery_v2_events"
data_extracao = dt.date.today().isoformat()

# Caminho do parquet bruto gerado no exemplo 04.
caminho_bronze = (
    Path("datalake")
    / "bronze"
    / nome_api
    / nome_endpoint
    / "parquet"
    / f"data_extracao={data_extracao}"
    / "events.parquet"
)

print("Exemplo 05 - tratamento com DuckDB")
print(
    "Vamos ler o parquet da bronze, explorar os dados aninhados e salvar tabelas na silver."
)
print(f"Arquivo de entrada: {caminho_bronze}")

conexao = duckdb.connect()
sql_bronze = str(caminho_bronze).replace("'", "''")

# Mostra a estrutura principal do parquet bronze.
print("\nSchema principal do parquet bronze:")
schema = conexao.execute(
    f"DESCRIBE SELECT * FROM read_parquet('{sql_bronze}')",
).fetchall()

for linha in schema:
    print(linha)

# Tabela silver principal com alguns campos mais diretos.
consulta_eventos = """
SELECT
    id AS evento_id,
    name AS evento_nome,
    type AS evento_tipo,
    url AS evento_url,
    locale,
    dates.start.localDate AS data_local,
    dates.start.localTime AS hora_local,
    dates.timezone AS fuso_horario,
    sales.public.startDateTime AS venda_inicio,
    sales.public.endDateTime AS venda_fim,
    etl_data_hora_extracao
FROM read_parquet(?)
"""

# Tabela silver de classificações.
consulta_classificacoes = """
SELECT
    e.id AS evento_id,
    c.unnest.segment.name AS segmento,
    c.unnest.genre.name AS genero,
    c.unnest.subGenre.name AS subgenero,
    c.unnest.primary AS classificacao_principal,
    e.etl_data_hora_extracao
FROM read_parquet(?) AS e,
UNNEST(e.classifications) AS c(unnest)
"""

# Tabela silver de atrações.
consulta_atracoes = """
SELECT
    e.id AS evento_id,
    a.unnest.id AS atracao_id,
    a.unnest.name AS atracao_nome,
    a.unnest.type AS atracao_tipo,
    a.unnest.url AS atracao_url,
    e.etl_data_hora_extracao
FROM read_parquet(?) AS e,
UNNEST(e._embedded.attractions) AS a(unnest)
"""

# Tabela silver de venues.
consulta_venues = """
SELECT
    e.id AS evento_id,
    v.unnest.id AS venue_id,
    v.unnest.name AS venue_nome,
    v.unnest.type AS venue_tipo,
    v.unnest.timezone AS venue_timezone,
    v.unnest.country.countryCode AS venue_pais,
    v.unnest.state.stateCode AS venue_estado,
    v.unnest.location.latitude AS venue_latitude,
    v.unnest.location.longitude AS venue_longitude,
    e.etl_data_hora_extracao
FROM read_parquet(?) AS e,
UNNEST(e._embedded.venues) AS v(unnest)
"""

# Caminhos de saída da silver.
caminho_eventos = (
    Path("datalake")
    / "silver"
    / nome_api
    / nome_endpoint
    / "eventos"
    / "parquet"
    / f"data_extracao={data_extracao}"
    / "eventos.parquet"
)

caminho_classificacoes = (
    Path("datalake")
    / "silver"
    / nome_api
    / nome_endpoint
    / "classificacoes"
    / "parquet"
    / f"data_extracao={data_extracao}"
    / "classificacoes.parquet"
)

caminho_atracoes = (
    Path("datalake")
    / "silver"
    / nome_api
    / nome_endpoint
    / "atracoes"
    / "parquet"
    / f"data_extracao={data_extracao}"
    / "atracoes.parquet"
)

caminho_venues = (
    Path("datalake")
    / "silver"
    / nome_api
    / nome_endpoint
    / "venues"
    / "parquet"
    / f"data_extracao={data_extracao}"
    / "venues.parquet"
)

sql_eventos = str(caminho_eventos).replace("'", "''")
sql_classificacoes = str(caminho_classificacoes).replace("'", "''")
sql_atracoes = str(caminho_atracoes).replace("'", "''")
sql_venues = str(caminho_venues).replace("'", "''")

# Cria as pastas automaticamente.
caminho_eventos.parent.mkdir(parents=True, exist_ok=True)
caminho_classificacoes.parent.mkdir(parents=True, exist_ok=True)
caminho_atracoes.parent.mkdir(parents=True, exist_ok=True)
caminho_venues.parent.mkdir(parents=True, exist_ok=True)

# Salva as tabelas tratadas na silver.
conexao.execute(
    "COPY ("
    + consulta_eventos.replace("?", f"'{sql_bronze}'")
    + ") TO '"
    + sql_eventos
    + "' (FORMAT PARQUET)"
)

conexao.execute(
    "COPY ("
    + consulta_classificacoes.replace("?", f"'{sql_bronze}'")
    + ") TO '"
    + sql_classificacoes
    + "' (FORMAT PARQUET)"
)

conexao.execute(
    "COPY ("
    + consulta_atracoes.replace("?", f"'{sql_bronze}'")
    + ") TO '"
    + sql_atracoes
    + "' (FORMAT PARQUET)"
)

conexao.execute(
    "COPY ("
    + consulta_venues.replace("?", f"'{sql_bronze}'")
    + ") TO '"
    + sql_venues
    + "' (FORMAT PARQUET)"
)

print("\nArquivos criados na silver:")
print(caminho_eventos)
print(caminho_classificacoes)
print(caminho_atracoes)
print(caminho_venues)

conexao.close()

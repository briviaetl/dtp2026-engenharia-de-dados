select
    -- Identificador único da linha da venda.
    -- Na prática, é o grão da nossa fato: 1 linha = 1 item vendido.
    invoice_and_item_number as invoice_item_id,

    -- Data original da venda.
    date as sale_date,

    -- Chave de data no formato YYYYMMDD.
    -- Muito usada em modelos dimensionais para ligar com a dm_date.
    cast(format_date('%Y%m%d', date) as int64) as date_key,

    -- Chave da loja.
    -- TRIM remove espaços extras.
    -- NULLIF transforma string vazia em NULL.
    nullif(trim(store_number), '') as store_key,

    -- Nome da loja.
    nullif(trim(store_name), '') as store_name,

    -- Endereço da loja.
    nullif(trim(address), '') as store_address,

    -- Cidade da loja.
    nullif(trim(city), '') as city,

    -- CEP da loja.
    -- Alguns valores vêm com ".0" no final, como "50314.0".
    -- REGEXP_REPLACE remove esse ruído.
    nullif(trim(regexp_replace(zip_code, r'\.0+$', '')), '') as zip_code,

    -- Código do condado.
    -- Também pode vir com ".0", então aplicamos a mesma limpeza.
    nullif(trim(regexp_replace(county_number, r'\.0+$', '')), '') as county_number,

    -- Nome do condado.
    nullif(trim(county), '') as county,

    -- Ponto geográfico completo da loja.
    store_location,

    -- Longitude extraída do ponto geográfico.
    st_x(store_location) as longitude,

    -- Latitude extraída do ponto geográfico.
    st_y(store_location) as latitude,

    -- Chave da categoria.
    -- Remove ".0", tira espaços e, se não existir valor válido,
    -- substitui por 'UNKNOWN' para não perder a linha na modelagem.
    coalesce(
        nullif(trim(regexp_replace(category, r'\.0+$', '')), ''), 'UNKNOWN'
    ) as category_key,

    -- Nome da categoria.
    -- Se vier vazio ou nulo, colocamos um valor padrão.
    coalesce(nullif(trim(category_name), ''), 'Unknown Category') as category_name,

    -- Chave do fornecedor.
    -- Mesma lógica: limpeza + valor padrão quando não houver código.
    coalesce(
        nullif(trim(regexp_replace(vendor_number, r'\.0+$', '')), ''), 'UNKNOWN'
    ) as vendor_key,

    -- Nome do fornecedor.
    coalesce(nullif(trim(vendor_name), ''), 'Unknown Vendor') as vendor_name,

    -- Chave do produto.
    nullif(trim(item_number), '') as product_key,

    -- Nome/descrição do produto.
    coalesce(nullif(trim(item_description), ''), 'Unknown Product') as product_name,

    -- Quantidade de garrafas por caixa.
    pack,

    -- Volume de cada garrafa em mililitros.
    bottle_volume_ml,

    -- Custo unitário por garrafa.
    -- ROUND limita em 2 casas.
    -- CAST para NUMERIC evita trabalhar com valor monetário em FLOAT.
    cast(round(state_bottle_cost, 2) as numeric) as unit_cost_amount,

    -- Preço de venda unitário por garrafa.
    cast(round(state_bottle_retail, 2) as numeric) as unit_retail_amount,

    -- Quantidade de garrafas vendidas nesta linha.
    bottles_sold,

    -- Valor total da venda da linha.
    cast(round(sale_dollars, 2) as numeric) as sale_amount,

    -- Custo total da linha:
    -- custo unitário * quantidade vendida.
    -- COALESCE evita que um NULL quebre o cálculo.
    cast(
        round(coalesce(state_bottle_cost, 0) * coalesce(bottles_sold, 0), 2) as numeric
    ) as cost_amount,

    -- Lucro bruto da linha:
    -- (preço unitário - custo unitário) * quantidade vendida.
    cast(
        round(
            (coalesce(state_bottle_retail, 0) - coalesce(state_bottle_cost, 0))
            * coalesce(bottles_sold, 0),
            2
        ) as numeric
    ) as gross_profit_amount,

    -- Volume total vendido em litros.
    cast(round(volume_sold_liters, 3) as numeric) as volume_sold_liters,

    -- Volume total vendido em galões.
    cast(round(volume_sold_gallons, 3) as numeric) as volume_sold_gallons,

    -- Flag para identificar devolução ou ajuste negativo.
    -- Se qualquer medida principal vier negativa, marcamos como TRUE.
    case
        when
            coalesce(sale_dollars, 0) < 0
            or coalesce(bottles_sold, 0) < 0
            or coalesce(volume_sold_liters, 0) < 0
        then true
        else false
    end as is_return

from {{ source("iwoa_liquor", "sales") }}

-- Mantemos apenas linhas com os campos mínimos necessários
-- para o nosso grão analítico.
where
    invoice_and_item_number is not null and date is not null and item_number is not null

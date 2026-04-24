import json

from extrator import extrair_eventos_pais


# País usado no exemplo mais simples.
codigo_pais = "BR"

# Vamos buscar poucos registros para a demonstração.
tamanho_pagina = 3
max_paginas = 1

print("Exemplo 01 - chamada simples da API")
print("Vamos buscar poucos registros só para demonstrar a extração.")

# Faz a chamada da função extratora.
eventos = extrair_eventos_pais(
    codigo_pais=codigo_pais,
    tamanho_pagina=tamanho_pagina,
    max_paginas=max_paginas,
)

print(f"\nTotal de eventos recebidos: {len(eventos)}")

# Se houver resultado, mostramos o primeiro evento completo.
# Isso ajuda a turma a enxergar o JSON real da API.
if len(eventos) > 0:
    print("\nPrimeiro evento retornado pela API:")
    print(json.dumps(eventos[0], indent=2, ensure_ascii=False))
else:
    print("Nenhum evento retornado para este exemplo.")

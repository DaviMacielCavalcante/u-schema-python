# resources/

Artefatos de referência versionados, consumidos pelo porte e pelos testes.

## Onde cada XMI mora

Três produtores, três lugares. **Não misturar** — foi por isso que a separação
existe: um XMI de referência e um XMI gerado pelo porte são indistinguíveis pelo
conteúdo, e confundi-los invalida qualquer comparação.

| Diretório | Quem produziu | Versionado? |
|---|---|---|
| **`resources/`** (este) | os **autores originais** do U-Schema, sobre o dataset deles | **sim** — é a amarra com o experimento publicado, e é imutável |
| **`out/oraculo/`** | o **oráculo Java** (Docker, `oracle/`) rodando sobre **os nossos** dados | não (`.gitignore`) — regenerável pela imagem + semente |
| **`out/porte/`** | o **porte Python** (baterias da Fase 3) | não (`.gitignore`) — regenerável pelo script + semente |
| **`out/fase4/oraculo/`**, **`out/fase4/porte/`** | os mesmos dois produtores, nas baterias da Fase 4.4 (as duas engines do porte); cada XMI se chama pelo `run_id` da corrida | não (`.gitignore`) — regenerável pelo script + semente + engine |

**Nunca sobrescrever `resources/` com saída nossa.** Os XMIs daqui vêm de
uma instância de dataset que não temos e não conseguimos reconstruir (os
geradores só ganharam `--seed` em 31/07/2026). Substituí-los perderia
definitivamente a ligação com o experimento do artigo. XMI-oráculo gerado por
nós vai para `out/oraculo/`; se algum dia um deles precisar virar referência
permanente de teste, entra aqui por **promoção deliberada**, com a proveniência
(semente, tamanho, SHA da imagem) registrada.

A comparação mais forte que o projeto pode fazer é `out/oraculo/` × `out/porte/`
sobre **a mesma instância semeada** — mesma entrada, duas implementações. O que
existe contra `resources/` é o porte sobre o nosso dado × o Java sobre o dado
deles, e é exatamente por isso que sobram divergências de `count`.

**Executada nos quatro tamanhos em 02/08/2026 (seed 23):** `equivalent=True` e
**zero divergências** em todas. Contra `resources/neo4j/` as mesmas corridas
acusam 7 não-fatais cada. A variável isolada é o dataset, não a implementação —
e é por isso que a distinção entre estes três diretórios não é organização, é
método.

| Arquivo | Papel |
|---|---|
| `uschema.ecore` | Metamodelo (19 EClasses, sem OCL). Carregado por `uschema.metamodel` (Fase 0.1). |
| `model_northwind.xmi` | XMI-oráculo do Northwind (19 `EntityType`, agregado `Detail`). Round-trip da Fase 0.2 e golden-master das Fases 2.3/3.1. |
| `model_mintest.xmi` | XMI-oráculo do `mintest` — golden-master da Fase 1.7 (0 divergências). |
| `model.xmi` | Modelo mínimo MongoDB (round-trip Fase 0.2). |
| `movies_min.xmi` | **Não é um "modelo mínimo"** — é o **User Profiles / Neo4j no tamanho `small`**: 100.000 `User` (5 variações) + 50.000 `Movie`. O nome engana. |
| `up_medium.xmi` · `up_large.xmi` · `up_larger.xmi` | Mesmo dataset nos tamanhos `medium`/`large`/`larger` — 200k/400k/**800k** `User` e 100k/200k/**400k** `Movie`. Os quatro tamanhos **dobram exatamente**. |

Os quatro XMIs do Neo4j são o **mesmo** dataset, gerado por
`scripts/gen_userprofiles_neo4j.py`; só mudam o `count` e o nome do schema. A
soma dos `count` das variações de `User` bate o volume gerado nas quatro (o
paradigma grafo tem núcleo próprio e o bug #8 não passa por ele) — é o que
torna a leitura integral verificável no grafo, e não no documento.

> Copie estes arquivos do repositório Java original / do oráculo em Docker
> (`oracle/`). São **entrada** do porte, não gerados por ele.

## De onde vem o dataset do Northwind

Os dados estão **versionados** em `resources/datasets/northwind/` (17 arquivos
JSONL, 304 KB) — proveniência completa no README de lá. Em resumo: vêm de
**<https://github.com/jasny/mongodb-northwind>**, a versão MongoDB do banco de
exemplo **Northwind** do Microsoft Access 2010, derivada do
[MyWind](https://github.com/dalers/mywind) (a versão MySQL). Autoria de Arnold
Daniels, **licença BSD 2-Clause** — o `LICENSE` está copiado junto, como ela
exige.

Ao contrário dos XMIs deste diretório, que são **saída** de referência, aqueles
são **entrada**: é o único dataset da fase que não se regenera por semente, daí
estar no repositório.

As transformações relacional → documento são **daquele** repositório, não do
U-Schema, e são justamente o que a Fase 3.1 mede: `_id` como chave primária de
toda coleção, `order_details` embutido como `details` em `orders`,
`purchase_order_details` idem em `purchase_order`, e `products.supplier_ids`
como lista de `int`. A entidade não-raiz `Detail` do invariante 19/17 nasce daí.

**Limitação a declarar: a entrada do oráculo não é publicada, só a saída.** Nos
dois clones Java existe **um único** arquivo mencionando Northwind — o
`es.um.uschema.mongodb2uschema/outputs/model_northwind.xmi`, que é a origem do
`model_northwind.xmi` daqui. Não há dados, script de carga nem lista de
coleções no repositório deles, então não se pode provar que usaram exatamente
este dataset. A evidência é indireta e forte: os `count` do XMI deles batem com
os nossos em **14 das 17** coleções, e as três que não batem falham pelo bug #8,
não por volume diferente — dataset diferente não se alinharia assim.

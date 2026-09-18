# TO-DO — Fase 3: validação ponta a ponta + volume

**Projeto:** Porte fiel e completo do U-Schema (Java/Spark/EMF) → Python — MongoDB e Neo4j
**Autores:** Davi Cavalcante · João — CESUPA
**Base:** `fase3_validacao_volume.md` · **Harness:** `validation/equivalence.py::compare` · **Bugs:** `bugs_originais.md`
**Pré-requisito:** Fases 0, 1 e 2 fechadas

> Esta fase não porta nada — **mede**. O produto é evidência: CSVs e tabelas. O
> código que entra é de bateria (orquestração, cronometragem, coleta).
>
> **Risco dominante: afirmar número que não foi medido.** Todo valor sai de uma
> corrida registrada, não de um `.md` anterior.

## Estado em 15/08/2026

| Bloco | Situação |
|---|---|
| **3.0** infra | esquema **revisado de novo** em 15/08 (`normalized`); implementado, **não medido** |
| **3.1** equivalência | **fechado** — Northwind (dois caminhos) e grafo (4 tamanhos × oráculo semeado); Sakila descartado |
| **3.2** volume | **a remedir** — o `results/` atual é anterior às colunas novas |
| **3.3** bugs | **fechado** — #6/#7 sem patch em volume, #8 medido dos dois lados |
| **3.4** análise | não começou — depende da remedição |

**Todos os números deste documento saíram de sessões de medição no kernel
`6.17.0-40`**, com as sementes 23, 69 e 207. Nenhuma tabela mistura ambientes.

---

## Sessão de 08/08/2026 — o `E1` era nosso, e reabre a §3.2

Duas tentativas de rodar o ensaio do esquema novo travaram no grafo, com
`--settle 30` e com `--settle 60`. A investigação achou a causa, que **não** é a
que estava catalogada:

`_read_label_combination` lia com `driver.execute_query`, que é *eager*.
Em resultado grande a lista cresce, o processo para de drenar o socket, a janela
TCP fecha e o servidor fica bloqueado — `rwnd_limited: 100,0%`, vazão em
~47 KB/s. **Defeito do porte, corrigido** (gerador sobre `session.run()`): o
`large` passou de "não terminou em 15 min" para **121,0s**. Detalhe, medição e
o que o diagnóstico antigo errava em `bugs_originais.md` §**E1**.

**O `E1` é intermitente, não sistemático.** Quando não dispara, a extração roda
na velocidade certa; quando dispara, contamina aquela corrida isolada. Foi
verificado remedindo depois da correção — os tempos batiam com os anteriores
dentro de 2% —, mas os CSVs dos dois lados foram apagados, então **a série tem
de ser restabelecida** antes de qualquer afirmação sobre magnitude.

A correção reduziu muito a frequência e **não a zerou**: a bateria pós-correção
ainda produziu um tamanho `small` uma ordem de grandeza acima do normal. A regra
da mediana continua necessária.

**A equivalência nunca esteve em risco:** o `E1` é de vazão, não de conteúdo —
mesmos registros, mesma ordem. Confirmado por medição: o modelo construído com
o código corrigido dá **zero divergências** contra o XMI que o próprio porte
gerou antes da correção, sobre a mesma instância.

## Sessão de 15/08/2026 — semente única, tabela do oráculo e `normalized`

Quatro mudanças de esquema, decididas nesta ordem:

**Semente única.** `--seed` deixou de ser obrigatório: `DEFAULT_SEED = 23` em
`scripts/output.py`, importado pelos quatro scripts e lido pelo `run_suite.sh`,
sobrescrevível. A **coluna `seed` saiu** de `runs.csv`; a
semente continua no `run_id`, porque é ela que separa duas corridas do mesmo
alvo — sem ela a chave duplicaria em silêncio. As 24 corridas das sementes 69 e
207 foram descartadas.

> **Consequência declarada:** a reprodutibilidade entre sementes (a amplitude de
> 0,37pt do #8, a dispersão do grafo) deixa de ser rotina. Vira medição pontual,
> com `--seed`, quando se quiser.

**Tabela do oráculo.** `oracle.csv` (`run_id`, `total_time`, `normalized`). Com
ela, `runs.csv` perdeu a coluna `producer`, as três parcelas de tempo deixaram
de ser vazias em metade das linhas, `total_time` voltou a ter **uma** definição
e `run_id` voltou a ser chave sozinho. O `producer` **desapareceu do esquema**, e
o lado a lado que sustentava o "#8 replicado" passou a existir só no log das
baterias.

**`experiment`: `head_to_head` → `oracle_chain`.** Os três valores nomeavam
coisas de tipos diferentes — dois propósitos e um método. Substituição textual,
194 ocorrências, sem remedir.

**Três colunas e uma tabela saem.** `triple_rows` e `archetypes` deixam de ser
gravadas, e a tabela `cleanup.csv` some: a limpeza é custo de ambiente, não
medida do porte nem do oráculo. O `clean_databases.py` continua como utilitário.

> **O que isso custa, declarado:** o contraste "13 → 421 esquemas distintos no
> documento contra 9 arquétipos constantes no grafo" era a coluna
> `triple_rows`/`archetypes`, e deixa de ser registrado. Para citá-lo, é preciso
> reconstruí-lo da extração numa corrida feita para isso.

**`query_time` e `normalized`.** A métrica do artigo, definida lá como *"the
normalized value (inference time divided by query time)"*. Exige medição nova: a
query de referência (média de filmes assistidos por usuário) não existia em
nenhuma bateria. Texto das queries e justificativa em `scripts/baseline.py`.

- **O dividendo é o `total_time`**, não o `inference_time`: o que o artigo chama
  de *inference* é o pipeline inteiro, e a nossa coluna homônima é ~0,00s.
- **A query roda por último**, depois do porte e do oráculo, para não aquecer o
  cache da extração medida.

### O `results/` atual não serve mais

As 26 corridas de 08/08 são anteriores a `query_time`/`normalized`, e esses
valores **não são retroativos** — a query nunca rodou naquelas instâncias. A
guarda de cabeçalho recusa anexar. **A fase precisa de uma remedição completa**
com o esquema atual:

```bash
./scripts/run_suite.sh
```

### Um número a conferir antes de publicar

O `normalized` do grafo `larger`, calculado com o `total_time` de 08/08 e a
query rodada em 15/08, dá **474,63×** contra os **9,13×** da Table 4 do artigo.
A diferença se decompõe em dois fatores que se multiplicam:

| | artigo | nosso | fator |
|---|---|---|---|
| pipeline | 109,7s | 437,6s | 4,0× |
| query de referência | 12,0s | **0,92s** | **13×** |

O pipeline 4× mais lento já era conhecido. Os 13× da query são novos, e há duas
explicações possíveis, **não separadas**: (a) a máquina — i9 de 2023 contra i7
de 2015, e a normalização só cancela o ambiente se numerador e denominador
escalarem juntos; (b) o plano de execução — 0,92s para 7,1M arestas sugere que o
Neo4j resolve o `count` por grau armazenado, sem travessia, enquanto os 12s do
artigo sugerem travessia real. O próprio artigo diz que a query *"is, by chance,
easily optimized by the database"*.

**Um `PROFILE` na query resolve (b).** Enquanto não for feito, o `474×` não deve
entrar em tabela.

## Revisão do esquema dos CSVs (decidida em 08/08/2026)

**O esquema foi refeito e implementado, e o dicionário de dados é
[`dicionario_de_dados.md`](dicionario_de_dados.md).** A §3.0 abaixo descreve o
formato **anterior**, que não existe mais em disco.

**Por quê.** Lendo os CSVs uma semana depois, não havia como saber o que era
medida do porte e o que era do oráculo. O `t_oraculo` era a única coluna de
outro sujeito dentro de uma tabela de medidas nossas, e as mensagens de
divergência falam em `Schema1`/`Schema2` — convenção que existe e é consistente
(`compare(referencia, porte)` nos cinco pontos de chamada), mas que mora no
código, não no dado. Decisão do Davi: **resolver na origem, não derivar na EDA**.

O que muda:

- **coluna `producer`** (`port`/`oracle`), que leva `runs.csv` ao formato longo:
  uma corrida `oracle_chain` passa a dar duas linhas;
- **nomes de coluna em inglês** — a justificativa do português era não invalidar
  arquivos já produzidos, e a regravação a derruba;
- **`cleanup.csv`** nasce, porque a limpeza não é medida de produtor nenhum;
  **`t_geracao` sai** (já refutou o que tinha para refutar) — e em 15/08 a
  própria `cleanup.csv` sai também, ver abaixo;
- **`write_time`** entra, e o `total_time` passa a ter a mesma definição dos
  dois lados: do banco até o XMI em disco.

Estado: `runs` fechada; `comparisons` e `divergences` em aberto. A `cleanup` e a
`entities` foram removidas em 15/08 — não há esquema a fechar nelas.

### Antes de regravar

**Arquive qualquer `results/` ativo** antes de começar: a guarda de cabeçalho
recusa anexar em esquema diferente, e um `results/` de ensaio anterior geraria
`run_id` duplicado.

**A remedição tem de ser completa.** Com o `E1` corrigido, nenhum tempo de
grafo medido antes de 08/08 vale para `large`/`larger`, e os dois ensaios
parciais deste esquema foram interrompidos justamente ali. O que fecha a fase é
a **cadeia inteira numa semente** (~25 min), rodada até o fim — não o ensaio
interrompido no meio.

> **Não são três sementes.** A varredura de 23/69/207 deixou de ser rotina na
> sessão de 15/08 (§Sessão de 15/08), junto com a coluna `seed`; as 24 corridas
> das outras duas foram descartadas. Repetir com `--seed` é medição pontual,
> para quando um número parecer fora da curva — não requisito do gate.

### Os comandos

A cadeia completa numa semente (~25 min). O `run_suite.sh` roda exatamente isto,
numa semente por invocação (`./scripts/run_suite.sh [semente]`, sem argumento
vale o `DEFAULT_SEED`); as chamadas abaixo são a forma manual, para rodar uma
bateria isolada ou retomar a cadeia do meio:

```bash
uv run python scripts/run_northwind.py &&
uv run python scripts/run_oracle_mongo.py --seed 23 &&
uv run python scripts/run_oracle_neo4j.py --seed 23 &&
uv run python scripts/run_size_mongo.py --seed 23 &&
uv run python scripts/run_size_neo4j.py --seed 23
```

**O que conferir quando terminar:**

```bash
wc -l results/*.csv
awk -F, 'NR>1 && $3=="seeded_oracle"{print $1, $4, $5}' results/comparisons.csv
awk -F, 'NR>1{print $1}' results/runs.csv | sort | uniq -d       # tem de sair vazio
```

A checagem de duplicata é sobre **`run_id` sozinho**. O `producer` do formato
longo não existe mais: era ele que deixava uma corrida `oracle_chain` dar duas
linhas com o mesmo `run_id`, e hoje o oráculo tem tabela própria (`oracle.csv`),
então em `runs.csv` cada corrida é uma linha só. Em `comparisons.csv` a chave é
`run_id` + `reference`.

O `uniq -d` é conferência do que já está em disco — a gravação em si já recusa
chave repetida (`KEYS` em `scripts/output.py`), então a duplicata não chega a
entrar no arquivo.

Esperado: `equivalente=True` em tudo; **zero** divergências no grafo contra o
oráculo semeado; no documento, `True` com divergências **não-fatais de `count`**
(assinatura do #8 dos dois lados, caindo em lugares diferentes porque a ordem de
leitura do cursor `pymongo` não é a do conector Spark).

**O `E1` foi corrigido em 08/08/2026** e não deve mais aparecer. Se o log
parar no `=== seed X | tamanho Y ===` sem emitir a linha de tempos, diagnostique
pela **vazão do socket**, não por `SHOW TRANSACTIONS`:

```bash
ss -tni state established '( sport = :7687 )' | grep -o "rwnd_limited:[^ ]*"
```

`rwnd_limited` alto significa que o **cliente** parou de ler e o servidor está
esperando — a assinatura do `E1`. Detalhe em `bugs_originais.md` §**E1**.

**Depois que os CSVs estiverem no lugar, o que resta é a 3.4** — coletar as
métricas e compará-las com o artigo, gerando as tabelas do `results/` em vez de
transcrevê-las à mão.

## Regras da fase

1. **O critério é casar com o oráculo, não com o volume real.** O #8 é replicado de propósito (`bugs_originais.md` §#8: "Decisão no porte: replicar"), e o oráculo também não o corrige — `oracle/patches/` tem `0001`, `0004`(×2), `0005`, `0006`, `0007`, **não existe `0008`**. Um resultado que "acertasse" o volume real no paradigma documento estaria **fora** do gate. O 50/50 do User Profiles e o 17/17 do Northwind são do experimento original **com** a correção: contexto do que o bug custa, nunca alvo.
2. **Todo `count` publicado vem com o caminho de extração que o produziu** — a quantidade de divergências do #8 é ordem-dependente.
3. **Todo par de números declara dataset e tamanho.** Já se chegou a contrastar grafo `larger` (800k) com Northwind (397 documentos).
4. **Comparar tendência, nunca tempo absoluto** — Python × JVM, e o baseline do artigo é outra máquina.

---

## 3.0 — Infra de bateria — **FECHADO**

> O guia assumia "ler o tempo de inferência do log do Spark". Não existe log de
> executor: a 2.0 decidiu driver nativo, Python. A medição é em processo.

### Scripts

| Script | Papel |
|---|---|
| `output.py` | escrita das quatro tabelas de resultado — usado por todas as baterias |
| `run_size_mongo.py` | bateria por tamanho do documento (2 rotas × 4 tamanhos) |
| `run_size_neo4j.py` | bateria por tamanho do grafo (4 tamanhos), compara com `resources/` |
| `run_oracle_mongo.py` | cadeia porte × **oráculo semeado** no documento (`--kind mongodb`) |
| `run_oracle_neo4j.py` | cadeia porte × **oráculo semeado** no grafo: regera, roda os dois lados sobre a mesma instância, compara |
| `run_suite.sh` | encadeia **todas** as baterias numa semente, sequencialmente; recusa repetir semente já gravada |
| `baseline.py` | query de referência (média de filmes por usuário) — o divisor de `normalized` |
| `run_northwind.py` | equivalência do Northwind pelos dois caminhos de leitura (arquivo e banco) |
| `check_northwind_invariants.py` | invariantes estruturais do Northwind, lidos do XMI |
| `gen_userprofiles{,_neo4j}.py` | geradores, ambos com `--seed` |
| `clean_databases.py` | apaga só os `up_*` e o grafo — nunca o `northwind` |

O módulo se chama `output.py` e não `results.py` porque o diretório de saída na
raiz é **`results/`**: um módulo homônimo vira *namespace package* e o mypy passa
a resolver o import para a pasta de dados.

- [x] **Cronometragem em processo**, separando **extração** (I/O do driver) de **inferência+construção**. Resultado: nos dois paradigmas a inferência é ≤0,05s e **o custo é todo de extração** — o rótulo "tempo de inferência" do artigo mede, na prática, extração.
- [x] **Sem `cli.py`/`[project.scripts]`.** Um script por bateria com `argparse`. A entrega é a equivalência demonstrada, não uma ferramenta de linha de comando.
- [x] **Baterias não rodam em paralelo.** Os clientes Python não disputam, mas mongod e Neo4j disputam CPU e disco. No grafo nem cabe paralelismo: Community tem um banco só.
- [x] **Guarda de cabeçalho** nos CSVs: recusa anexar num arquivo de esquema antigo em vez de corromper em silêncio.
- [x] **`--settle` removido em 08/08/2026** — flag, constante e o `SETTLE` da suíte. Foi adicionado em 02/08 para "deixar o servidor assentar" depois de uma deleção massiva; a investigação de 08/08 mostrou que o `E1` não é do servidor e não tem relação com a deleção — é a leitura *eager* do nosso extrator (`bugs_originais.md` §**E1**, corrigido). Falhou nas duas tentativas em que foi exigido, com 30s e com 60s, e custava ~6 min por suíte de três sementes. A limpeza segue acontecendo, impressa no log; deixou de ser gravada em 15/08.

### Formato dos CSVs — fixado em 02/08/2026, **superado em 08/08**

> **Esta subseção descreve o formato ANTIGO**, que não existe mais em disco. A
> referência do esquema é
> [`dicionario_de_dados.md`](dicionario_de_dados.md) — ver "Revisão do esquema
> dos CSVs" no topo. O que segue continua valendo como registro de *por que* o
> formato foi para uma tabela por grão, decisão que a revisão mantém.

O esquema abaixo é a referência do formato.

**Refeito em 02/08/2026: uma tabela por grão.** A primeira versão dava um CSV
por bateria, o que misturava três granularidades no mesmo arquivo — os tempos da
corrida repetidos em cada linha de entidade, o veredito repetido em cada linha
de divergência — e obrigava a deduplicar antes de analisar. Pior: o mesmo fato
saía *long* numa bateria e *wide* noutra. Escrita em `scripts/output.py`.

| Arquivo | Grão | Chave |
|---|---|---|
| `corridas.csv` | uma corrida | `corrida_id` |
| `entidades.csv` | uma entidade por corrida | `corrida_id` + `entidade` |
| `comparacoes.csv` | um confronto com um XMI de referência | `corrida_id` + `referencia` |
| `divergencias.csv` | uma divergência | `corrida_id` + `referencia` |

O `corrida_id` é determinístico — `tamanho-mongodb-up_a_small-23`,
`oraculo-neo4j-movies_min-23`, `equivalência-mongodb-northwind-arquivo` —, montado
a partir de bateria, paradigma, alvo, semente e origem. A **bateria** entra na
chave porque o mesmo tamanho com a mesma semente é medida duas vezes, na bateria
de tamanho e na cadeia do oráculo, e são corridas distintas.

Decisões de coluna:

- **`origem`** (`arquivo`/`banco`) e **`referencia`** (`resources`/`oraculo_semeado`) são o que torna as linhas comparáveis: o #8 é sensível ao caminho de leitura, e o mesmo dataset confrontado com referências diferentes dá resultados diferentes **de propósito**.
- **`capturado` saiu** — é `modelo / real`, conta da análise, não dado.
- **`linhas_tripla` e `arquetipos` continuam separadas**, vazias no paradigma que não as produz. Fundi-las esconderia que os dois não passam pelo mesmo núcleo de construção.
- **`pico_memoria` não entra**: nenhuma corrida mediu isso, e coluna vazia num CSV de evidência é pior que coluna ausente.

### Máquina de referência

| | |
|---|---|
| **CPU** | Intel Core i9-14900K — 24 núcleos (8P + 16E), 32 threads, até 6,0 GHz, L3 36 MiB |
| **RAM** | 64 GB (62 GiB úteis) + swap de 2 GB em arquivo |
| **Disco** | NVMe Kingston SNV3S1000G 931,5 GB, ext4 na raiz (os bancos moram aqui) |
| **SO** | Linux Mint 22.3 |
| **Kernel** | **6.17.0-40-generic** — o pin da fase |
| **Python** | 3.12.3 (uv 0.11.14) |
| **MongoDB** | 8.0.29 · **Neo4j** 2026.06.0 Community |
| **Docker** | 29.7.1 · imagem `extrator-uschema`: Temurin JDK 1.8.0_492, Maven 3.9.16, Spark 3.0.1 |

Ressalvas que mudam a leitura dos tempos:

1. **O `6.17.0-40` é o único kernel onde as duas metades rodam** — o `mongod` não sobe no 7.0.0-28 (ver Riscos), e o Neo4j sobe nos dois. Por isso o pin. As corridas anteriores, feitas no 7.0.0-28, foram descartadas e refeitas aqui.
2. **Equivalência não depende de kernel nem de máquina** — `equivalent=True`, as zero divergências e o 19/17 do Northwind saem da semente e do código. Só a tabela de **tempo** é sensível ao ambiente.
3. **O baseline do artigo é um i7-6700 de 2015**, com 48 GB e SSD — lido no fonte em 15/08/2026 (§Evaluation: *"an Intel(R) Core(TM) i7-6700 CPU @ 3.40GHz with 48 GB of RAM and using SSD storage"*). Este documento afirmava "também é um i9, de geração não informada", o que era **falso**. São nove gerações de diferença para o i9-14900K daqui, e é por isso que a comparação absoluta não se sustenta — e que a coluna `normalized` existe.
4. **Bancos e saídas em sistemas de arquivos diferentes:** os bancos em `/var/lib/...` (ext4 no NVMe, sem cifra); o repositório, e portanto `out/` e `results/`, em `/home/davi`, que é **eCryptfs**. A extração mede I/O sem cifra; a escrita dos XMIs passa pela camada cifrada.
5. **Governor `powersave` com turbo ligado** — frequência não fixa. Mantido de propósito (medir a máquina como ela é usada), mas explica parte da dispersão entre sementes.
6. **A identidade da imagem do oráculo não precisa entrar no CSV:** o que determina o XMI é o fonte, pinado por SHA no `Dockerfile`. O único resíduo é a tag base `maven:3.9-eclipse-temurin-8`, que flutua e mexe marginalmente no `t_oraculo` — e as versões que ela entrega estão na tabela acima.

---

## 3.1 — Equivalência

### Northwind — **fecha**

> **Nem toda divergência do Northwind é de `count`.** Das 15 do caminho
> `arquivo` e das 12 do `banco`, **5 de cada** são de categoria `variation` —
> variação da referência sem par do nosso lado, todas em `Orders` e
> `Purchase_orders`. Continuam não-fatais e continuam sendo consequência do
> colapso do #8, mas "todas na assinatura do #8" sugere uma homogeneidade que o
> dado não tem. Contra o oráculo semeado, aí sim é 100% de `count`.

- [x] **`equivalent=True` pelos dois caminhos de leitura**, com só divergências não-fatais, todas na assinatura do #8. 17 coleções, 397 documentos, **49 linhas de tripla nos dois** (`scripts/run_northwind.py`, evidência em `results/comparacoes.csv`).
- [x] **A ordem-dependência do #8 está medida numa corrida só, com os dois caminhos lado a lado** — 15 divergências lendo os arquivos, 12 lendo o cursor do Mongo, mesmo dado:

| | arquivo | banco (cursor) |
|---|---|---|
| divergências | 15 | 12 |
| coleções fechando | **14/17** | **14/17** |
| `orders` (48 reais) | 24 | 38 |
| `products` (45 reais) | 40 | 40 |
| `purchase_orders` (28 reais) | 22 | 23 |

- [x] **O que é invariante e o que não é** — a distinção que o número sozinho esconde: **quais** coleções o #8 atinge não muda (sempre `orders`, `products`, `purchase_orders`, sempre 14/17 fechando), e `products` até captura o mesmo 40 nos dois. O que muda é **quanto** ele come nas outras duas. Citável: `equivalent=True` + não-fatais + 14/17. Não citável sem dizer a origem: o número de divergências e o `count` de `orders`.
- [x] **SHA-256 do dataset registrado** na saída do script (`3700157b…`), já que os JSONs não são versionados e não têm outro identificador.
- [x] **Invariantes estruturais confirmados** (`check_northwind_invariants.py`, 02/08), idênticos no XMI do oráculo e no do porte: **19 `EntityType`**, **17 raiz**, as duas não-raiz sendo `Detail` e `_id`, e `Aggregate` para `Detail` com `upperBound=-1`/`optional=true` em `Orders` e `Purchase_orders`.
  - **Não é redundante com o `compare()`:** o harness afirma que os dois lados são **iguais**, não **o que** o modelo contém. Se `Detail` sumisse dos dois, ele seguiria dando `equivalent=True`. E o invariante é **livre de contagem**, então escapa da ordem-dependência do #8.
  - **Vale por variação, não por entidade:** 9 `Aggregate` para `Detail` em `Orders` e 8 em `Purchase_orders`, um por variação que tem `details`.
  - **Achado: `fase3_validacao_volume.md:42` erra ao dizer "17 coleções, `_id` inteiros".** `sales_reports` e `strings` têm `_id` **objeto** — é daí que sai a entidade não-raiz `_id`, agregada com `upperBound=1`/`optional=false`, forma oposta à do `Detail`.
- [x] **Proveniência do dataset levantada (02/08/2026)** — detalhe em `resources/README.md`, "De onde vem o dataset do Northwind":
  - Os JSONs vêm de **<https://github.com/jasny/mongodb-northwind>** (Arnold Daniels, 2020): a versão MongoDB do banco de exemplo **Northwind** do Microsoft Access 2010, derivada do MyWind (MySQL). O clone local está no commit inicial `967bfec`.
  - **Licença BSD 2-Clause** — redistribuir em forma de fonte é permitido mantendo o aviso de copyright e o disclaimer. **Versionar os 17 arquivos neste repo é legítimo**, levando o `LICENSE` junto.
  - As transformações relacional → documento são daquele repositório, não do U-Schema: `_id` como chave primária de toda coleção, `order_details` embutido como `details` em `orders` (e idem em `purchase_order`), `products.supplier_ids` como lista de `int`. **A entidade `Detail` do invariante 19/17 nasce daí.**
  - **A entrada do oráculo não é publicada, só a saída.** Nos dois clones Java existe um único arquivo citando Northwind — o `outputs/model_northwind.xmi`, origem do nosso `resources/mongodb/model_northwind.xmi`. Sem dados, sem script de carga, sem lista de coleções. Não se prova que usaram este dataset; a evidência é indireta — 14/17 coleções com `count` idêntico, e as 3 restantes falhando pelo #8, não por volume.
- [x] **JSONs versionados em `resources/datasets/northwind/`** (02/08/2026, decisão do Davi): os 17 arquivos (304 KB) mais o `LICENSE` exigido pela BSD 2-Clause e um `README.md` com proveniência, commit de origem e o digest. O `run_northwind.py` passou a ler daí por padrão e reproduz **o mesmo SHA-256 e o mesmo resultado** da cópia externa. **O único dataset da fase que não se regenera por semente agora está preso ao repositório.**
- [x] **Decidido: o Northwind NÃO vira teste de CI.** Com os dados versionados o caminho `arquivo` roda offline, então o teste seria viável (~2s, sem banco) — e foi por isso que se cogitou. **Recusado porque a intenção declarada é corrigir os bugs catalogados no futuro:** um teste afirmando `14/17` cimentaria em CI justamente o comportamento que se pretende consertar, e viraria alarme falso no dia da correção. O resultado continua sendo produzido sob demanda pelo `run_northwind.py`, com CSV.
- [x] **Decidido (02/08/2026): a "variação estrutural sobre o aninhado" (`fase3_validacao_volume.md:45`) NÃO entra no `check_northwind_invariants.py`.** O que ela descreve foi investigado no fonte e é **design do original, não defeito**: o agregado é nomeado pelo campo, sem o caminho, então `orders.details` e `purchase_orders.details` colapsam num único `Detail` com 5 variações em duas famílias disjuntas. Evidência de linha em `bugs_originais.md`, "O que **não** é defeito". Sem defeito contra o que proteger, não há verificação a acrescentar.
  - **Limitação que fica declarada:** o `compare_aggregate` casa as variações agregadas **só pelo nome do container** (deliberado, é o que evita recursão em agregado cíclico), então o mapeamento variação-pai → variação-filha é a única parte do modelo que a equivalência estrutural não verifica. Nenhuma evidência de que divirja — apenas não é coberto.

### User Profiles / grafo — **fecha nos quatro tamanhos**

`run_oracle_neo4j.py --seed 23` roda os dois lados sobre **a mesma instância** —
mesma entrada, duas implementações. É o padrão-ouro da equivalência do grafo.

| tamanho | `User` | `Movie` | vs oráculo semeado | vs `resources/` |
|---|---|---|---|---|
| small (100k) | 100.000 — 100% | 50.000 — 100% | **True, 0** | True, 7 não-fatais |
| medium (200k) | 200.000 — 100% | 100.000 — 100% | **True, 0** | True, 7 não-fatais |
| large (400k) | 400.000 — 100% | 200.000 — 100% | **True, 0** | True, 7 não-fatais |
| larger (800k) | 800.000 — 100% | 400.000 — 100% | **True, 0** | True, 7 não-fatais |

- [x] **9 arquétipos nos quatro tamanhos** — a estrutura não muda com o volume. **Leitura integral confirmada**: soma dos `count` == volume gerado, contra banco real. O grafo tem núcleo próprio; o #8 não passa por lá.
- [x] **As 7 não-fatais contra `resources/` não são defeito do porte** — são de `count`, nas 5 variações de `User` mais `WATCHED`/`FAVORITE`, dentro do ruído de amostragem. Os XMIs de `resources/` vêm de uma instância de **semente desconhecida**; contra a mesma instância, zero. A variável isolada é o dataset.
- [x] **É a extração do grafo que passou a ser confrontada com o oráculo.** A 2.2 comparava arquétipos **reconstruídos a partir do próprio XMI**; aqui eles vêm do banco, por bolt.
- [x] **O container aguenta o `larger`** — 10,2M arestas com `--memory=6g`, sem estouro. Era o risco declarado: a 0.5 só tinha exercitado `--kind neo4j` em grafo mínimo.
- [x] **Achado que destrava o Neo4j Community** (lido no `Neo4j2USchema.java` do SHA pinado): o `--db` **não é usado para conectar** — o `SparkProcess` recebe só `(samplingRatio, bolt, user, password)` e sempre lê o banco padrão. O nome vai para `Json2USchemaModel`, ou seja, é o **nome do schema**. Consequência: um único banco de usuário não é obstáculo, mas o valor tem de casar com o do porte — divergência de `SCHEMA_NAME` é **fatal** no harness.
- [x] **`N1` não disparou** e não podia: nenhum nó multi-label no dataset (`size(labels(n)) > 1` = 0). Segue sem confirmação empírica.
- [x] **Três pendências fechadas como dispensáveis (02/08/2026), decisão do Davi:**
  - **Nenhum XMI-oráculo semeado é promovido para `resources/`.** Aquele diretório é a amarra com o experimento publicado e é imutável (`resources/README.md`); XMI nosso lá borraria a distinção que a fase usa como método. Os quatro ficam em `out/oraculo/`, regeneráveis por semente + imagem.
  - **Dono `root` dos XMIs do oráculo: não corrigir.** Arquivos legíveis, em `out/`, fora do git, regeneráveis.
  - **Sem segunda semente.** O gate não pede — a equivalência é estrutural, não estatística, e uma semente já deu zero divergência nos quatro tamanhos.

### Sakila — **descartado em 02/08/2026**

Levantamento do que existe publicado, antes de decidir:

| Repositório | Licença | Formato | Modelagem |
|---|---|---|---|
| `lilhuss26/sakila25` | **MIT** | dump `mongorestore`, 3 coleções, ~200 KB | aninhada (`films` com actors/categories, `customers` com address/payments) |
| `SouthbankSoftware/dbkoda-data` | nenhuma | dump BSON, 4,8 MB | aninhada (porte do Guy Harrison) |
| `Ciges/MongoDB_Sample_Databases` | nenhuma | `sakila.tar.bz2`, 345 KB | migração 1:1 do MySQL, plana |
| `vitorecarpe/Sakila-NoSQL` | nenhuma | CSV + script | aninhada; **é o único com Neo4j** |

- [x] **Decidido: não entra.** Existe versão MongoDB publicada e licenciada (`sakila25`, MIT), mas **em grafo não existe dataset nenhum** — o único candidato é um pipeline acadêmico que exige montar o Sakila no MySQL e converter, o que produziria um grafo **nosso**, não de terceiros. E o `sakila25` não é o Sakila clássico: é o esquema repovoado em 2025 com dados da API do TMDB.
- [x] **Limitação assumida, e ela é a mais séria da fase:** a validação de equivalência usa **um único dataset real**, o Northwind — e só no paradigma documento. O grafo é validado **inteiramente sobre dado sintético** gerado por script nosso (`gen_userprofiles_neo4j.py`), com 9 arquétipos e nenhum nó multi-label, que é por que o `N1` nunca disparou. Consequências a declarar sem rodeio na 3.4:
  - o *overfitting* ao Northwind **não está descartado** no documento;
  - nenhuma corrida do grafo encontrou estrutura que não tenha sido desenhada por nós;
  - o que sustenta a generalização não é variedade de dataset, e sim a **variedade de caminhos**: dois paradigmas, dois núcleos de construção, duas origens de leitura no Northwind, e o confronto com o oráculo Java sobre a mesma instância.

**Saída:** CSV de equivalência cobrindo Northwind (dois caminhos de leitura) e User Profiles (4 tamanhos, grafo), com toda divergência fatal explicada por bug catalogado.

---

## 3.2 — Volume

Quatro tamanhos: 100k/200k/400k/800k `User` (50k/100k/200k/400k `Movie`).
**Rota A** com `_id` ObjectId nativo; **Rota B** com `_id` inteiro e ~15% de
arrays vazios (cenário relacional→NoSQL, que exercita #6 e #7).

> **Aviso sobre os números derivados desta seção.** Só valem como registrados os
> valores que estão **crus no CSV**: tempos por corrida, contagens, percentuais
> de captura, `linhas_tripla`, vereditos. Tudo que é **derivado** — as medianas
> entre sementes, os fatores de crescimento (18,0× · 15,3× · 26,3× · 5,25× ·
> 3,57× · 2,08×) e as razões porte/oráculo (1,8× a 8,9×) — foi calculado **ad
> hoc durante a sessão de 02/08/2026, sem script versionado**, e portanto não
> atende ao critério de rastreabilidade que a própria fase impõe.
>
> **Os derivados abaixo são indicativos**, não resultado publicável: valem até a
> remedição, e são substituídos pelo que o script da §3.4 gerar do `results/`.

- [x] Gerar os 4 tamanhos, rodar Mongo (A e B) e grafo, coletar CSV.
- [x] **Leitura integral** — fecha no grafo (100% em todas as corridas), **não** fecha no documento, e não deve: é o #8 replicado.
- [x] **Sem `OutOfMemoryError`/`MemoryError`** em nenhuma corrida, apesar de rodar em Python nos 800k. Se um dia estourar, `mapPartitions` entra sem reescrever a lógica (o `reduce_pairs` é comutativo e associativo, provado na 2.0) e os testes novos levam `@pytest.mark.spark`.
- [x] **Extração domina a geração nos quatro tamanhos do grafo** — 6,18 vs 16,36s (`small`), 15,88 vs 36,78s (`medium`), 49,85 vs 120,34s (`large`), 173,41 vs 429,85s (`larger`). A afirmação do guia ("a geração custa mais") **não se sustenta**: a evidência que a sustentava vinha de um cronômetro que incluía apagar o grafo anterior, por causa do `--drop`. Hoje a limpeza é etapa própria, com coluna `t_limpeza`.
- [x] **`t_limpeza` de uma linha é o custo de apagar o grafo da linha anterior** — 2,58s (apagando `small`), 7,29s (`medium`), 26,47s (`large`), ~110s (`larger`, 10,2M arestas). Ler ao contrário inverte a conclusão.
- [x] **A Rota B é ~2× mais rápida que a A** (14,52 vs 30,07s no `larger`). O artigo também tem B mais rápida, mas por ~15%. Suspeita: o ObjectId da Rota A exige extrair `generation_time` e montar o agregado `{"$oid": …}` por documento.

### Documento — subcontagem do #8 por tamanho (semente 23)

> **Os números foram apagados com os CSVs.** O que fica é a **forma** do
> achado, que a remedição tem de reencontrar: na Rota A a massa capturada é
> praticamente constante entre os tamanhos enquanto o volume cresce 8×, então o
> percentual despenca sem que o #8 "coma" proporcionalmente; a Rota B captura
> bem mais que a A em todo tamanho; e `linhas_tripla` cresce ~32× de `small` a
> `larger` no documento.

`Movie` capturou **100%** nas 24 corridas — é o controle interno. Análise em §3.3.

### Reprodutibilidade entre sementes — **fora de escopo desde 15/08**

A convenção passou a ser **semente única** (§Sessão de 15/08). A varredura de
três sementes que sustentava esta seção não é mais rotina, e os CSVs dela foram
apagados. Para voltar a afirmar reprodutibilidade entre sementes, remedir com
`./scripts/run_suite.sh 69` e `207`.

### O tempo do grafo tem outliers esporádicos de 2–5× — usar mediana

Ao contrário do documento, a extração do grafo tem corridas isoladas muito
acima do modo, **sempre nos tamanhos menores** — e não é o volume que
desestabiliza: o `larger` é o tamanho mais reprodutível de todos.

> Os números que sustentavam esta seção foram apagados com os CSVs. A regra e a
> causa seguem; as magnitudes têm de ser remedidas.

- [x] **A causa foi identificada em 08/08/2026, e não é a que estava escrita aqui.** Este bloco atribuía os outliers a "trabalho de fundo do servidor após deleção massiva". É o `E1`, e o `E1` é **defeito do porte**: a leitura *eager* (`driver.execute_query`) enchia o buffer, fechava a janela TCP e derrubava a vazão a ~47 KB/s. Corrigido — `bugs_originais.md` §**E1**.
  - **A deleção era coincidência de ordem.** As baterias vão `small`→`larger`, então a maior deleção sempre precede a maior extração. O que decide é o **tamanho do resultado**.
  - **A regra da mediana continua necessária.** Ela existia para descartar outliers de causa desconhecida; a correção do `E1` reduziu muito a frequência, mas **não a zerou**. Com a semente única, "mediana de 3" vira repetir a corrida quando o número parecer fora da curva.

### Tendência: preservada em direção, **não** em fator de crescimento

Porte × oráculo sobre **a mesma instância** (semente 23). O `t_porte` é a
mediana das 3 sementes; o `t_oráculo` vem de `run_oracle_neo4j.py`.

> **Tabela apagada com os CSVs.** A forma a reencontrar: a razão porte/oráculo
> **cresce com o tamanho** no grafo — o que descarta "Python é N vezes mais
> lento" como explicação, já que constante de linguagem daria razão constante.
> A magnitude tem de ser remedida, e agora com a coluna `normalized`, que é a
> forma comparável com o artigo.

- [x] **A razão porte/oráculo cresce com o tamanho — 1,8× a 8,9×.** Isso **descarta "Python é N vezes mais lento"** como explicação: fosse constante de linguagem, a razão não subiria.
- [x] **`t_oraculo` é relógio de parede do `docker run`**, não tempo de inferência: engole boot de Maven + JVM + Spark. Nesta sessão o custo fixo ficou visível — o `small` gastou 9,21s para 150k nós, quase tudo boot.
- [x] **Medido: o porte cresce mais que o oráculo nos dois paradigmas.**
  - **Documento:** as duas rotas do porte cresceram bem mais que 8× para 8× de dado; o oráculo, pouco mais que 8×. Magnitudes a remedir.
  - **Grafo:** o porte cresce **26,3×** para 8× de dado, contra **5,25×** do oráculo; em 2× de dado (`large`→`larger`), **3,57×** contra **2,08×** — o oráculo é praticamente linear, o porte não.
  - **Fato estrutural que restringe as explicações:** no documento o número de esquemas distintos cresce 13 → 421 (**32×**); no grafo são **9 arquétipos em todos os tamanhos**. Qualquer explicação única para os dois paradigmas esbarra nisso.
- [x] **Assimetria fechada em 02/08/2026: o documento também tem porte × oráculo na mesma máquina.** Até então o `8,7×` e o `8,1×` acima vinham das tabelas do artigo — um i7-6700 de 2015, medida que ninguém aqui reproduziu —, que é o confundidor eliminado no grafo e que no documento continuava de pé por não existir bateria equivalente. O `scripts/run_oracle_mongo.py` rodou as **oito** combinações, inclusive o `larger` com 800 mil documentos, e o risco declarado (o `--kind mongodb` do container só tinha visto os 397 do Northwind, na Fase 0.5) não se materializou:

> **Tabela apagada com os CSVs.** A forma a reencontrar: o oráculo é **quase
> plano** no documento — o boot fixo da JVM domina, e o trabalho real sobre 800
> mil documentos é uma fração dele —, enquanto o porte cresce com o volume. É
> daí que sai a inversão: nos tamanhos pequenos o porte é mais rápido que o
> container; na maior, não.

  - **`equivalent=True` nas oito, e as 59 divergências são todas de `count`** — nenhuma estrutural. É a assinatura do #8 dos dois lados, caindo em lugares diferentes porque a ordem de leitura do cursor `pymongo` não é a do conector Spark. Mesma ordem-dependência que o Northwind mostra entre arquivo e banco.
  - **O documento não é afetado pelo `E1`** — a leitura do Mongo não passa pelo extrator do grafo.

**Saída:** CSV de volume completo + curva tempo × volume, porte vs. oráculo.

---

## 3.3 — Bugs: #6/#7 por construção, #8 replicado

| Bug | No oráculo | No porte | O que a fase mede |
|---|---|---|---|
| **#6** `_id` inteiro | corrigido por **patch** (`0006`) | corrigido **por construção** | roda os 800k da Rota B sem `TypeError` |
| **#7** array vazio | corrigido por **patch** (`0007`) | corrigido **por construção** | roda os ~15% de `[]` sem `IndexError` |
| **#8** subcontagem | **não corrigido** (não há patch `0008`) | **replicado fielmente** | a subcontagem do porte casa com a do oráculo |

- [x] **#6/#7 demonstrados em volume.** A Rota B rodou os quatro tamanhos, até 800.000 `User`, **sem exceção e sem patch**. A afirmação do TCC é sobre o **mecanismo**, não o resultado: os dois lados chegam ao mesmo modelo; muda como se chega.
- [x] **#8 — a faixa do experimento original foi reproduzida nas duas pontas:** **2,62%** (A/`larger`) e **31,17%** (B/`small`), contra os "~2,6%–31%" registrados. Intervalo inteiro, não aproximação.
- [x] **O achado que vale mais que o percentual:** na Rota A a massa capturada é praticamente **constante** — 22.377 → 24.252 → 21.867 → 20.988 — enquanto o volume real cresce 8×. O #8 não subconta proporcionalmente: **trava num teto quase fixo**, e o percentual só despenca porque o denominador cresce. Citar "captura 2,6%" sem dizer o tamanho é citar um artefato.
- [x] **O gatilho está isolado sem ambiguidade:** `Movie` captura 100% em todas as corridas, e a única diferença para `User` é o array de tamanho variável — o `ArraySC.__eq__` que ignora tamanho.
- [x] **A estrutura sai correta, só a contagem é comida:** `User` tem as 2 variações certas; no `up_a_larger`, 420 linhas de tripla colapsam nelas e sobrevive só o `count` da primeira de cada grupo (954 e 20.059).
- [x] **Decidido (02/08/2026): não entra dataset mínimo versionado.** O item pedia uma fixture nova para #6, #7 e #8. O **#8 sai** pela mesma razão que barrou o teste do Northwind (§3.1) — corrigi-lo é intenção declarada, e travar a subcontagem em CI viraria alarme falso no dia da correção. **#6 e #7 saem por redundância**: já estão travados nas duas camadas onde o defeito ocorre, e a fixture só acrescentaria a cola entre elas, que o `test_mintest_golden_master.py` já exercita.

| Bug | Onde já está travado |
|---|---|
| **#6** | `tests/regression/test_objectid.py::test_id_nao_objectid_infere_sem_estourar` (regressão portada do JUnit) e `tests/unit/test_extractors_mongo.py::test_generate_document_pair_id_nao_object_id_usa_timestamp_zero` — a camada de **extração**, que é onde o Java estourava |
| **#7** | `tests/unit/test_builder.py::test_feature_from_array_vazio_nao_estoura_bug_7` — e o dado do teste é o `privileges` do Northwind, hoje versionado em `resources/datasets/northwind/` |
| **#8** | `tests/unit/test_schema_inference.py` — cobertura existente, mantida como está |

- [x] **Regra observada: o #8 não foi "consertado" no meio da bateria.** Nenhuma das 24 corridas do documento fechou com o volume real — se tivesse fechado, seria sinal de que o porte divergiu do oráculo, a investigar como regressão. Não é mais pendência: as baterias acabaram.

**Saída:** tabela de contagens por bug, com #6/#7 rodando sem patch em volume e a subcontagem do #8 casada com a do oráculo.

---

## 3.4 — Coleta e análise

Coletar as métricas e compará-las com o artigo. Nada além.

- [ ] Rodar `./scripts/run_suite.sh` e ter as quatro tabelas em `results/`.
- [ ] Uma tabela de comparação: `normalized` do porte, do oráculo e a Table 4 do artigo, por paradigma e tamanho. Gerada por script lendo o `results/`, não transcrita à mão.
- [ ] Redigir a avaliação: equivalência, volume e correções por construção.
- [ ] Declarar as **limitações**: a divergência de crescimento fica medida, não explicada; a equivalência do documento repousa sobre um único dataset real (Sakila descartado); a entrada do oráculo do Northwind não é publicada, só a saída; os CSVs ficam fora do git, e o que sustenta os números é a regeneração por semente; o `$numberLong` não tem fixture-oráculo; a comparação é estrutural, não byte a byte.

---

## Housekeeping

- [x] `resources/README.md` — o item pedia corrigir a descrição de `movies_min.xmi` ("modelo mínimo Neo4j", quando é o User Profiles **Small**, 100.000 `User`) e listar os `up_*.xmi`. **Já estava feito**; o item é que estava desatualizado.
- [x] **O buraco da guarda de cabeçalho foi fechado no lugar certo.** O item dizia que `run_size_mongo.py` e `run_size_neo4j.py` precisavam da correção cada um; ela acabou centralizada em `scripts/output.py` — arquivo de zero byte conta como novo (`:124`) e cada linha sai com `flush` imediato (`:176`) —, então vale para todas as baterias. Item estava desatualizado (conferido em 08/08).
- [x] **CSVs não serão versionados** (decisão de 02/08). `.gitignore` cobre `out/` e `results/`. Já custou uma vez: o CSV da bateria do grafo, apagado em 01/08, levou junto o respaldo da dispersão entre sementes.
- [x] **Saída separada por produtor:** `out/porte/`, `out/oraculo/`, `resources/` (versionado). Convenção em `resources/README.md`.
- [x] **Scripts em inglês** — `check_extraction_{mongo,neo4j}.py`, `check_northwind_invariants.py`. Seguem em português o diretório `results/` e os cabeçalhos dos CSVs; mudar agora invalidaria os arquivos já produzidos.
- [x] **`cli.py` não será criado** — decisão da 3.0, registrada no `CLAUDE.md`.

---

## Gate de aceite

> **Reaberto em 15/08/2026.** Os CSVs que sustentavam os itens marcados foram
> apagados, e o esquema mudou depois deles (`query_time`/`normalized`). Nenhum
> item está apoiado numa corrida registrada hoje — que é o critério que a própria
> fase declara como risco dominante. O que segue descreve **o que a remedição
> tem de reencontrar**, não o que está provado.

- [ ] **Equivalência:** Northwind fecha pelos dois caminhos de leitura (`equivalent=True`, só não-fatais, invariantes estruturais confirmados) e o grafo fecha nos 4 tamanhos contra o oráculo semeado com **zero divergências**. **Sakila descartado** — com a limitação declarada de que o documento fica com um dataset real só.
- [ ] **Volume:** os quatro tamanhos nos dois paradigmas, mais a cadeia do oráculo. Leitura integral no grafo em todas, subcontagem do documento reprodutível, sem estouro de memória.
- [ ] **Tendência:** o fator de crescimento medido nos dois paradigmas, e a comparação com a Table 4 do artigo pela coluna `normalized`. Não fecha com "curva preservada" arredondado.
- [ ] **Bugs #6/#7:** a Rota B tem de rodar os 4 tamanhos até 800k sem patch e sem exceção.
- [ ] **Bug #8:** a subcontagem do porte tem de casar com a do oráculo, com `Movie` a 100% como controle, e o Northwind na mesma planilha. **Nenhum** número do documento pode fechar com o volume real — se fechar, o porte divergiu do oráculo.
- [ ] **Rastreabilidade: todo número sai de uma corrida registrada.** É o item que a sessão de 15/08 zerou, e o que a remedição restabelece. **Exceção declarada:** os invariantes estruturais do Northwind (19/17, `Aggregate`) saem do `check_northwind_invariants.py`, que imprime e não escreve arquivo — são livres de contagem e reproduzíveis por um comando.

**Entregáveis:** `scripts/` (geradores + baterias), `results/` (CSVs), a tabela de comparação com o artigo e o material da avaliação experimental.

---

## Riscos

- **Afirmar número não medido** — os `.md` anteriores citam valores do experimento original, não do porte.
- **Citar número ordem-dependente como invariante** — aconteceu com as "15 divergências" do Northwind.
- **Comparar tamanhos diferentes sob o mesmo rótulo** — aconteceu com grafo `larger` × Northwind.
- **MongoDB não sobe em kernel ≥6.19.** O `mongod` recusa iniciar — guarda deliberada, não crash. Contorno: bootar o **6.17.0-40-generic**. Docker **não** resolve, o container compartilha o kernel do host. Diagnóstico medido em 15/08/2026 abaixo.
- **Alvo do #8 conflitante com a fidelidade** — se passar batido, ou o gate de equivalência quebra ou se publica um número que o porte não produz.
- **Memória em Python** nos 800k — perfil diferente do Spark; `mapPartitions` é a saída, não a reescrita.
- **Sakila descartado (02/08/2026)** — a equivalência do documento fica com um dataset real só, o Northwind, e o *overfitting* a ele não está descartado. Deixa de ser risco em aberto e passa a ser limitação declarada.
- **`N1` (nós multi-label)** aparecendo pela primeira vez numa extração real — diagnosticar pelo `.java`, não pelo sintoma.

### O bloqueio do `mongod` — medido em 15/08/2026

O que a mensagem de recusa diz, no `journalctl`:

> MongoDB cannot start: Linux kernel versions 6.19 and newer has a known
> incompatibility with this version of MongoDB. See
> <https://jira.mongodb.org/browse/SERVER-121912> for more information.

**A causa é `rseq`, e quem dispara a guarda é o pacote.** O unit do systemd
(`/usr/lib/systemd/system/mongod.service`) traz
`Environment="GLIBC_TUNABLES=glibc.pthread.rseq=0"`, e o binário recusa
exatamente nessa condição — os símbolos são `mongo::validateRseqKernelCompat` e
`ProcessInfo::checkGlibcRseqTunable`. Medido no `7.0.0-28-generic` com o
`mongod` **8.0.29**, num `dbpath` descartável:

| como | resultado |
|---|---|
| `GLIBC_TUNABLES=glibc.pthread.rseq=0` (o que o serviço faz) | recusa |
| sem o tunable (padrão do glibc) | **sobe**, chega a `Waiting for connections` |
| `glibc.pthread.rseq=1` | sobe |

**Não use isso como contorno sem decidir o custo.** A guarda existe para evitar
um crash real do TCMalloc vendorizado, e a saída documentada é o kernel, não a
variável. O pin em `6.17.0-40` continua sendo o único caminho testado ponta a
ponta, e é o kernel de onde saíram todos os números da fase.

**A faixa afetada tem fim datado:** 6.19 até 7.0.13, com **7.0.14 em diante
resolvendo** — o `SERVER-121912` é o upgrade do TCMalloc e o `SERVER-125742`
remove a saída graciosa a partir daí. Nenhuma das duas metades chegou nesta
máquina: o apt não oferece kernel ≥7.0.14 (só sabores `7.0.0-*`) e o
`mongodb-org-server` mais novo do repositório é o 8.0.29, cuja mensagem ainda
diz "6.19 **and newer**", sem limite superior. **A bateria exige kernel <6.19
ou** — quando as duas chegarem — **kernel ≥7.0.14 com um `mongod` que conheça o
limite superior.**

Referências: [SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912) ·
[Release Notes 8.0](https://www.mongodb.com/docs/v8.0/release-notes/8.0/) ·
[MongoDB 8.x and Linux Kernel 6.19](https://www.mongodb.com/community/forums/t/mongodb-8-x-and-linux-kernel-6-19/337547)

# Dicionário de dados — `results/`

Esquema das tabelas de evidência das **Fases 3 e 4**, escritas por
`scripts/output.py`.

> **Estado: dois diretórios, dois esquemas de `runs.csv`.**
>
> - **`results/`** guarda a bateria canônica da Fase 3 (27/08/2026, versionada
>   arquivo a arquivo — ver `results/README.md`). O `runs.csv` dela tem 13
>   colunas e é todo da engine Python, que era a única.
> - **`results/fase4/`** recebe a bateria comparativa da Fase 4.4. O `runs.csv`
>   ganha duas colunas, `engine` e `boot_time`, e a engine entra no `run_id`.
>   As outras três tabelas não mudam de esquema.
> - **Dois kernels em `results/fase4/`.** Na bateria de 04/10/2026, o MongoDB
>   foi medido no `6.17.0-40`, o mesmo da bateria canônica, e o Neo4j no
>   `7.0.0-34`: o `mongod` não sobe no 7.0, e no 6.17 a leitura do grafo pela
>   engine Spark travou (`bugs_originais.md` §E1). O kernel não é coluna. Comparar
>   as engines dentro de um banco é seguro; comparar tempo entre os bancos, ou o
>   grafo de `results/fase4/` com o da bateria canônica, carrega essa diferença.
>
> O código atual só produz o esquema novo. A guarda de cabeçalho do `output.py`
> impede a mistura: uma corrida de agora recusa anexar no `results/` canônico.
> Tudo o que este documento diz sobre `engine` e `boot_time` vale só para
> `results/fase4/`.

## Convenções

- **Nomes de coluna e valores em inglês.** A prosa dos `.md` segue em português.
- **Célula vazia não é zero.** Significa que aquele produtor ou paradigma não
  produz o valor.
- **Tempos em segundos**, com duas casas decimais. A exceção é `query_time`, com
  **quatro**: ele é o divisor de `normalized`, custa décimos de segundo no grafo,
  e a duas casas o arredondamento sozinho desloca a razão em até 8% — a coluna
  derivada deixava de bater com as colunas que a definem.
- **Colunas derivadas não entram.** Razões, percentuais e vereditos por linha
  são conta da análise.

## As quatro tabelas

| arquivo | grão | chave |
|---|---|---|
| `runs.csv` | uma execução do **porte** | `run_id` |
| `oracle.csv` | uma execução do **oráculo Java** | `run_id` |
| `comparisons.csv` | um confronto com um XMI de referência | `run_id` + `reference` |
| `divergences.csv` | uma divergência | sem chave natural |

---

## `runs.csv`

Só o **porte**. O oráculo tem tabela própria.

| # | coluna | tipo | valores | vazia quando |
|---|---|---|---|---|
| 1 | `run_id` | texto | `size-mongodb-up_a_small-23-spark` | nunca |
| 2 | `experiment` | texto | `equivalence` · `size` · `oracle_chain` | nunca |
| 3 | `size` | texto | `small` · `medium` · `large` · `larger` | Northwind |
| 4 | `paradigm` | texto | `mongodb` · `neo4j` | nunca |
| 5 | `route` | texto | `A` · `B` | Neo4j, Northwind |
| 6 | `target` | texto | `up_a_small` · `northwind` · `movies_min` | nunca |
| 7 | `origin` | texto | `file` · `database` | nunca |
| 8 | `engine` | texto | `python` · `spark` | nunca |
| 9 | `total_time` | segundos | — | nunca |
| 10 | `extraction_time` | segundos | — | nunca |
| 11 | `inference_time` | segundos | — | nunca |
| 12 | `write_time` | segundos | — | nunca |
| 13 | `boot_time` | segundos | — | engine `python` |
| 14 | `query_time` | segundos | — | `equivalence` |
| 15 | `normalized` | razão | — | `equivalence` |

`engine` e `boot_time` existem só em `results/fase4/`. A bateria canônica de
`results/` tem as outras 13 colunas, e o `run_id` dela não leva a engine.

### Colunas

**`run_id`** — determinístico, montado de
`experiment`-`paradigm`-`target`-`seed`-`origin`-`engine`, omitindo as partes
ausentes. É a chave desta tabela e da `oracle.csv`, e junta com as outras três.

> **A engine entra sempre, inclusive `python`.** Sem ela, a corrida Python e a
> Spark do mesmo alvo e semente teriam a mesma chave, e a guarda de duplicata
> recusaria a segunda. Consequência: os `run_id` de `results/fase4/` não casam
> com os da bateria canônica (`size-mongodb-up_a_small-23` lá,
> `size-mongodb-up_a_small-23-python` aqui). Para cruzar as duas, tire o último
> segmento.

> **Sozinho ele não identifica um confronto nem uma divergência.** Uma corrida
> do grafo compara o mesmo modelo com `resources` **e** com `seeded_oracle`:
> em `comparisons.csv` a chave é `run_id` + `reference`, e em `divergences.csv`
> esse par ainda se abre em N linhas. Junte sempre por `run_id` + `reference`
> quando o destino for uma dessas duas.

> **A semente não é coluna, mas está no identificador.** É ela que separa duas
> corridas do mesmo alvo com `--seed` diferente; sem ela a chave duplicaria em
> silêncio. Para filtrar por semente, quebre o `run_id` nos hífens: na bateria
> canônica a semente é o último segmento; em `results/fase4/`, o penúltimo,
> porque o último é a engine.

**`experiment`** — qual bateria gerou a linha.

| valor | script | produtores |
|---|---|---|
| `equivalence` | `run_northwind.py` | `port` |
| `size` | `run_size_{mongo,neo4j}.py` | `port` |
| `oracle_chain` | `run_oracle_{mongo,neo4j}.py` | `port` e `oracle` |

`size` e `oracle_chain` medem o mesmo alvo com a mesma semente; é a coluna
`experiment` que as distingue.

**`seed`** — semente do gerador. Vazia no Northwind, que é dataset real
versionado em `resources/datasets/northwind/` e não se regenera.

**`size`** — 100k / 200k / 400k / 800k nós ou documentos `User`, com metade
disso em `Movie`. Os quatro tamanhos dobram exatamente.

**`route`** — só no paradigma documento. `A` usa `_id` ObjectId nativo; `B` usa
`_id` inteiro com ~15% de arrays vazios — o cenário que exercita os bugs **#6** e
**#7**.

**`target`** — nome do banco ou schema medido, que é também o nome do schema
dentro do XMI. Divergência de `SCHEMA_NAME` é **fatal** no harness.

> **`target` não é derivável de `size` + `route`.** No Neo4j o tamanho `small` se
> chama `movies_min`, nome herdado do XMI de referência e enganoso: não é um
> modelo mínimo, é o User Profiles com 100.000 `User`.
> `resources/neo4j/movies_min.xmi` é versionado e imutável.

**`origin`** — por qual caminho o dado foi lido. Só o Northwind é medido pelos
dois; todo o resto é `database`.

**`engine`** — qual engine do porte fez a extração (Fase 4): `python`, o
caminho de sempre, ou `spark`, que paraleliza o map-reduce da extração
(`extractors/mongo.py` e `extractors/neo4j.py`). Só a extração muda: a
inferência e a escrita rodam o mesmo código nas duas engines, no processo
principal. O oráculo não tem engine — é o Java, com o Spark dele dentro do
container, sempre.

**`total_time`** — relógio de parede do banco até o XMI em disco: o pipeline
inteiro, extração, inferência e serialização.

É sempre `extraction_time + inference_time + write_time`, medido em processo.
O equivalente do oráculo está em `oracle.csv`, e a comparação entre os dois é um
join por `run_id`.

**`extraction_time`** — ler o banco pelo driver nativo, classificar os tipos e
reduzir. Única etapa que toca I/O.

**`inference_time`** — transformar o resultado da extração em modelo U-Schema,
em memória. Fica em ~0,00s porque a redução já ocorreu na extração: a inferência
recebe centenas de linhas, não centenas de milhares de documentos. Máximo
medido: **0,17s**, no Northwind — o dataset com mais entidades (17 coleções),
não o de maior volume.

**`write_time`** — serializar o modelo e gravar o XMI (`save_model`). O XMI
descreve formas, não dados: o maior já produzido tem 107 KB (Northwind), e os do
grafo têm 9,4 KB. **Máximo de 0,01s** em toda a bateria de 08/08/2026, nas 62
corridas.

**`boot_time`** — subir a `SparkSession` local (JVM e contexto Spark) antes da
extração: o custo fixo que a engine Spark paga e a Python não. Medido em
processo, da chamada ao `local_session()` até a sessão pronta. Vazia na engine
`python`.

> **Uma corrida Spark por processo.** O `session.stop()` do PySpark encerra a
> sessão, mas não a JVM: ela segue viva no processo, e a sessão seguinte a
> reaproveita já aquecida. Com vários tamanhos num processo só, o primeiro
> pagaria o boot inteiro e os outros quase nada — e são justamente os tamanhos
> pequenos que a comparação distorceria. Por isso as baterias recusam mais de
> uma combinação com `--engine spark`, e o `run_suite.sh` roda cada combinação
> num processo próprio (`scripts/engines.py`). Todo `boot_time` gravado é,
> portanto, o de uma JVM nova.

> **Fica fora do `total_time`.** As três parcelas continuam somando o
> `total_time`, e o `normalized` continua `total_time / query_time` —
> comparáveis entre as engines e com a bateria canônica. O custo de ponta a
> ponta da engine Spark é `boot_time + total_time`, e é conta da análise.

> **O que o `boot_time` não pega.** O aquecimento da JVM durante o primeiro job
> (carga de classes, JIT) acontece dentro da extração e fica no
> `extraction_time`. Como toda corrida começa com JVM nova, ele pesa igual em
> todos os tamanhos. O oráculo não tem coluna equivalente: o container reporta
> um número só, com o boot dentro (ver `oracle.csv`).

**`query_time`** — tempo da **query de referência**: a média de filmes
assistidos por usuário, rodada no mesmo banco. Não é parte do pipeline; existe
só para servir de divisor. O texto das duas queries (uma por paradigma) está em
`scripts/baseline.py`.

Roda **por último**, depois do porte e do oráculo, antes da limpeza — antes,
aqueceria o cache e aceleraria a extração medida. A contrapartida é que ela
mesma sai com cache quente.

Vazia na bateria `equivalence`: o Northwind não tem `User` nem `watchedMovies`.

**`normalized`** — `total_time / query_time`. É a métrica do artigo do U-Schema
(Information Systems 104, 2022), definida lá como *"the normalized value
(inference time divided by query time)"*, e existe para comparar com a Table 4
sem depender da máquina.

> **O dividendo é o `total_time`, não o `inference_time`.** O que o artigo chama
> de *inference* é o pipeline inteiro. A nossa coluna `inference_time` é outra
> coisa e fica em ~0,00s; usá-la daria ~0,0001 em vez de ~10.

> **Recalculando, tem de bater.** O valor é gravado em precisão cheia, e é por
> isso que `query_time` leva quatro casas: dividir as duas colunas como estão no
> CSV reproduz esta aqui. Se não bater, a coluna foi gravada por código antigo —
> as corridas anteriores a 15/08/2026 têm o divisor a duas casas e erram até 8%.

> **Exceção declarada à regra das derivadas.** As Convenções barram colunas
> calculadas, e esta é `total_time / query_time`. Entra assim mesmo porque é a
> métrica de comparabilidade com o baseline publicado: recalculá-la na análise a
> desconectaria da corrida que a produziu. A regra continua valendo para o
> resto.

### Notas de leitura

**As três parcelas somam o `total_time`**, sempre — nenhuma é vazia. Verificado
nas 26 corridas de 08/08/2026. O `boot_time` não entra na soma.

**`run_id` é chave sozinho.** A checagem de duplicata, em cada diretório:

```bash
awk -F, 'NR>1{print $1}' results/runs.csv | sort | uniq -d         # tem de sair vazio
awk -F, 'NR>1{print $1}' results/fase4/runs.csv | sort | uniq -d   # idem
```

**Bancos e saídas ficam em sistemas de arquivos diferentes.** Os bancos em
`/var/lib/...`, sem cifra; o repositório, e portanto `out/`, em eCryptfs. O
`extraction_time` lê sem cifra, o `write_time` grava cifrado.

---

## `oracle.csv`

O extrator Java em Docker (`oracle/`), rodado sobre **os nossos** dados. Só a
bateria `oracle_chain` produz linhas.

| # | coluna | tipo | valores |
|---|---|---|---|
| 1 | `run_id` | texto | `oracle_chain-mongodb-up_a_small-23` |
| 2 | `total_time` | segundos | — |
| 3 | `normalized` | razão | — |

O `normalized` usa o **mesmo** `query_time` da linha correspondente em
`runs.csv` — a query roda uma vez por instância, e serve os dois lados. Por isso
`query_time` não se repete aqui; para tê-lo, junte por `run_id`.

Tabela separada porque o container reporta **um** número: o relógio de parede do
`docker run` inteiro. Não há como abrir em extração, inferência e escrita — na
tabela do porte, essas três colunas ficariam vazias em toda linha, e
`total_time` teria duas definições.

### Notas de leitura

> **Comparar tendência, nunca subtrair de `runs.total_time`.** O número carrega
> **~18s de boot** de Maven, JVM e Spark, parcela que o container não separa. No
> paradigma documento isso é quase tudo: 18,32s no `small` contra 25,23s no
> `larger`, para 8× de dado.

**Os autores originais do U-Schema não aparecem aqui.** São um terceiro
produtor, e os XMIs deles (`resources/`) não têm cronômetro — existem só como
`reference=resources` em `comparisons.csv`.

---

## Fora das tabelas

Três coisas são medidas durante a corrida, impressas no log das baterias e
**não** gravadas:

| | por quê |
|---|---|
| limpeza do dataset anterior | é custo de ambiente, de produtor nenhum; `clean_databases.py` segue como utilitário |
| geração do dataset | cronometra um script nosso, descartável — não é evidência sobre o porte |
| contagem por entidade | real no banco contra somado no XMI (`modeled_counts`), impressa como `real=… modelo=…` |

Para citar qualquer uma delas é preciso lê-la do log. A contagem por entidade é
o que sustenta o invariante do Northwind
(**14 de 17** coleções fechando) e é a única medida que confronta o modelo com o
banco — as quatro tabelas confrontam um modelo com outro.

---

## `comparisons.csv`

| # | coluna | tipo | valores |
|---|---|---|---|
| 1 | `run_id` | texto | `oracle_chain-mongodb-up_a_small-23` |
| 2 | `subject` | texto | `port` |
| 3 | `reference` | texto | `resources` · `seeded_oracle` |
| 4 | `equivalent` | booleano | `True` · `False` |
| 5 | `fatal_divergences` | inteiro | — |
| 6 | `non_fatal_divergences` | inteiro | — |

### Colunas

**`subject`** e **`reference`** são os dois lados do `compare()`. O `subject` é
sempre `port` nas baterias atuais.

**`reference`** — contra o que se comparou, e o que a comparação isola:

| valor | o quê | variáveis livres |
|---|---|---|
| `resources` | XMI publicado pelos autores originais, sobre o dataset **deles** | implementação **e** dataset |
| `seeded_oracle` | XMI do oráculo Java sobre **a mesma instância** | só a implementação |

O grafo dá zero divergências contra `seeded_oracle` e 7 contra `resources`: os
XMIs de `resources/` vêm de uma instância de semente desconhecida, que não se
reconstrói.

**`equivalent`** — equivalência estrutural no sentido do `USchemaCompareMain`,
que é o `warningLog` dele vazio. Em código, `not any(d.fatal)`. Divergência
não-fatal não reprova.

**`fatal_divergences`**, **`non_fatal_divergences`** — contagem separada por
fatalidade. `equivalent` é `fatal_divergences == 0`.

### Notas de leitura

**As duas contagens são deriváveis de `divergences.csv`, e servem de
conferência**: se as tabelas discordarem, alguma linha não foi gravada.

**`equivalent=True` não afirma que a leitura foi integral.** A comparação é
modelo contra modelo: onde o **#8** subconta, porte e oráculo subcontam de forma
equivalente e o veredito passa. Quem detecta a subcontagem é a contagem por
entidade, que sai no log — ver *Fora das tabelas*.

---

## `divergences.csv`

Detalhe por trás dos contadores de `comparisons.csv`. Sem chave natural: uma
comparação produz de zero a N linhas.

| # | coluna | tipo | valores |
|---|---|---|---|
| 1 | `run_id` | texto | `equivalence-mongodb-northwind-file` |
| 2 | `subject` | texto | `port` |
| 3 | `reference` | texto | `resources` · `seeded_oracle` |
| 4 | `category` | texto | `schema_name` · `entity` · `relationship` · `variation` · `count` · `root` |
| 5 | `fatal` | booleano | `True` · `False` |
| 6 | `message` | texto | uma linha |

`subject` e `reference` repetem os de `comparisons.csv` e ligam cada divergência
ao confronto que a produziu — `run_id` sozinho não basta, porque o grafo é
comparado contra as duas referências na mesma corrida.

### `fatal`

Determina o veredito: `equivalent` é a ausência de qualquer `fatal=True`.

São **não-fatais** as categorias que o Java não registra em lugar nenhum
(`count`, `root`), as que ele registra como `hit` (o fallback fuzzy de nome de
entidade) e as variações órfãs de `Schema2`.

> **A categoria não determina a fatalidade.** `entity` e `variation` são fatais
> ou não conforme o caso. As 10 linhas de categoria `variation` do Northwind são
> **não-fatais**; julgar pela categoria concluiria que o modelo reprovou.

### `category`

Seis valores, não sete. O enum `DivergenceCategory` tem um sétimo membro,
`feature`, que **nenhum ponto do código emite**: a granularidade do harness para
no nível da variação. Está marcado no fonte para remoção se chegar ao fim do
projeto sem uso.

### `message`

Gravada com os dois lados **já nomeados**. O harness emite os rótulos
posicionais `Schema1` e `Schema2`, herdados do `USchemaCompareMain`; a escrita
substitui `Schema1` pelo valor de `reference` e `Schema2` pelo de `subject`.

```text
no harness:  Count differs: Schema1 Orders.1 has 4, Schema2 Orders.2 has 7
no CSV:      Count differs: resources Orders.1 has 4, port Orders.2 has 7
```

> A convenção é `compare(referência, porte)` em todos os pontos de chamada, logo
> `Schema1` = `reference` e `Schema2` = `subject`. Uma busca por `Schema1` no
> `equivalence.py` não casa com o texto do CSV.

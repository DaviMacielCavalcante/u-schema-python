# scripts/ — baterias de avaliação e geradores (Fases 3 e 4)

Scripts de execução das baterias de **equivalência** e **volume**, fora do pacote
importável.

> **O esquema das tabelas que estas baterias escrevem está em
> [`dicionario_de_dados.md`](../dicionario_de_dados.md)** — o que cada coluna
> significa, quem produziu o número e as leituras que induzem a erro. Este
> README descreve *como rodar*; o dicionário, *o que sai*.
>
> **Dois diretórios de saída.** `results/` e `out/{porte,oraculo}/` guardam a
> bateria canônica da Fase 3 (27/08/2026, versionada arquivo a arquivo). Desde a
> Fase 4.4 as baterias gravam em `results/fase4/` e `out/fase4/`, com a coluna
> `engine` e o `boot_time` no `runs.csv` — o código atual não produz mais o
> esquema canônico, e a guarda de cabeçalho recusa anexar nele.

## Geradores de dataset (vindos do repo original, autoria do Davi)

- `gen_userprofiles.py` — User Profiles em MongoDB (Rotas A e B). Trazido de
  `~/Documents/teste_uschema/` (repo próprio, não o clone Java) com type hints,
  docstrings NumPy, `pip3` → `uv` e sem a instrução de cronometrar pelo log do
  Spark (não há Spark desde a 2.0).
- `gen_userprofiles_neo4j.py` — User Profiles em grafo. Trazido do original; só
  a instrução de instalação foi ajustada de `pip3` pra `uv`, resto não
  reformatado. Confirma a estrutura dos XMIs-oráculo
  (`resources/neo4j/movies_min.xmi`/`up_*.xmi`).

Os dois aceitam `--seed`, então a bateria é reproduzível por terceiros.

Quatro tamanhos: 100k / 200k / 400k / 800k `User` (50k / 100k / 200k / 400k `Movie`).

> **Sem `--seed`, cada execução produz um dataset diferente** — mesmos totais,
> divisão entre variações diferente (o sorteio de users isolados e favoritos
> muda). Consequência medida em 31/07/2026, antes de a semente existir: regerar
> os tamanhos `small`/`medium`/`large` do grafo fez o `compare()` contra os
> XMIs-oráculo de `resources/` acusar **7 divergências não-fatais de `count`**
> em cada uma, enquanto o `larger` (não regerado, instância original) fechou com
> **zero**. Não é defeito do porte — é o dataset que mudou. **Provado nas quatro
> tamanhos em 02/08/2026:** com a semente fixa e o **oráculo Java rodado sobre a
> mesma instância**, o `compare()` dá `equivalent=True` com **zero**
> divergências em todas. Contra `resources/` continuam aparecendo as 7, porque
> aqueles XMIs vêm de uma instância de semente desconhecida — e essa é a
> comparação mais fraca das duas.

- **Rota A**: `_id` ObjectId nativo.
- **Rota B**: `_id` inteiro + ~15% arrays vazios (cenário relacional→NoSQL —
  exercita #6 e #7).

## Equivalência do Northwind — `run_northwind.py`

Roda o dataset pelos **dois caminhos de leitura** — os 17 JSONs direto do disco
e o banco pelo cursor do `pymongo` — e compara cada um com
`resources/mongodb/model_northwind.xmi`, gravando as divergências no
`results/` com a coluna `origin` (`file`/`database`).

```bash
uv run python scripts/run_northwind.py
```

Os dois caminhos existem porque o **#8 é sensível à ordem de leitura**: na
bateria canônica (27/08) deram 15 divergências por arquivo e 12 por cursor, com
o mesmo dado. Desde a 4.3 o cursor lê em ordem de `_id`, que é a ordem do
arquivo, e os dois caminhos dão as mesmas 15, com XMI idêntico (medido em
03/10/2026). O invariante citável não é esse número — é `equivalent=True` mais
**14/17** coleções fechando a contagem, que vale nos dois caminhos.

Os JSONs estão versionados em `resources/datasets/northwind/` (BSD 2-Clause, com
o `LICENSE` junto), então o caminho `file` **roda sem banco e sem dependência
externa**. O script imprime o SHA-256 do conjunto a cada corrida, prendendo o
resultado a uma versão do dataset.

Os JSONs são JSONL em extended JSON, então a leitura usa `bson.json_util.loads`
— com `json.load` puro o `{"$date": …}` viraria objeto aninhado e o modelo
ganharia uma entidade que o oráculo não tem.

## Cadeia porte × oráculo — `run_oracle_neo4j.py`, `run_oracle_mongo.py`

Por tamanho: limpa, regera com a semente, roda o porte, roda o **oráculo Java em
Docker sobre a mesma instância** e compara os dois XMIs. É a comparação mais
forte que o projeto faz — mesma entrada, duas implementações. Exige a imagem
buildada (`docker build -t extrator-uschema oracle/`) e o banco no ar.

```bash
uv run python scripts/run_oracle_neo4j.py --seed 23
uv run python scripts/run_oracle_neo4j.py --seed 23 --sizes larger
uv run python scripts/run_oracle_mongo.py --seed 23 --sizes small
```

No **grafo** há ainda uma segunda comparação, contra `resources/`, que serve de
contraste: dá 7 divergências não-fatais de `count`, porque aqueles XMIs vêm de
uma instância de semente desconhecida.

No **documento** não existe XMI-oráculo publicado do User Profiles — é
justamente o que essa bateria produz. Rodou pela primeira vez em 02/08/2026, nas
oito combinações: o risco declarado (o `--kind mongodb` do container só tinha
visto os 397 documentos do Northwind, na Fase 0.5) não se materializou, e o
container é quase plano no documento — ~18s de boot mais ~6s de trabalho real
sobre 800 mil documentos.

Uma diferença de interface entre os dois, que já custou tempo: no Neo4j o
`--db` é só o **nome do schema** (o conector lê sempre o banco padrão); no
MongoDB o `--db` **é** o banco a conectar, e as coleções vão por
`MONGO_COLLECTIONS`. Nos dois casos o valor precisa casar com o nome que o porte
usa — divergência de `SCHEMA_NAME` é fatal no harness.

**Engine do porte (Fases 4.3 e 4.4).** `--engine spark` extrai pela engine
Spark, com `--slices` fatias (padrão: uma por núcleo), e grava o tempo de subir
a sessão em `boot_time`, fora do `total_time`. **Uma corrida Spark por
processo:** o PySpark não derruba a JVM entre sessões, e os tamanhos seguintes
pegariam a JVM aquecida pelo primeiro; por isso, com `--engine spark`, as quatro
baterias recusam mais de uma rota ou tamanho (`engines.py`). A mesma regra vale
nas baterias por tamanho, abaixo.

```bash
uv run python scripts/run_oracle_mongo.py --seed 23 --engine spark --routes A --sizes small
uv run python scripts/run_oracle_neo4j.py --seed 23 --engine spark --sizes small
```

Os dois XMIs da corrida — o do porte e o do oráculo — se chamam pelo `run_id`
(`out/fase4/porte/<run_id>.xmi` e `out/fase4/oraculo/<run_id>.xmi`), para que a
corrida seguinte não sobrescreva a evidência da anterior. Na bateria canônica o
oráculo ficou com a semente no nome (`out/oraculo/neo4j_<schema>_seed<N>.xmi`). O `total_time` da linha `producer=oracle`
é **relógio de parede do container** — inclui boot de Maven, JVM e Spark, que no
MongoDB são ~18s fixos, então não é comparável ao cronômetro interno do Java nem
citável em volume pequeno.

## Baterias por tamanho — `run_size_mongo.py`, `run_size_neo4j.py`

Uma bateria por paradigma, quatro tamanhos cada. Não rode as duas em paralelo:
os clientes Python não disputam, mas mongod e Neo4j disputam CPU e disco, e os
tempos vão para a avaliação.

São as baterias que medem as curvas da Fase 4.4, nas duas engines. O
`--engine`/`--slices` e a regra de uma corrida Spark por processo são os da
cadeia do oráculo, acima:

```bash
uv run python scripts/run_size_mongo.py --seed 23 --engine spark --routes A --sizes small
uv run python scripts/run_size_neo4j.py --seed 23 --engine spark --sizes small
```

## Orquestração — `run_suite.sh`

Encadeia **tudo**, sequencialmente, com log em `logs/`: limpeza inicial,
Northwind, as duas cadeias do oráculo e as duas por tamanho.

A limpeza inicial é o `clean_databases.py`, que também é chamado pelas duas
baterias do grafo antes de cada tamanho. Apaga **só** os bancos `up_*` do MongoDB
e o grafo do Neo4j — o `northwind` nunca é tocado, porque é dataset real
versionado e não se regenera por semente.

```bash
./scripts/run_suite.sh                  # semente padrão dos scripts (23), engine python
./scripts/run_suite.sh 69               # outra semente
./scripts/run_suite.sh 69 spark         # outra semente, engine spark
./scripts/run_suite.sh 69 spark neo4j   # só as baterias de um banco (mongodb ou neo4j)
```

**Um banco por suíte, se pedido.** O terceiro argumento restringe a suíte às
baterias de um banco; sem ele, roda os dois, na ordem de sempre. A limpeza
inicial passa a tocar só esse banco, o Northwind só roda na suíte do MongoDB, e a
guarda de semente repetida olha só as corridas dele. Existe para que cada banco
rode no seu kernel: a leitura do grafo trava no 6.17 (`bugs_originais.md` §E1) e
roda num kernel mais novo, em que o mongod não sobe.

**Uma combinação por processo, nas duas engines.** Desde a 4.4 a suíte chama
cada bateria uma vez por rota e tamanho: é o que a engine Spark exige para que
todo boot seja o de uma JVM nova, e a Python roda igual para as duas serem
medidas do mesmo jeito. O **Northwind** só roda na primeira suíte de
`results/fase4/`: não tem semente nem engine Spark, e repeti-lo duplicaria a
chave.

Substituiu o `run_scale_suite.sh`, que orquestrava só o tamanho e existia para
varrer três sementes — com a semente única, o que restava a orquestrar era a
cadeia inteira.

Sem argumento ele **não passa** `--seed`: vale o `DEFAULT_SEED`, que mora em
`output.py` e é de onde os quatro scripts o importam. A guarda de semente
repetida, abaixo, lê o mesmo valor de lá — duplicá-lo no shell abriria espaço
para os dois divergirem, que é justamente o caso que a guarda existe para pegar.

**Recusa repetir uma semente já gravada na mesma engine** (e no mesmo banco,
quando há um) (exit 3). As baterias
gravam em append, então repetir não sobrescreve — duplica, e a duplicata só
apareceria depois, num `uniq -d`. Já custou duas sessões.

## Query de referência — `baseline.py`

O divisor da coluna `normalized`: a média de filmes assistidos por usuário, uma
query por paradigma. Não faz parte do pipeline — existe só para tornar o tempo
comparável com a Table 4 do artigo, que é medida noutra máquina (um i7-6700 de
2015).

Roda **por último** em cada corrida, depois do porte e do oráculo: antes,
aqueceria o cache e aceleraria a extração medida.

## Verificações fora da suíte — os três `check_*`

Não entram no `run_suite.sh`, não escrevem CSV e não têm semente: **imprimem** e
saem com código de erro. Rodam à mão, e cada um cobre uma coisa que as baterias
**não conseguem** cobrir — é por isso que sobrevivem ao fim das Fases 2 e 3, e
não por inércia.

```bash
uv run python scripts/check_northwind_invariants.py
uv run python scripts/check_extraction_neo4j.py --uri bolt://localhost:7687 --drop
uv run python scripts/check_extraction_mongo.py --uri mongodb://localhost:27017 \
    --db verificacao_manual --drop
```

### `check_northwind_invariants.py` — a única afirmação absoluta

Lê o XMI e afirma o que ele **contém**: 19 `EntityType`, 17 raiz, as duas
não-raiz sendo `Detail` e `_id`, e o `Aggregate` de `Detail` com
`upperBound=-1`/`optional=true` em `Orders` e `Purchase_orders`. Confirmado
idêntico no XMI do oráculo e no do porte em 02/08/2026.

**Todo o resto do aparato compara um modelo com outro.** O `compare()` afirma que
os dois lados são iguais, nunca o que eles contêm: se porte e oráculo perdessem
`Detail` juntos, ele seguiria dando `equivalent=True`. Este script é a única
coisa que checa o modelo contra um valor absoluto gravado, e o invariante é
**livre de contagem** — escapa da ordem-dependência do #8.

É a exceção declarada à regra "todo número sai de uma corrida registrada"
(`todolist_fase3.md` §3.4): imprime e não grava, mas é reproduzível por um
comando.

### `check_extraction_neo4j.py` — a premissa do N1, que só um servidor real dá

Semeia um nó `:Zebra:Apple` **nessa ordem de inserção** e observa se o `N1` se
manifesta: `node_archetype` ordena os labels próprios, `_relationship_archetype`
**não** ordena os do `refsTo`, então o mesmo nó físico pode virar dois
`EntityType`.

Os unit tests fixam os dois lados da assimetria com fakes
([`test_extractors_neo4j.py:213,259`](../tests/unit/test_extractors_neo4j.py)),
mas a **premissa** do bug é que um Neo4j real devolve `labels()` em ordem de
inserção, e não alfabética — propriedade do servidor, que nenhum fake
estabelece. As baterias também não chegam lá: o `gen_userprofiles_neo4j.py` só
cria nó de label único (`CREATE (u:User)`, `CREATE (m:Movie)`), que é a razão de
o `N1` nunca ter disparado numa corrida. É o artefato que sustenta essa
limitação na 3.4.

### `check_extraction_mongo.py` — registro do gate 2.1

Fumaça do cursor `pymongo` real, o caso `Int64` e o Northwind pelas 17 coleções.
**É o mais fraco dos três**, e vale saber por quê antes de confiar nele como
evidência: o caminho do cursor real já é exercitado a cada corrida pelo
`run_northwind.py` (`origin=database`), e o unit test do `Int64` constrói o mesmo
objeto que o driver devolveria — rodar contra servidor não estabelece nada além.

O que ele ainda é o único a tocar: nenhum dataset da fase tem `Int64`
(**zero** `$numberLong` nos 17 JSONs do Northwind), então a ordem de despacho
`Int64` → `bool` → `int` nunca é exercitada contra dado que veio pelo *wire*.

## Saída — quatro tabelas, um grão cada

Todas as baterias gravam pelo `output.py`, em `results/fase4/`, modo append,
com **guarda de cabeçalho** (se o esquema mudar, a bateria recusa anexar em vez
de corromper o arquivo em silêncio) e `flush` por linha, para que uma corrida de
uma hora interrompida preserve o que já mediu.

| Arquivo | Grão |
|---|---|
| `runs.csv` | uma execução do **porte** — tempos e metadados |
| `oracle.csv` | uma execução do **oráculo Java** — só o relógio de parede |
| `comparisons.csv` | um confronto com um XMI de referência |
| `divergences.csv` | uma divergência |

A contagem por entidade (real contra modelo) **não vira CSV** — as tabelas
carregam as métricas do artigo e nada além. Ela segue calculada e **impressa** no
log de cada bateria.

**O significado de cada coluna está em [`dicionario_de_dados.md`](../dicionario_de_dados.md).**
O que segue aqui é só o que muda a forma de rodar as baterias.

Unidas por `run_id`, determinístico a partir de experimento, paradigma, alvo,
semente, origem e engine (`size-mongodb-up_a_small-23-spark`,
`oracle_chain-neo4j-movies_min-23-python`,
`equivalence-mongodb-northwind-file-python`). Na bateria canônica o `run_id` não
leva a engine.

**Uma corrida `oracle_chain` grava em `runs.csv` e em `oracle.csv`**, uma linha
em cada. Em `runs.csv` o `run_id` é chave sozinho.

Os XMIs vão para **`out/fase4/porte/`**, separados dos do oráculo
(`out/fase4/oraculo/`) e dos de referência (`resources/`), cada um com o nome do
`run_id` da corrida que o produziu. Os da bateria canônica seguem em
`out/porte/` e `out/oraculo/` — a convenção está em `resources/README.md`, "Onde
cada XMI mora".

**A limpeza tem tabela própria**, e isso é lição aprendida: na primeira bateria o
gerador rodava com `--drop` e o cronômetro da geração engolia a deleção do grafo
anterior — `small` da semente 23 marcou 7,60s e o das sementes 69/207 marcou
~127s, porque estavam apagando 10,2M arestas da corrida anterior. Apagar um
grafo grande é custo real do paradigma, mas não é geração — nem é medida de
produtor nenhum, que é por que não cabe em `runs.csv`. Só o grafo preenche:
dropar um banco no Mongo é instantâneo.

**O tempo de geração não é mais gravado.** Ele existia para refutar a afirmação
do guia de que a geração custaria mais que a extração — refutada (no `larger` do
grafo, ~180s de geração contra ~440s de extração). Segue impresso no log das
baterias, fora dos CSVs: cronometrar um script descartável nosso não é evidência
sobre o porte.

## Baterias — protocolo

- Equivalência: Northwind (dois caminhos de leitura) e User Profiles em grafo
  (4 tamanhos × oráculo semeado) → comparar via `uschema.validation`. **Sakila
  foi descartado** — não existe versão em grafo publicada, e a consequência é a
  limitação declarada de haver um único dataset real (`todolist_fase3.md` §3.1).
- Volume: rodar os quatro tamanhos, **cronometrar a extração e a inferência em
  processo** (a leitura é por driver nativo desde a 2.0 — não há log de executor
  Spark), confirmar leitura integral (soma dos `count` = volume gerado), comparar
  a **tendência** (não o tempo absoluto).
- Re-execução completa: `./scripts/run_suite.sh`, que já encadeia tudo na ordem
  certa (~25 min na semente padrão).

## Números-alvo

O critério é **casar com o oráculo**, não com o volume real — o **#8** é
replicado de propósito (`bugs_originais.md` §#8), e o oráculo também não o
corrige (não há patch `0008`).

> **Estes dois números saem do log, não de CSV**: as baterias imprimem
> `real=… modelo=…` por entidade a cada corrida. O `comparisons.csv` **não** os
> substitui — ele afirma que porte e oráculo são equivalentes, e eles subcontam
> igual.

- **Grafo (User Profiles):** soma dos `count` de `User` = volume gerado — 100k/200k/400k/800k. Fecha exato; o núcleo do Neo4j é próprio e o #8 não passa por ele (verificado nos 4 XMIs-oráculo).
- **Documento (User Profiles, Northwind):** a soma **não** fecha, e não deve. Medir a subcontagem e mostrar que é a mesma do oráculo. No Northwind isso aparece como divergências **não-fatais** em `orders`/`products`/`purchase_orders` — a quantidade delas varia com a ordem de leitura (15 por arquivo, 12 por cursor), então cite o invariante (**14/17** coleções fechando), não o número.

> O 50/50 do User Profiles e o "17 de 17" do Northwind são resultados do
> experimento **original com a correção do #8** — contexto do que o bug custa,
> **não** alvo deste porte. Persegui-los quebraria a equivalência.

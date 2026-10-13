# Artefatos da bateria canônica — 2026-08-27 11:10

Este diretório e mais dois (`out/` e `logs/`) guardam **uma** execução da suíte
de baterias da Fase 3: a de **2026-08-27, 11:10:58 → 12:07:49 (-03:00)**. É a
corrida da qual saem as tabelas e figuras da Fase 3 no artigo.

Há uma segunda bateria versionada: a comparativa da Fase 4.4, de 2026-10-04, em
`results/fase4/`, com README próprio. Ela não substitui esta — tem outro esquema
de `runs.csv` e mede as duas engines do porte.

As outras execuções que existam em disco **não** estão no repositório, de
propósito. O `.gitignore` libera esta bateria **arquivo a arquivo** — não por
diretório nem por glob — para que uma corrida futura, mesmo na mesma semente,
não entre sozinha no commit.

## Índice dos artefatos (39 arquivos)

| Caminho | Conteúdo |
| --- | --- |
| `results/runs.csv` | uma linha por corrida: tempos de extração/inferência/escrita/query e o tempo normalizado |
| `results/oracle.csv` | tempo total da cadeia do oráculo Java, por corrida |
| `results/comparisons.csv` | veredito de equivalência por par (sujeito × referência) |
| `results/divergences.csv` | cada divergência individual, com categoria e mensagem |
| `out/oraculo/*.xmi` | 12 XMIs gerados pelo oráculo Java em Docker |
| `out/porte/*.xmi` | 22 XMIs gerados pelo porte Python |
| `logs/baterias_20260827_111058.log` | saída bruta da suíte (**anonimizada** — ver abaixo) |

O que cada coluna dos CSVs significa, quem produziu o número e quais leituras
estão erradas: `dicionario_de_dados.md`, na raiz. Várias colunas são
ordem-dependentes ou de sujeitos diferentes; não interprete os CSVs sem ele.

## Proveniência

| Item | Valor |
| --- | --- |
| Commit do porte | `de33a53` — `fix(1.2): trocar o set de inner_schema_names por ordered-set` |
| Semente | `23` (o `DEFAULT_SEED` de `scripts/output.py`; a suíte rodou sem `--seed`) |
| Dataset Northwind | `sha256:3700157bd0bcca944b5a869dbfdd2065ae180bc538ec1e816a94e0f86c7271ae` |
| MongoDB | 8.0.31 (`localhost:27017`, sem autenticação) |
| Neo4j | 2026.07.1 Community (`bolt://localhost:7687`, sem autenticação) |
| Spark | 3.0.1 — dentro do contêiner do oráculo; o porte **não** usa Spark em runtime |
| JDK | Eclipse Temurin 8 — imagem base `maven:3.9-eclipse-temurin-8`, digest pinado no `oracle/Dockerfile` |
| Imagem do oráculo | `extrator-uschema:latest`, `sha256:ca377aee…f126f7`, construída em 2026-08-01 |
| Fontes do oráculo | `uschema@6dfd6b4a`, `uschema-inference@0f8f58c3` (pinados no `oracle/Dockerfile`) |
| Python | 3.12.3 (teto fixado em 3.12 pelo PySpark) |

**Sobre o commit do porte.** A bateria rodou às 11:10 com a troca do
`inner_schema_names` por ordered-set **já aplicada na árvore de trabalho**; o
commit `de33a53` que a carrega só foi criado às 22:13 do mesmo dia. A evidência
de que é este o código medido está no próprio `comparisons.csv`: a Rota A fecha
em **7** divergências não-fatais nos quatro tamanhos, e não em 8 — 8 era o
número antes da troca. Para reproduzir, use `de33a53`, não o commit anterior.

## Como reproduzir

Pré-requisitos: `mongod` e `neo4j` ativos, imagem do oráculo construída.

```bash
systemctl is-active mongod neo4j
docker image inspect extrator-uschema > /dev/null   # senão: docker build -t extrator-uschema oracle/

git checkout de33a53
uv sync
./scripts/run_suite.sh          # sem argumento = semente 23
```

O `run_suite.sh` encadeia as cinco baterias na ordem em que as evidências
dependem umas das outras (Northwind → cadeia do oráculo mongo/neo4j → tamanho
mongo/neo4j), grava um log novo em `logs/baterias_<timestamp>.log` e **acrescenta**
aos CSVs de `results/`. Ele recusa rodar se `results/runs.csv` já tiver corridas
da semente pedida — arquive o diretório antes de repetir:

```bash
mv results results_$(date +%d-%m)
```

**A suíte é destrutiva**: esvazia os bancos `up_*` do MongoDB e o grafo do Neo4j
e regera cada um a partir da semente. O `northwind` não é tocado.

Detalhe de cada bateria, e o que cada script mede: `scripts/README.md`.

## Anonimização do log

`logs/baterias_20260827_111058.log` **não é saída bruta literal**. Três
substituições textuais foram aplicadas antes de versionar, todas em identidade
de máquina, nenhuma em número medido:

| Original | Placeholder | Ocorrências |
| --- | --- | --- |
| o hostname da máquina | `<HOSTNAME>` | 12 |
| o diretório *home* do usuário, prefixo do caminho do repositório | `<HOME>` | 1 |
| o endereço IP da máquina na LAN | `<LAN_IP>` | 498 |

As três aparecem em linhas de *log* de infraestrutura do Spark
(`WARN Utils: Your hostname… resolves to a loopback address`, e os `TID … on
<LAN_IP> (executor driver)`), mais a linha `dataset:` do Northwind.

Verificado após a substituição: o número de linhas é o mesmo (2916); toda linha
alterada contém um dos três placeholders; e as **28 linhas
`Job N finished: …, took X s`** — que são a fonte da figura dos tempos de job do
Spark — estão **byte a byte idênticas** ao original (`md5` do conjunto:
`64a69a2260d782cb2a940b9dcb4ebc64`). Nenhuma delas continha hostname, caminho ou
IP.

O `<LAN_IP>` não foi pedido junto com os outros dois; entrou porque é a mesma
classe de vazamento (identidade da máquina) e responde por quase toda a
incidência. Se a preferência for manter o IP, ele é recuperável do log original.

## Limitação: os XMIs do porte no paradigma grafo

**`out/porte/neo4j_*_seed23.xmi` são da bateria de tamanho, não da cadeia do
oráculo.**

`run_oracle_neo4j.py` e `run_size_neo4j.py` gravavam no mesmo caminho
(`out/porte/neo4j_{schema}_seed{seed}.xmi`). As duas rodam na mesma suíte e a de
tamanho vem depois, então ela sobrescreveu o XMI que a cadeia do oráculo tinha
deixado. Os mtimes desta bateria mostram isso: os quatro
`out/oraculo/neo4j_*.xmi` são de 11:18–11:34 (janela do `run_oracle_neo4j`,
11:17:37–11:34:42) e os quatro `out/porte/neo4j_*.xmi` são de 11:38–12:07
(janela do `run_size_neo4j`, 11:36:43–12:07:49).

**O veredito de equivalência não é afetado.** Os dois scripts comparam o objeto
`USchema` em memória, recém-construído, contra a referência — não releem o
arquivo que acabaram de gravar. `comparisons.csv` está correto; o que estava
errado era o arquivo deixado em disco, cujo nome sugeria uma procedência que ele
não tinha.

O que **não** se pode fazer com estes quatro arquivos: usá-los como o "XMI do
porte" da linha `oracle_chain-neo4j-*` do `comparisons.csv`. Para isso, rode
`run_oracle_neo4j.py` sozinho.

A colisão de nome está **corrigida** em `scripts/run_size_neo4j.py`: a bateria de
tamanho agora grava `neo4j_{schema}.xmi`, sem o sufixo de semente, seguindo o que
`run_size_mongo.py` já fazia (`mongo_{database}.xmi`). A correção vale para as
próximas corridas — **os artefatos aqui não foram regerados**, e continuam a ser
o que esta seção descreve.

## Verificação de segredos

Nenhum dos 39 arquivos contém credencial, token, chave, caminho de máquina ou
URI de conexão. MongoDB e Neo4j rodaram em `localhost` sem autenticação, então
não há segredo a vazar nas strings de conexão; os XMIs contêm apenas o modelo
(nomes de entidade, atributos, contagens) e os CSVs apenas identificadores de
corrida e números.

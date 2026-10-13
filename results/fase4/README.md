# Artefatos da bateria comparativa — Fase 4.4, 2026-10-04

Este diretório, mais `out/fase4/` e cinco logs em `logs/`, guarda a bateria da
Fase 4.4: as baterias da Fase 3 rodadas nas duas engines do porte, Python e
Spark. É a corrida da qual saem as Tabelas 1 a 4 do artigo na versão da Fase 4 e
as curvas tempo × volume das duas engines.

A bateria canônica da Fase 3 continua em `results/` (ver `results/README.md`).
As duas não se misturam: o `runs.csv` daqui tem duas colunas a mais (`engine` e
`boot_time`), e a engine entra no `run_id`.

Como na canônica, o `.gitignore` libera esta bateria **arquivo a arquivo**, para
que uma corrida futura, mesmo na mesma semente, não entre sozinha no commit.

## Índice dos artefatos (83 arquivos)

| Caminho | Conteúdo |
| --- | --- |
| `results/fase4/runs.csv` | 50 corridas do porte: tempos de extração, inferência, escrita, boot da JVM e query, e o tempo normalizado |
| `results/fase4/oracle.csv` | 24 execuções do oráculo Java, uma por alvo em cada suíte |
| `results/fase4/comparisons.csv` | 42 vereditos de equivalência por par (sujeito × referência) |
| `results/fase4/divergences.csv` | 234 divergências, com categoria e mensagem |
| `out/fase4/oraculo/*.xmi` | 24 XMIs do oráculo, nomeados pelo `run_id` da corrida |
| `out/fase4/porte/*.xmi` | 50 XMIs do porte, nomeados pelo `run_id` da corrida |
| `logs/baterias_20261004_*.log` | as quatro suítes e a suíte abortada (**anonimizados** — ver abaixo) |

Os XMIs levam o `run_id` no nome, então cada corrida tem o seu arquivo. A
colisão de nome descrita no README da bateria canônica não acontece aqui.

O que cada coluna dos CSVs significa: `dicionario_de_dados.md`, na raiz.

## As quatro suítes

A bateria rodou como quatro suítes, uma por banco e engine. Cada banco rodou no
seu kernel (ver "Dois kernels", abaixo).

| Log | Banco | Engine | Kernel | Início → fim (-03:00) |
| --- | --- | --- | --- | --- |
| `baterias_20261004_125459.log` | MongoDB | python | `6.17.0-40` | 12:54:59 → 13:02:29 |
| `baterias_20261004_130229.log` | MongoDB | spark | `6.17.0-40` | 13:02:29 → 13:08:47 |
| `baterias_20261004_131929.log` | Neo4j | python | `7.0.0-34` | 13:19:29 → 13:52:51 |
| `baterias_20261004_135251.log` | Neo4j | spark | `7.0.0-34` | 13:52:51 → 14:09:41 |

Os tempos de job do oráculo, que a Tabela 2 e as curvas usam, são lidos destes
quatro logs: o `oracle.csv` traz o `docker run` inteiro, com ~18 s de boot.

`baterias_20261004_115923.log` **não é fonte de nenhuma linha dos CSVs.** É uma
primeira suíte Spark, com os dois bancos, no 6.17, que parou no `oracle_chain`
`up_large` do grafo, travado três vezes seguidas. As linhas que ela gravou foram
descartadas, e os CSVs refeitos pelas quatro suítes acima. O log está aqui como
evidência do travamento, citado em `bugs_originais.md` §E1.

## Proveniência

| Item | Valor |
| --- | --- |
| Commit do porte | `7c68227` — `feat(4.4): baterias medem as duas engines, com o boot da JVM separado` (ver abaixo) |
| Semente | `23`, passada à suíte: `./scripts/run_suite.sh 23 <engine> <banco>` |
| Dataset Northwind | o mesmo da bateria canônica: `resources/datasets/northwind/` não mudou desde `de33a53` |
| MongoDB | 8.0.32 (`localhost:27017`, sem autenticação), no kernel `6.17.0-40` |
| Neo4j | 2026.09.0 Community (`bolt://localhost:7687`, sem autenticação), no kernel `7.0.0-34` (upstream 7.0.14) |
| Engine Spark do porte | PySpark 4.1.2, `local[*]`, sobre OpenJDK 21.0.12.1 (`JAVA_HOME`) |
| Imagem do oráculo | a mesma da bateria canônica: `extrator-uschema:latest`, `sha256:ca377aee…f126f7` |
| Python | 3.12.3, com as dependências do `uv.lock` de `7c68227` (pymongo 4.17.0, neo4j 6.2.0) |

**Sobre o código medido.** O `src/` é o de `7c68227`, sem nenhuma alteração. O
`run_suite.sh` da corrida tinha duas mudanças ainda sem commit:

- **O terceiro argumento** (`mongodb` ou `neo4j`), que restringe a suíte a um
  banco. É o que permitiu rodar cada banco no seu kernel, e entrou no repositório
  em `1180729`.
- **Um watchdog** (`scripts/bolt_watchdog.sh`), que matava e repetia, até três
  vezes, a combinação em que uma conexão com o Neo4j rastejasse. Foi removido em
  05/10/2026, porque a saída para o travamento é rodar o grafo num kernel mais
  novo. Ele não mexeu em nenhum número: nas quatro suítes, nenhuma tentativa foi
  morta, e todo cabeçalho de combinação diz `(tentativa 1)` — o sufixo que ele
  escrevia. As mortes estão só no log abortado, nas linhas com `!!!`.

## Dois kernels

O MongoDB foi medido no `6.17.0-40`, o mesmo da bateria canônica; o Neo4j, no
`7.0.0-34`. No 7.0 o `mongod` não sobe (`todolist_fase4.md`, requisito de
ambiente da 4.1), e no 6.17 a leitura do grafo trava (`bugs_originais.md` §E1,
"Regressão do kernel 6.17").

O kernel não é coluna dos CSVs. Comparar as engines dentro de um banco é seguro;
comparar tempo entre os bancos, ou o grafo daqui com o da bateria canônica,
carrega essa diferença.

## Como reproduzir

Pré-requisitos: o banco da suíte ativo e a imagem do oráculo construída. Os
testes e a engine Spark exigem Java 17 ou 21 no `JAVA_HOME`.

```bash
docker image inspect extrator-uschema > /dev/null   # senão: docker build -t extrator-uschema oracle/
git checkout 1180729      # primeiro commit com o argumento do banco; o src/ é o de 7c68227
uv sync

# no kernel 6.17, com o mongod ativo
./scripts/run_suite.sh 23 python mongodb
./scripts/run_suite.sh 23 spark mongodb

# no kernel 7.0, com o neo4j ativo
./scripts/run_suite.sh 23 python neo4j
./scripts/run_suite.sh 23 spark neo4j
```

A suíte grava em `results/fase4/` e `out/fase4/`, e recusa repetir uma semente
já gravada no mesmo banco e engine. Para repetir, arquive o diretório antes:

```bash
mv results/fase4 results/fase4_$(date +%d-%m)
```

**A suíte é destrutiva**: esvazia os bancos `up_*` do MongoDB ou o grafo do Neo4j,
conforme o banco pedido, e regera cada um a partir da semente. O `northwind` não
é tocado.

Detalhe de cada bateria e da suíte: `scripts/README.md`.

## Anonimização dos logs

Os cinco logs **não são saída bruta literal**. As mesmas três substituições do
log canônico foram aplicadas antes de versionar, todas em identidade de máquina,
nenhuma em número medido:

| Log | `<HOSTNAME>` | `<HOME>` | `<LAN_IP>` |
| --- | --- | --- | --- |
| `baterias_20261004_115923.log` | 23 | 0 | 441 |
| `baterias_20261004_125459.log` | 8 | 1 | 356 |
| `baterias_20261004_130229.log` | 24 | 0 | 372 |
| `baterias_20261004_131929.log` | 4 | 0 | 143 |
| `baterias_20261004_135251.log` | 12 | 0 | 149 |

Como no canônico, elas aparecem nas linhas de infraestrutura do Spark e na linha
`dataset:` do Northwind.

Além delas, o espaço no fim de linha foi removido de 282 linhas `INFO` do Spark
do oráculo (as `Removed TaskSet …, from pool` e as `Changing … acls groups to:`,
que terminavam em espaço). O hook `trailing-whitespace` do pre-commit o
removeria no commit de qualquer forma; o log canônico também não tem nenhuma
linha assim.

Verificado após a substituição, em cada log: o número de linhas é o mesmo; toda
linha alterada contém um dos três placeholders; não sobrou ocorrência do
hostname, do caminho nem do IP; e as linhas `Job N finished: …, took X s` estão
**byte a byte idênticas** ao original. As curvas e as tabelas geradas a partir
dos logs anonimizados saíram idênticas às geradas dos originais.

## Verificação de segredos

Nenhum dos 83 arquivos contém credencial, token, chave ou URI de conexão com
usuário. MongoDB e Neo4j rodaram em `localhost` sem autenticação; os XMIs contêm
apenas o modelo, e os CSVs apenas identificadores de corrida e números.

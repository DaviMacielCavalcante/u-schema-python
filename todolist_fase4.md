# TO-DO — Fase 4: Spark como paralelizador da extração

**Projeto:** Porte fiel e completo do U-Schema (Java/Spark/EMF) → Python — MongoDB e Neo4j
**Autores:** Davi Cavalcante · João — CESUPA
**Base:** `fase4_spark.md` · **Linha de base:** `results/` da Fase 3 · **Bugs:** `bugs_originais.md` §#8
**Pré-requisito:** Fase 3 fechada — a 3.4 (análise e redação) concluiu em 17/09/2026, com a prévia do TC qualificada

> **Organização por entrega.** Tarefas agrupadas por entregável (4.0–4.4), não
> por autor. Cada bloco define uma **Saída** que serve de critério de "pronto".
>
> **A fase mede, não promete velocidade.** A pergunta é de onde vem a diferença
> de crescimento entre porte e oráculo: arquitetura (o oráculo distribui, o
> porte roda em um processo) ou algoritmo (o número de esquemas distintos cresce
> 13 → 421 no documento). **"Não houve ganho" fecha o gate igual** — e é a
> resposta mais forte, porque joga a diferença inteira para o lado algorítmico.
>
> **Nada da Fase 2 muda.** `simplify`, `generate_document_pair`, `reduce_pairs`,
> `node_archetype` e `reduce_archetypes_by_node` ficam intocados. Se algum
> precisar mudar para caber no Spark, **parar** — a fidelidade da Fase 2 está em
> risco e a causa tem de ser entendida antes.

---

## Decisões tomadas em 17/09/2026 (com o Davi)

| # | Decisão | Consequência |
|---|---|---|
| 1 | **Começa agora** — a Fase 3 fechou | a linha de base é a do `results/` já analisado; não se remede para comparar |
| 2 | **Mongo primeiro; Neo4j só se o Mongo mostrar ganho** | 4.2 é **condicional** e pode não existir; se o Mongo não ganhar, o grafo não se justifica |
| 3 | **Cada processo lê sua fatia** (`mapPartitions`, cliente por executor) | paraleliza também a leitura, que é o gargalo; exige desenhar as fatias (4.0) |
| 4 | **Código mantido, não experimento** | backend escolhível na API, testes `@pytest.mark.spark`, JVM no CI |

**O que continua valendo da 2.0, e não se reabre:** a leitura é por **driver
nativo**. Passar pelo DataFrame do conector destruiria a tipagem BSON
(`ObjectId`/`Int64`) e uniformizaria os documentos num esquema único com nulos —
que é exatamente o sinal que a ferramenta mede. **Spark paraleliza o
map-reduce, nunca é camada de tipagem.**

---

## Cadeia de desbloqueio

```text
Fase 3 (linha de base) ─→ 4.0 (fatias + infra Spark) ─→ 4.1 (Mongo) ─→ 4.3 (determinismo) ─→ 4.4 (bateria)
                                                              └─→ 4.2 (Neo4j, condicional ao ganho da 4.1)
```

| Etapa | Depende de | Libera | Decide |
|---|---|---|---|
| **4.0** fatias + `SparkSession` | Fase 3 | 4.1 | como particionar `_id` cobrindo Rota A (`ObjectId`) e B (inteiro) |
| **4.1** backend Mongo | 4.0 | 4.2, 4.3 | **se existe ganho** — é o portão da 4.2 |
| **4.2** backend Neo4j | 4.1 **com ganho** | 4.3 | pode não acontecer, e isso é resultado |
| **4.3** determinismo sob partição | 4.1 | 4.4 | ordem forçada ou faixa reportada |
| **4.4** bateria comparativa | 4.3 | fim da fase | a resposta da fase |

---

## 4.0 — Fatias e infraestrutura Spark

- [x] `SparkSession` local mínima (`local[*]`), criada e fechada pelo backend, sem
      configuração global escondida — `extractors/spark.py::local_session`,
      *context manager* que fecha no `finally`.
- [x] **Desenhar a fatia do Mongo**: faixas de `_id` sobre a coleção, cobrindo
      `ObjectId` (Rota A) **e** inteiro (Rota B) — `extractors/partition.py`.
      `skip`/`limit` foi **descartado**: o `skip` percorre e descarta, então a
      soma das fatias varre ~2,8M de posições para ler 800 mil documentos, e o
      custo cresce com o número de fatias. O filtro `{"_id": {"$gte", "$lt"}}` é
      servido pelo índice.
- [x] Resolver o **desbalanceamento**: a subdivisão é por faixa de `_id`, não por
      coleção — os cortes saem de uma leitura ordenada só dos `_id` (*covered
      query*), a cada `total // fatias` posições. Fatias iguais por construção;
      a sobra da divisão fica na última.
- [x] Decidir e registrar o **número de fatias**: uma por **núcleo lógico**
      (`spark.py::default_slices`, `os.cpu_count() or 1`). Como as fatias já são
      equilibradas, não há motivo para super-particionar — só somaria custo fixo
      por tarefa. Falta a coluna nos CSVs (fica na 4.4).
- [x] Garantir que **o BSON não cruza a rede**: o `simplify` roda dentro do
      executor e o que volta é o dict de sentinelas (`str`/`int`/`float`/`bool`/
      `dict`/`list`). Se `ObjectId`/`Int64` aparecer na fronteira, o desenho vazou
      e a tipagem virou risco. **Só verificável na 4.1**, quando o `mapPartitions`
      existir. *Garantido por construção desde a 4.1:* o `_read_partition` só
      emite `str` (a chave canônica) e `int` (os dados); o dict nem viaja.
      **Confirmado em execução em 25/09/2026:** nos quatro bancos da comparação
      avulsa (Northwind, `testdb`, `up_a_small`, `up_b_small`), o único formato
      que sai do executor é `((str, str), (int, int, int))`.

**Premissa registrada (decisão do Davi, 20/09/2026):** o banco está **parado**
durante a extração — nada é inserido ou removido entre o cálculo dos cortes e a
leitura das fatias. É o cenário da ferramenta (inferir o esquema de uma base
para migrá-la) e o mesmo da bateria da Fase 3. Sem isso, faixas e contagem
poderiam divergir.

**Requisito de ambiente descoberto aqui:** o PySpark 4.1 exige **Java 17 ou 21**;
nesta máquina o `java` padrão é o **8** e `JAVA_HOME` vem vazio, então a sessão
não sobe. `local_session` converte a falha numa mensagem que diz isso, em vez do
`UnsupportedClassVersionError` cru. Para o pre-push passar, `JAVA_HOME` tem de
estar exportado no ambiente (há JDK 17 e 21 instalados em `/usr/lib/jvm/`); no CI,
é o `setup-java`.

**Saída:** esquema de partição escrito e testado (33 testes), sessão Spark subindo
e fechando (6 testes `spark`, os primeiros do repositório).

---

## 4.1 — Backend Spark do extrator MongoDB

> **Estado em 26/09/2026: Gate 4.1 fechado.** O backend (`extractors/mongo.py`)
> dá triplas idênticas às do Python, como conjunto, nos 5 testes do gate na suíte
> e, fora dela, nos 9 bancos da comparação avulsa (Northwind + os 8 `up_*`, 3
> corridas cada). Suíte completa: 665 testes verdes.

- [x] Backend escolhido em `extract_triples` pela `SparkSession` que ela recebe
      (`*, spark=None, slices=None`); `spark=None` é o caminho Python de hoje, e
      nenhum chamador existente muda. **Não** em `extract_database_triples`: ela
      recebe uma conexão já aberta, que não vai para o executor — cada partição
      precisa da URI para abrir a sua. A sessão vem pronta de fora para a 4.4
      medir o boot separado do trabalho. `slices` conta **por coleção**, a mesma
      unidade do `id_boundaries`.
- [x] `mapPartitions` sobre as fatias da 4.0; cada partição abre o próprio
      `MongoClient`, lê, e devolve pares já simplificados —
      `_read_partition`. As fatias `(coleção, inferior, superior)` de todas as
      coleções vão num RDD só, uma por partição (`parallelize(fatias,
      len(fatias))`). Lista de coleções vazia devolve `[]` antes do Spark: o
      `parallelize` com zero partições divide por zero.
- [x] `reduceByKey` com o `reduce_pairs` **como está** — `(min, max, soma)` é
      comutativa e associativa, provado na 2.0. Cabe sem *wrapper* porque a
      chave é `(coleção, _group_key(schema))` e o valor é só a tripla de dados.
      **A coleção entra na chave** porque o RDD é compartilhado: sem ela, duas
      coleções com o mesmo schema virariam uma tripla só.
- [x] No driver, sobre o resultado coletado, só o **final** do `build_triples`:
      anexar o `_type` e montar as linhas. O `build_triples` inteiro não serve
      ali — ele recebe documentos crus e faz o map e o reduce; depois do
      `reduceByKey`, o que chega são pares já agrupados. Esse final virou
      `_triple_row`, e a chave canônica virou `_group_key` — os dois
      compartilhados pelos backends, para que não divirjam em formato. O schema
      volta por `json.loads` da chave, com as chaves ordenadas em vez da ordem do
      documento; não muda nada a jusante, porque a inferência ordena os campos
      (`TreeSet`, `SchemaInference.java:190-194`).
- [x] Testes `@pytest.mark.spark` contra as mesmas fixtures-oráculo da 2.1 —
      5 testes, verdes em 26/09/2026. **Checados por mutação:** tirar a coleção
      da chave derruba 4 dos 5 (inclusive o que existe para isso); trocar o
      `reduce_pairs` por um *reduce* que descarta derruba 3. Cada teste também
      afirma que o resultado não é vazio — dois `[]` seriam "iguais" e o gate
      passaria em falso.
      **Comparar como conjunto**, não como lista: a ordem do `collect()` sai do
      hash do `reduceByKey`, não da primeira aparição — igualar a ordem é a 4.3.
      Exigem `mongod` no ar (ver o requisito de ambiente abaixo). Desenho, em
      `tests/unit/test_extractors_mongo_spark.py`:
      - **Banco de verdade, não coleção falsa.** Os workers do Spark são processos
        separados; um *fake* injetado no processo do teste não chega a eles.
      - **Banco temporário por teste**, com nome aleatório, apagado no fim. O
        teste não depende dos `up_*` que as baterias geram.
      - Marcados `spark` **e** `integration`, e **pulados** (com o motivo) se o
        `mongod` não responder — o CI de hoje não tem `mongod` (ver Infra).
      - Casos: o Northwind real (`_id` inteiro, coleções menores que as fatias);
        `ObjectId` espalhado por várias fatias (timestamps combinados entre
        partições); duas coleções com o mesmo schema (a coleção na chave); os
        tipos BSON (`Int64`, `bool`, `None`, aninhado, lista, lista vazia, ordem
        de chave); coleção vazia e lista de coleções vazia.
- [x] **Medir o ganho** nos quatro tamanhos e decidir a 4.2 com esse número.
      Medição **avulsa** de 25/09/2026 (mediana de 3, boot fora), não a bateria
      da 4.4. O ganho cresce com o tamanho: empate no `small`, **4,5×** no
      `larger`, nas duas rotas (Rota A 33,8s → 7,4s; Rota B 14,9s → 3,3s). Contra
      os jobs do oráculo no log canônico (sem boot), o porte Spark empata na Rota
      B (3,3s × 3,3s) e fica 2× acima na A. **Pela decisão 2, a 4.2 fica
      liberada.** Dados, scripts e gráfico em `~/Documents/uschema_fase4_medicoes/`
      (fora do repo; o `README.md` de lá traz as ressalvas).

**Requisito de ambiente descoberto aqui (25/09/2026):** o `mongod` continua não
subindo no kernel padrão, mas por outro motivo. O `7.0.0-34-generic` já é upstream
**7.0.14** (`/proc/version_signature`), a versão que corrige a incompatibilidade de
`rseq`, e o `mongod` **8.0.32** já libera 7.0.14+ (`SERVER-125742`) — só que lê a
versão do `uname`, onde o Ubuntu mostra `7.0.0`, e recusa. A correção para o
Ubuntu é o `SERVER-131779`, na **8.0.35**, ainda não lançada. Até lá:

- **Teste de igualdade (gate 4.1):** serve o `mongod` sem o
  `GLIBC_TUNABLES=glibc.pthread.rseq=0` do unit do pacote — testado, sobe neste
  kernel. O tcmalloc cai para cache por thread, mais lento, o que não afeta uma
  comparação de conjuntos.
- **Medição de tempo (4.1 e 4.4):** **não** nesse modo. A linha de base da Fase 3
  foi medida no `6.17.0-40` com o cache por CPU; medir o porte em outro regime do
  `mongod` mistura o efeito do backend com o do alocador. Bootar o 6.17 ou
  esperar a 8.0.35.

Para bootar o 6.17 uma vez só (o próximo boot volta ao padrão):

```bash
sudo grub-reboot "gnulinux-advanced-b7d6e68a-369f-4f7a-a480-73115485012b>gnulinux-6.17.0-40-generic-advanced-b7d6e68a-369f-4f7a-a480-73115485012b"
sudo reboot
# depois: `uname -r` deve dar 6.17.0-40-generic; então `sudo systemctl start mongod`
```

**Gate 4.1:** para o mesmo banco, o conjunto de `SchemaTriple` do backend Spark é
**idêntico** ao do backend Python — mesmos esquemas, mesmos `count`, mesmos
timestamps. Sem isso, nada do resto vale.

---

## 4.2 — Backend Spark do extrator Neo4j — **condicional**

> **Só começa se a 4.1 mostrar ganho.** Sem ganho no documento, o grafo não se
> justifica: a leitura é client-bound (~30k linhas/s, medido no `E1`) e o
> map-reduce é minúsculo (9 arquétipos em todos os tamanhos). Não executar este
> bloco é uma conclusão da fase, não uma pendência.

- [ ] Fatia do grafo: `skip`/`limit` sobre a *cypher* ordenada, por combinação de
      labels.
- [ ] `mapPartitions` sobre `node_archetype`; dedup por `element_id` vira
      `reduceByKey` pela chave do nó, **não** pelo arquétipo.
- [ ] Leitura **lazy** por partição — o `E1` foi a leitura *eager* enchendo o
      buffer; repetir isso dentro de um executor esconde o problema.
- [ ] `build_archetype_counts` e todo o `neo4j_model.py` continuam no driver (o
      grafo não passa pelo núcleo da Fase 1).
- [ ] Testes `@pytest.mark.spark` com driver falso particionado.

**Gate 4.2:** contagens idênticas às do backend Python, incluindo o `count` de
`RelationshipType` (fontes distintas, não arestas brutas) — é o que a partição
quebra mais fácil.

---

## 4.3 — Determinismo sob particionamento

O `count` não depende da partição, mas **a ordem das triplas depende** — e o
**#8 é ordem-dependente**: o colapso de variações descarta o `meta` da ocorrência
perdida, então outra ordem move a subcontagem de lugar. É a mesma sensibilidade
já medida no Northwind entre ler por arquivo (15 divergências) e por cursor (12).

**Escrever isto antes de rodar a bateria**, ou a primeira corrida vira alarme falso.

- [ ] Tentar a ordem igual por construção: fatias de `_id` ordenadas, concatenadas
      por índice, reproduzindo a ordem de um cursor ordenado por `_id` — com a
      linha de base Python rodando o mesmo `sort`. Se fechar, os dois backends
      ficam **idênticos tripla a tripla** e a dúvida some.
- [ ] Se não fechar, reportar a **faixa** de divergências não-fatais, nunca um
      número solto.
- [ ] Registrar o achado em `bugs_originais.md` §#8 — mais uma evidência de
      ordem-dependência, agora por partição.

**Gate 4.3:** `compare()` dá `equivalent=True` em todos os tamanhos, com as
divergências restantes **todas de `count`** e todas na assinatura do #8.
Divergência estrutural aqui é defeito do backend, não é o #8.

---

## 4.4 — Bateria comparativa

Reaproveita a infra da 3.0; a fase não constrói medição nova, só acrescenta uma
dimensão.

- [ ] Coluna `backend` (`python`/`spark`) nas tabelas de `results/`, com o
      significado escrito em `dicionario_de_dados.md` **antes** de medir.
- [ ] Rodar os quatro tamanhos nos dois backends, com a regra da mediana da 3.2.
- [ ] **Separar o boot do trabalho.** O oráculo já mostrou que o boot da JVM
      domina os tamanhos pequenos (9,21s para 150k nós); o backend Spark paga o
      mesmo pedágio, e sem separá-lo os tamanhos menores não dizem nada.
- [ ] Reportar as curvas: porte Python, porte Spark e oráculo, com a coluna
      `normalized` (a máquina do artigo é outra).
- [ ] Responder a pergunta da fase por escrito, nos dois sentidos possíveis.

**Saída:** tabelas de volume com a dimensão `backend` e a resposta da fase.

---

## Infra e qualidade (decisão 4)

- [ ] JVM no CI (`setup-java`) para o job que roda os testes `spark`.
- [ ] `mongod` no CI (*service container* `mongo:8.0`) para o teste do gate da
      4.1 — sem ele, o teste é pulado no CI e o gate só roda local. **Conferir o
      kernel do *runner*** antes: a guarda de `rseq` do `mongod` recusa de 6.19 a
      7.0.13 (ver o requisito de ambiente da 4.1), e o container usa o kernel do
      host.
- [ ] Testes novos marcados `@pytest.mark.spark` — pre-push e CI, **não**
      pre-commit (o marker já existe no `pyproject.toml` desde a Fase 2 e nunca
      foi usado; esta fase é a primeira a usá-lo).
- [x] `uv run mypy` limpo com `pyspark` (o `pyproject.toml` já libera
      `pyspark.*` do *strict*, por falta de stubs) — 66 arquivos, 25/09/2026,
      com o backend Mongo dentro. O `SparkSession` do `mongo.py` é importado só
      sob `TYPE_CHECKING`, para o caminho Python não carregar o `pyspark`.
- [ ] Atualizar o **CLAUDE.md**: `pyspark` deixa de ser "declarada, hoje não
      usada em runtime", e a suíte deixa de ser toda `unit`. Corrigir também o
      "`mapPartitions` entra depois sem reescrever nada": vale para o
      `reduce_pairs`, não para o `build_triples` (ver 4.1).

---

## Gate de aceite da Fase 4

- Backend Spark no Mongo (e no Neo4j, se a 4.1 justificar), **opcional**, com o
  Python como padrão e sem mudança no caminho existente.
- Triplas/arquétipos idênticos entre backends (4.1/4.2) e `equivalent=True`
  contra o oráculo nos quatro tamanhos (4.3).
- Curva medida nos dois backends, boot separado do trabalho, conclusão escrita
  — inclusive se for "sem ganho".
- Suíte `spark` verde no pre-push e no CI.

---

## Riscos

- **O ganho pode não existir.** Se o gargalo for o cliente do banco, dividir não
  resolve. É resultado publicável, mas frustra a expectativa de velocidade.
- **O boot da JVM domina os tamanhos pequenos** e inverte o sinal se não for
  separado.
- **A tipagem BSON é o único ponto onde a fase pode quebrar a equivalência da
  Fase 2 em silêncio** — daí o item de fronteira na 4.0.
- **O CI fica mais lento e mais pesado** (JVM + `pyspark`).
- **O teto de Python 3.12** deixa de ser restrição herdada sem contrapartida — ou,
  se a fase concluir que não há ganho, vira o argumento para remover o `pyspark`
  e subir o teto.

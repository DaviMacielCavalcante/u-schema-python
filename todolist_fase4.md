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
| 4 | **Código mantido, não experimento** | engine escolhível na API, testes `@pytest.mark.spark`, JVM no CI |

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
| **4.1** engine Spark (Mongo) | 4.0 | 4.2, 4.3 | **se existe ganho** — é o portão da 4.2 |
| **4.2** engine Spark (Neo4j) | 4.1 **com ganho** | 4.3 | pode não acontecer, e isso é resultado |
| **4.3** determinismo sob partição | 4.1 | 4.4 | ordem forçada ou faixa reportada |
| **4.4** bateria comparativa | 4.3 | fim da fase | a resposta da fase |

---

## 4.0 — Fatias e infraestrutura Spark

- [x] `SparkSession` local mínima (`local[*]`), criada e fechada pela engine, sem
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

## 4.1 — Engine Spark do extrator MongoDB

> **Estado em 26/09/2026: Gate 4.1 fechado.** A engine (`extractors/mongo.py`)
> dá triplas idênticas às do Python, como conjunto, nos 5 testes do gate na suíte
> e, fora dela, nos 9 bancos da comparação avulsa (Northwind + os 8 `up_*`, 3
> corridas cada). Suíte completa: 665 testes verdes.

- [x] Engine escolhida em `extract_triples` pela `SparkSession` que ela recebe
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
      compartilhados pelas engines, para que não divirjam em formato. O schema
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
  `mongod` mistura o efeito da engine com o do alocador. Bootar o 6.17 ou
  esperar a 8.0.35.

Para bootar o 6.17 uma vez só (o próximo boot volta ao padrão):

```bash
sudo grub-reboot "gnulinux-advanced-b7d6e68a-369f-4f7a-a480-73115485012b>gnulinux-6.17.0-40-generic-advanced-b7d6e68a-369f-4f7a-a480-73115485012b"
sudo reboot
# depois: `uname -r` deve dar 6.17.0-40-generic; então `sudo systemctl start mongod`
```

**Gate 4.1:** para o mesmo banco, o conjunto de `SchemaTriple` da engine Spark é
**idêntico** ao da engine Python — mesmos esquemas, mesmos `count`, mesmos
timestamps. Sem isso, nada do resto vale.

---

## 4.2 — Engine Spark do extrator Neo4j — **condicional**

> **Só começa se a 4.1 mostrar ganho.** Sem ganho no documento, o grafo não se
> justifica: a leitura é client-bound (~30k linhas/s, medido no `E1`) e o
> map-reduce é minúsculo (9 arquétipos em todos os tamanhos). Não executar este
> bloco é uma conclusão da fase, não uma pendência.
>
> **Liberada em 26/09/2026** pelo ganho da 4.1 (4,5× no `larger`). Testes do
> gate em `tests/unit/test_extractors_neo4j_spark.py`, validados contra uma
> implementação de referência fora do repo (passam todos).
>
> **Estado em 30/09/2026: Gate 4.2 fechado.** As cinco funções da engine
> (`extractors/neo4j.py`) estão prontas, sem nenhum `TODO(4.2)`. Os 10 testes
> do gate passam com o driver falso. Fora da suíte, as duas engines deram
> contagens idênticas como conjunto no `up_larger` do Neo4j local: 9
> arquétipos, 1,2 milhão de nós contados nos dois lados. Suíte completa: 670
> testes verdes e 5 pulados (os do gate da 4.1, sem `mongod` no ar).
>
> Pendência de acabamento, sem bloquear o gate: os dois ramos do
> `_read_label_combination` repetem a *query* inteira. A sugestão é calcular
> antes a condição extra e os parâmetros, e manter uma *query* só, para as duas
> cópias não divergirem.
>
> **O desenho muda o plano abaixo em dois pontos** (riscados nos itens), pelo
> que se mediu no Neo4j local (2026.09 Community, `up_larger` carregado):
>
> 1. **Fatia por `id(n) % slices`, não por `skip`/`limit` nem por faixa.** O
>    `skip` percorre e descarta (o mesmo motivo do Mongo, 4.0). Faixa de id
>    desbalanceia: os ids dos labels são **intercalados** (`Movie` de 0 a
>    1.217.297, `User` de 9.670 a 1.219.984 — as baterias apagam e recriam, e o
>    Neo4j reaproveita ids). O resto da divisão é equilibrado por construção e
>    dispensa consulta de cortes. O plano do servidor é `NodeByLabelScan →
>    Filter → OptionalExpand`: o filtro roda **antes** de expandir as arestas.
>    **Risco:** `id()` é *deprecated* (o servidor devolve aviso); se sair, a
>    troca é para `elementId`.
> 2. **O pipeline puro inteiro roda dentro da partição**, e o Spark só soma as
>    contagens. Como o filtro é sobre `n`, **cada nó cai inteiro numa partição**
>    — todas as arestas dele vêm na mesma *query*. Então `node_archetype` +
>    `reduce_archetypes_by_node` + `build_archetype_counts` (o
>    `extract_archetype_counts`, sem mudar uma linha) rodam no executor, e o
>    `reduceByKey` só soma `(arquétipo, contagem)`. O plano previa `reduceByKey`
>    por nó e o `build_archetype_counts` no driver: traria ~1,2 milhão de
>    arquétipos de nó para o driver e obrigaria a reescrever a fusão por nó
>    como função par-a-par — risco de fidelidade na parte mais delicada.
>
> Mais três decisões: o **arquétipo viaja no valor**, junto com a contagem (não
> reconstruído por `json.loads` como no Mongo — aqui o `neo4j_model` não ordena
> as propriedades, e a ordem delas vira a ordem dos atributos); o driver entra
> como **fábrica** (`partial(GraphDatabase.driver, uri, auth=auth)` em
> produção, driver falso nos testes); e uma **porta de entrada por URI**,
> `extract_archetype_counts_from_uri`, que o grafo não tinha.

- [x] ~~Fatia do grafo: `skip`/`limit` sobre a *cypher* ordenada~~ → fatia por
      combinação de labels × `id(n) % slices`, com o filtro no
      `_read_label_combination` (parâmetros opcionais; sem eles, a *query* é a
      de sempre). A lista de fatias é montada no
      `_extract_archetype_counts_spark`, uma por partição
      (`parallelize(fatias, len(fatias))`). Banco sem nó devolve `[]` antes do
      Spark, como no Mongo.
- [x] ~~`mapPartitions` sobre `node_archetype`; dedup por `element_id` vira
      `reduceByKey` pela chave do nó~~ → `mapPartitions` com o
      `extract_archetype_counts` inteiro por fatia (`_count_partition`); a
      dedup por nó fica dentro da partição, porque o nó está inteiro nela.
- [x] Leitura **lazy** por partição — o `E1` foi a leitura *eager* enchendo o
      buffer; repetir isso dentro de um executor esconde o problema. (O
      `_read_label_combination` já lê em *streaming*; a fatia só acrescenta o
      filtro.)
- [x] ~~`build_archetype_counts` [...] continuam no driver~~ →
      `build_archetype_counts` roda por partição; no driver fica só a soma
      (`_merge_counts`) e todo o `neo4j_model.py` (o grafo não passa pelo núcleo
      da Fase 1).
- [x] Testes `@pytest.mark.spark` com driver falso particionado — 10: a *query*
      com e sem fatia, o `_merge_counts`, o `sampling_rate` nas duas engines, a
      igualdade como conjunto, o `count` de relacionamento por fontes
      distintas, o banco vazio e a porta de entrada por URI com `spark`. O
      driver falso chega aos workers por `cloudpickle.register_pickle_by_value`
      — por referência, o worker não consegue importar o módulo de teste. **Sem
      Neo4j de verdade**: o Community tem um banco só, e o teste não pode sujar
      o das baterias.
      O teste da porta por URI entrou em 30/09 para cobrir um buraco: o de
      `sampling_rate` termina no `raise` e nunca chega à chamada da engine Spark.
      Uma chamada errada ali (o caminho Python recebendo a fábrica) passou pelos
      outros 9, e só o `mypy` pegou. **Checado por mutação:** com esse erro de
      volta, o teste novo falha com `AttributeError`.
- [x] **Comparar as duas engines no Neo4j real.** Medição **avulsa** de
      30/09/2026 (uma corrida, boot fora), no `up_larger`: 1,2 milhão de nós e
      10,2 milhões de arestas, com 32 fatias por combinação de labels (64
      partições). Contagens idênticas como conjunto. Python 446s, Spark 41s,
      cerca de **11×**. É indicativo e não substitui a bateria da 4.4. O número
      é compatível com a caracterização do `E1`: a leitura é limitada pelo
      cliente, a ~30k linhas/s, e as ~11 milhões de linhas dariam ~370s. Com 64
      clientes lendo em paralelo, esse gargalo se divide. O servidor devolveu o
      aviso de `id()` *deprecated* uma vez por partição (64); é o risco já
      registrado no desenho. Script e saída em `~/Documents/uschema_fase4_medicoes/`
      (`compare_backends_neo4j.py`/`.out`).

**Gate 4.2:** contagens idênticas às da engine Python, incluindo o `count` de
`RelationshipType` (fontes distintas, não arestas brutas) — é o que a partição
quebra mais fácil.

---

## 4.3 — Determinismo sob particionamento

O `count` não depende da partição, mas **a ordem das triplas depende** — e o
**#8 é ordem-dependente**: o colapso de variações descarta o `meta` da ocorrência
perdida, então outra ordem move a subcontagem de lugar. É a mesma sensibilidade
já medida no Northwind entre ler por arquivo (15 divergências) e por cursor (12).

**Escrever isto antes de rodar a bateria**, ou a primeira corrida vira alarme falso.

> **Estado em 03/10/2026: Gate 4.3 fechado.** Desenho decidido em 01/10/2026 (a
> opção 2, ordem igual por construção), com os testes do gate validados antes
> contra uma implementação de referência fora do repo. Implementado em
> 02/10/2026 (`extractors/mongo.py`): suíte completa com 681 testes verdes e
> nenhum pulado (com `mongod` no ar). Gate contra o oráculo em 03/10/2026, nos
> dois paradigmas (abaixo). Registrado em `bugs_originais.md` §#8,
> "Ordem-dependência sob partição".
>
> - **A ordem comum é a de primeira aparição, com cada coleção lida em ordem de
>   `_id`.** No Python, o `find()` ganha `sort("_id", 1)`. No Spark, cada fatia
>   leva o seu número, a partição lê ordenada por `_id` e emite a posição
>   (fatia, documento) junto com os dados, o `reduceByKey` fica com a menor
>   posição (`_reduce_with_position`, que chama o `reduce_pairs` sem mudá-lo) e
>   o driver ordena por ela.
> - **"Concatenar as partições por índice" não basta sozinho:** o
>   `reduceByKey` embaralha a ordem de novo. A posição da primeira aparição
>   tem de viajar no valor.
> - **O `sort` dentro da partição é necessário** mesmo com o filtro de faixa: a
>   coleção menor que o número de fatias vira uma fatia sem limites, com filtro
>   `{}`, que vem na ordem física.
> - **O `sort` não muda a linha de base da bateria.** Medido em 01/10/2026: os
>   oito `up_*` regerados com a semente 23 têm a ordem física igual à de `_id`
>   (o gerador grava em ordem), e as listas de triplas com e sem `sort` são
>   idênticas nos oito (13/31/111/421 linhas na Rota A, 21/43/133/463 na B, as
>   mesmas da Fase 3). Ressalva da Rota A: o contador do `ObjectId` começa
>   num valor sorteado e dá a volta em 2²⁴; se der a volta durante a geração
>   (chance de ~1% no `small` a ~7% no `larger`), os documentos daquele segundo
>   saem fora de ordem. **Muda** o Northwind lido do banco, carregado fora de
>   ordem: previsão de 12 → 15 divergências (o arquivo está em ordem de
>   `_id`), confirmada em 03/10/2026 (abaixo).
> - **O grafo não precisa de nada.** O `neo4j_model` soma os `count` ao fundir
>   variações repetidas, então não tem o #8. As 40.320 ordens possíveis dos 8
>   arquétipos do golden-master dão `equivalent=True` com zero divergências nos
>   quatro XMIs do oráculo. No grafo, o gate é só rodar o `compare()`.
> - **Testes:** o gate da 4.1 passa a comparar **como lista**; os dados são
>   gravados fora da ordem de `_id`, para o teste não passar por sorte.
>   **Checados por mutação:** tirar o `sort` do Python, o `sort` da partição ou
>   a ordenação no driver, trocar o menor pelo maior, tirar a fatia da posição
>   ou descartar os dados no *reduce* — cada uma derruba ao menos um teste.
>
> **Gate contra o oráculo rodado em 03/10/2026.** Os dois `run_oracle_*` ganharam
> `--engine`/`--slices` (commit `4da37c4`). Semente 23, engine Spark, tabelas
> fora de `results/` (`out/gate_4_3/` e `out/gate_4_3_neo4j/`, com os logs).
>
> - **Mongo, 8 bancos: passou.** `equivalent=True` nos oito. As 46 divergências
>   restantes são todas `count`, não-fatais, e **idênticas linha a linha** às da
>   linha de base Python da Fase 3 (`results/divergences.csv`, semente 23). O XMI
>   Spark é igual ao Python byte a byte na Rota B; na A, só diferem
>   `firstTimestamp`/`lastTimestamp`, que vêm do `ObjectId` e guardam a hora em
>   que o banco foi gerado. A ordem fechou: não há faixa a reportar.
> - **Neo4j, 4 tamanhos: passou.** Os quatro deram `equivalent=True` com zero
>   divergências contra o oráculo semeado (e as 7 de sempre contra
>   `resources/`). Os XMIs diferem dos da engine Python só na ordem de entidades
>   e variações, e no `variationId` que segue essa ordem — o `compare()` não vê,
>   como previsto acima.
> - **O `up_larger` precisou de duas tentativas.** Na primeira, uma das 64
>   fatias rastejou a ~46 KB/s por 40 minutos, com o worker e o Neo4j ociosos, e
>   a corrida foi interrompida — ver `bugs_originais.md` §**E1**, "Reincidência
>   sob streaming". A segunda, no mesmo dia, extraiu em 67 s.

- [x] Tentar a ordem igual por construção: fatias de `_id` ordenadas, concatenadas
      por índice, reproduzindo a ordem de um cursor ordenado por `_id` — com a
      linha de base Python rodando o mesmo `sort`. Se fechar, as duas engines
      ficam **idênticas tripla a tripla** e a dúvida some. **Fechou** (gate do
      Mongo de 03/10/2026, acima).
- [x] ~~Se não fechar, reportar a **faixa** de divergências não-fatais, nunca um
      número solto.~~ *Não se aplica: fechou.*
- [x] Gate do grafo no `up_larger` — passou na segunda tentativa (03/10/2026).
- [x] Medir o Northwind lido do banco com o `sort` (`run_northwind.py`, outro
      `--output-dir`): previsão de 12 → 15 divergências. **Confirmada em
      03/10/2026** (`out/northwind_4_3/`): o caminho `database` dá 15, idênticas
      linha a linha às do `file`, com XMI igual byte a byte; o `file` segue com
      as mesmas 15 da bateria canônica. `equivalent=True` e 14/17 nos dois. O
      XMI canônico `out/porte/mongo_northwind_database.xmi` foi restaurado; o
      novo está em `out/northwind_4_3/mongo_northwind_database_sorted.xmi`.
- [x] Registrar o achado em `bugs_originais.md` §#8 — mais uma evidência de
      ordem-dependência, agora por partição. Feito em 03/10/2026 ("Ordem-
      dependência sob partição"), com o Northwind.

**Gate 4.3:** `compare()` dá `equivalent=True` em todos os tamanhos, com as
divergências restantes **todas de `count`** e todas na assinatura do #8.
Divergência estrutural aqui é defeito da engine, não é o #8. **Cumprido em
03/10/2026**, nos dois paradigmas.

---

## 4.4 — Bateria comparativa

Reaproveita a infra da 3.0; a fase não constrói medição nova, só acrescenta uma
dimensão.

> **Estado em 05/10/2026: bateria rodada, curvas e tabelas feitas; falta a resposta
> escrita e o CI.**
>
> - **Bateria de 04/10/2026, semente 23, nas duas engines.** 50 corridas em
>   `results/fase4/`, as 42 comparações com `equivalent=True` e nenhuma corrida
>   repetida. Logs: `logs/baterias_20261004_125459.log` e `_130229`
>   (MongoDB, Python e Spark); `_131929` e `_135251` (Neo4j).
> - **Os dois bancos em kernels diferentes.** O MongoDB no `6.17.0-40`, o da linha
>   de base; o Neo4j no `7.0.0-34`. Uma primeira suíte inteira no 6.17 parou no
>   `oracle_chain` `up_large` Spark do grafo, travado três vezes seguidas (63/64
>   fatias, uma conexão a 49 KB/s); no 7.0, a suíte do grafo passou de primeira.
>   O travamento já era conhecido, e a saída também: rodar o grafo num kernel
>   mais novo. O mecanismo provável é uma regressão do TCP do 6.17
>   (`bugs_originais.md` §E1, "Regressão do kernel 6.17"). Para rodar cada banco
>   no seu kernel, o `run_suite.sh` ganhou um terceiro argumento (`mongodb` ou
>   `neo4j`).
> - **Extração no `larger`**, média das duas corridas por engine, boot fora:
>
>   | banco | Python | Spark | oráculo (jobs) | Python/Spark | Spark/oráculo |
>   |---|---|---|---|---|---|
>   | MongoDB, Rota A | 32,14 s | 8,73 s | 3,69 s | 3,7× | 2,4× |
>   | MongoDB, Rota B | 15,38 s | 4,46 s | 3,49 s | 3,4× | 1,3× |
>   | Neo4j | 422,73 s | 39,31 s | 35,20 s | 10,8× | 1,1× |
>
>   No `small` do MongoDB, a Spark perde para a Python nas duas rotas (2,31 ×
>   1,73 s na A; 2,24 × 1,04 s na B). No Neo4j, porte Spark e oráculo andam
>   juntos nos quatro tamanhos (de 0,84× a 1,12×).
> - **Curvas e tabelas**, fora do repo, em `~/Documents/uschema_fase4_medicoes/`:
>   `plot_backends.py` gera uma figura por banco, e `tabelas_4_4.py` refaz as
>   Tabelas 2, 3 e 4 do TC num `.docx` sem legenda, com o próprio TC de modelo.
>   Os dois leem o `results/fase4/runs.csv` e os quatro logs; a docstring de cada
>   um diz de onde sai cada número.
> - **Contagens da rota A iguais às da Fase 3.** As duas engines do porte dão as
>   mesmas contagens de `User`, e elas repetem as da Tabela 4 do TC, assim como o
>   oráculo da suíte Python. O oráculo da suíte Spark difere só no `larger`:
>   20.901, com 940 na variação com `favoriteMovies`, contra 20.932 e 971. As
>   Tabelas 3 e 4 usam o da suíte Python.

- [x] Coluna `engine` (`python`/`spark`) nas tabelas de `results/`, com o
      significado escrito em `dicionario_de_dados.md` **antes** de medir. Entrou
      junto com o `boot_time`, só em `results/fase4/` (commit `7c68227`).
- [x] Rodar os quatro tamanhos nas duas engines, com a regra da mediana da 3.2.
      Na semente única (convenção desde 15/08, `todolist_fase3.md`), a regra é
      repetir a corrida que sai da curva; na bateria de 04/10 nenhuma precisou.
      Cada ponto tem duas corridas por engine, uma do `size` e outra do
      `oracle_chain`, que concordaram a menos de 10%.
- [x] **Separar o boot do trabalho.** O oráculo já mostrou que o boot da JVM
      domina os tamanhos pequenos (9,21s para 150k nós); a engine Spark paga o
      mesmo pedágio, e sem separá-lo os tamanhos menores não dizem nada.
      Coluna `boot_time`, fora do `total_time`, com uma corrida Spark por
      processo para todo boot ser o de uma JVM nova. Ficou em ~2 s em todas as
      corridas de 04/10. No oráculo, o boot sai somando os jobs Spark do log, como
      na 4.1.
- [x] Reportar as curvas: porte Python, porte Spark e oráculo, com a coluna
      `normalized` (a máquina do artigo é outra). Curvas de tempo de extração e
      a Tabela 2 do TC refeita com o `normalized`, uma linha por engine
      (05/10/2026, ver o estado acima).
- [ ] Responder a pergunta da fase por escrito, nos dois sentidos possíveis.

**Saída:** tabelas de volume com a dimensão `engine` e a resposta da fase.

---

## Infra e qualidade (decisão 4)

> **O CI nunca rodou os testes `spark`** (conferido em 05/10/2026). Ele só
> dispara em push na `main` e em PR, e a `feat/spark` ainda não tem PR. Não se
> sabe se a JVM que já vem no *runner* basta.

- [ ] JVM no CI (`setup-java`) para o job que roda os testes `spark`.
- [ ] `mongod` no CI (*service container* `mongo:8.0`) para o teste do gate da
      4.1 — sem ele, o teste é pulado no CI e o gate só roda local. **Conferir o
      kernel do *runner*** antes: a guarda de `rseq` do `mongod` recusa de 6.19 a
      7.0.13 (ver o requisito de ambiente da 4.1), e o container usa o kernel do
      host.
- [x] Testes novos marcados `@pytest.mark.spark` — pre-push e CI, **não**
      pre-commit (o marker já existe no `pyproject.toml` desde a Fase 2 e nunca
      foi usado; esta fase é a primeira a usá-lo). Os do gate da 4.1 levam
      também `integration` (precisam do `mongod`); os da 4.2 que só olham a
      *query* e o `_merge_counts` são `unit` e rodam no pre-commit.
- [x] `uv run mypy` limpo com `pyspark` (o `pyproject.toml` já libera
      `pyspark.*` do *strict*, por falta de stubs) — 66 arquivos, 25/09/2026,
      com a engine Spark do Mongo dentro. O `SparkSession` do `mongo.py` é importado só
      sob `TYPE_CHECKING`, para o caminho Python não carregar o `pyspark`.
- [x] Atualizar o **CLAUDE.md**: `pyspark` deixa de ser "declarada, hoje não
      usada em runtime", e a suíte deixa de ser toda `unit`. Corrigir também o
      "`mapPartitions` entra depois sem reescrever nada": vale para o
      `reduce_pairs`, não para o `build_triples` (ver 4.1). Feito em
      26/09/2026, junto com o estado da fase (4.0/4.1 fechadas, 4.2 com
      esqueleto). Revisado em 30/09/2026, com a 4.2 fechada: o pacote não tem
      mais stubs.

---

## Gate de aceite da Fase 4

- Engine Spark no Mongo (e no Neo4j, se a 4.1 justificar), **opcional**, com o
  Python como padrão e sem mudança no caminho existente.
- Triplas/arquétipos idênticos entre engines (4.1/4.2) e `equivalent=True`
  contra o oráculo nos quatro tamanhos (4.3).
- Curva medida nas duas engines, boot separado do trabalho, conclusão escrita
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

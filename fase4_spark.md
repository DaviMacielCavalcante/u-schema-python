# Fase 4 — Spark como paralelizador da extração (guia detalhado)

**Parte de:** `roadmap_portabilidade.md`
**Entregável:** backend Spark opcional para os dois extratores + bateria comparativa · **Pré-requisito:** Fase 3 fechada (é dela que vem a linha de base)

> **Este documento é o plano; as tarefas e o status estão em
> `todolist_fase4.md`**, que é a fonte da verdade da fase — mesma divisão das
> Fases 0–3. As quatro decisões de desenho foram tomadas em 17/09/2026 e estão
> registradas lá.
>
> **Esta fase não reabre a decisão da 2.0.** A leitura continua por **driver
> nativo**: nenhum conector oficial (Mongo v11.x, Neo4j v6.x) expõe mais a API
> RDD que o oráculo usa, e passar pelo DataFrame destruiria a tipagem BSON
> (`ObjectId`/`Int64` viram string ou long, e a assinatura muda). **Spark entra
> como paralelizador do map-reduce, nunca como camada de tipagem** —
> exatamente o que a 2.0 deixou em aberto.

## Objetivo

Dar aos extratores da Fase 2 um **backend Spark opcional**, mantendo o
Python como padrão, e medir o que ele muda na curva tempo × volume. O
`pyspark` já é dependência declarada (e é o motivo do teto em Python 3.12);
hoje não é usado em runtime. A Fase 4 ou o justifica com medida, ou o remove.

## Por que agora, e qual é a pergunta

A 3.2 mediu, na mesma máquina e sobre a mesma instância, que **a razão
porte/oráculo cresce com o tamanho** (grafo: 26,3× de crescimento no porte
contra 5,25× do oráculo para 8× de dado). Isso já descarta "Python é N vezes
mais lento" — constante de linguagem daria razão constante. Duas explicações
sobreviveram e a Fase 3 não separou: **algorítmica** (estruturas que degradam
com o número de esquemas distintos — 13 → 421 no documento) e **arquitetural**
(o oráculo distribui o map-reduce em executores; o porte roda single-thread).

A pergunta da fase é essa separação:

> Rodando o **mesmo** código de assinatura sob um paralelizador, a curva do
> porte se aproxima da do oráculo?

Um **ganho** confirma a explicação arquitetural. **Nenhum ganho** é resultado
igualmente publicável e mais forte: joga a diferença inteira para o lado
algorítmico e aponta onde otimizar. O risco a evitar é só um — vender a fase
como "deixar mais rápido" em vez de "medir de onde vem a diferença".

---

## 4.0 — Fronteira: o que o Spark paraleliza

O map-reduce do oráculo tem três etapas; só duas são candidatas.

| Etapa | Paraleliza? | Por quê |
|---|---|---|
| leitura (cursor `pymongo` / `execute_query`) | **só com múltiplos cursores** | com um cursor só, o driver é o gargalo — é o que o `E1` já mostrou no grafo (client-bound a ~30k linhas/s) |
| assinatura (`simplify`/`generate_document_pair`, `node_archetype`) | **sim** | função pura por documento/linha |
| redução (`reduce_pairs`, `reduce_archetypes_by_node`) | **sim** | `(min, max, soma)` é comutativa e associativa — provado na 2.0 |

Dois desenhos possíveis, e a escolha não é neutra:

- **(a) `sc.parallelize` sobre lotes do cursor** — o driver lê tudo e distribui.
  Simples, mas mantém a leitura serial no driver e ainda paga *pickle* de ida.
  Como a leitura é justamente o lado client-bound, (a) paraleliza o que não é o
  gargalo.
- **(b) `mapPartitions` sobre partições lógicas**, cada executor abrindo o
  próprio `MongoClient`/`GraphDatabase.driver` e lendo a sua fatia. Paraleliza
  **também a leitura**, e o BSON nunca sai do executor — a tipagem nativa fica
  preservada, que é a armadilha registrada na 2.0.

**Decidido: (b)** (17/09/2026). A unidade natural de partição é a coleção (Mongo) e a
combinação de labels (Neo4j) — mas ela **não basta**: o User Profiles tem duas
coleções e nove arquétipos, então particionar por unidade natural dá duas
partições úteis num caso e nove no outro, com uma dominante. É preciso
**subparticionar por faixa de `_id`** (Mongo) e por `skip`/`limit` sobre a
*cypher* ordenada (Neo4j). Sem isso a fase mede o paralelismo de duas tarefas
desbalanceadas e conclui errado.

A fronteira de tipagem fica **antes** do Spark: o `simplify`/`node_archetype`
roda dentro do executor, e o que volta é o dict de sentinelas — `str`/`int`/
`float`/`bool`/`dict`/`list`, JSON puro, que *pickla* sem perda. `ObjectId` ou
`Int64` aparecendo na fronteira é sinal de que o desenho vazou para (a).

**Saída:** desenho de partição escrito, com o caso da coleção única dominante resolvido.

---

## 4.1 — Backend Spark do extrator MongoDB

`extract_database_triples` ganha um parâmetro de backend; **o padrão continua
Python** e nenhum caminho existente muda de assinatura. O `reduce_pairs`
vai direto para `reduceByKey` — é o mesmo `(min, max, soma)`.

Nada de `simplify`, `generate_document_pair` ou `reduce_pairs` muda — se algum
precisar mudar para caber no Spark, a fidelidade da Fase 2 está em risco e o
trabalho para até a causa estar entendida. O `build_triples` continua no driver,
sobre o resultado coletado.

**Gate 4.1:** para o mesmo banco, o conjunto de `SchemaTriple` do backend Spark
é **idêntico** ao do Python — mesmos esquemas, mesmos `count`, mesmos
timestamps.

---

## 4.2 — Backend Spark do extrator Neo4j — condicional ao ganho da 4.1

**Só acontece se o Mongo mostrar ganho** (decisão de 17/09/2026). Sem ganho no
documento, o grafo não se justifica: a leitura é client-bound (~30k linhas/s,
medido no `E1`) e o map-reduce é minúsculo — nove arquétipos em todos os
tamanhos. **Não executar este bloco é uma conclusão da fase, não uma pendência.**

O grafo **não passa pelo núcleo da Fase 1** (achado da 2.2): o que paraleliza
aqui é `node_archetype` + `reduce_archetypes_by_node`; `build_archetype_counts`
e todo o `neo4j_model.py` continuam no driver. A dedup por `element_id` vira
`reduceByKey` pela chave do nó, não pelo arquétipo, e a leitura de cada partição
tem de ser **lazy** — o `E1` foi exatamente a leitura *eager* enchendo o buffer,
e repeti-la dentro de um executor esconde o problema em vez de mostrá-lo.

**Gate 4.2:** contagens idênticas às do backend Python, incluindo a
semântica de `count` em `RelationshipType` (fontes distintas, não arestas
brutas) — é a que a partição mais facilmente quebra.

---

## 4.3 — Determinismo sob particionamento (o ponto delicado da fase)

O `count` da tripla é invariante à partição, mas **a ordem em que as triplas
chegam ao `BuildUSchema` não é** — e o **#8 é ordem-dependente**: o colapso de
variações descarta o `meta` da ocorrência perdida, então uma ordem diferente
move a subcontagem de lugar. É a mesma sensibilidade já medida entre ler o
Northwind por arquivo (15 divergências) e por cursor (12), com o mesmo dado.

Consequência: **mudar de backend muda a quantidade de divergências não-fatais,
sem quebrar a equivalência.** Isso é resultado esperado, não regressão — e
precisa estar escrito antes de a bateria rodar, ou vira alarme falso.

Duas formas de tratar, e a segunda é a que se tenta primeiro:

1. Aceitar a ordem-dependência e reportar a faixa, como a Fase 3 já faz.
2. **Tornar as duas ordens iguais por construção:** particionar por faixa de
   `_id` ordenada e concatenar as partições por índice reproduz a ordem de um
   cursor ordenado por `_id`. A linha de base Python roda com o mesmo
   `sort`, e aí os dois backends ficam **idênticos tripla a tripla** — o que
   remove a dúvida inteira em vez de documentá-la.

Se (2) não fechar, cai-se para (1) **com a faixa medida**, nunca com um número
solto — e o achado entra em `bugs_originais.md` §#8 como mais uma evidência de
ordem-dependência, agora por partição.

**Gate 4.3:** `compare()` dá `equivalent=True` nos dois paradigmas e em todos os
tamanhos, com as divergências restantes **todas de `count`** e todas na
assinatura do #8. Divergência estrutural aqui é defeito do backend, não do #8.

---

## 4.4 — Bateria comparativa e infraestrutura

A bateria da Fase 3 é reaproveitada — a fase não constrói medição nova, só
acrescenta uma dimensão.

Os quatro tamanhos rodam nos dois backends, com a regra da mediana da 3.2, e o
resultado sai como **três curvas** — porte Python, porte Spark e oráculo —, com a
coluna `normalized`, já que a máquina do artigo é outra. As tabelas de `results/`
ganham a coluna `backend`, com o significado escrito em `dicionario_de_dados.md`
**antes** de medir.

Uma armadilha de leitura: o **custo fixo de boot** da JVM/Spark tem de sair do
tempo de trabalho. O oráculo já mostrou que ele domina os tamanhos pequenos
(9,21s para 150k nós, quase tudo boot); o backend Spark paga o mesmo pedágio, e
sem separá-lo a comparação nos tamanhos menores não diz nada.

A infraestrutura que a decisão de manter o backend exige — JVM no CI para o job
de testes `spark`, pre-commit continuando só com `unit` — está no todolist.

**Saída:** as tabelas de volume com a dimensão `backend`, e a resposta à pergunta da fase.

---

## Gate de aceite da Fase 4

- Backend Spark disponível nos dois extratores, **opcional**, com o Python
  como padrão e sem mudança de comportamento no caminho existente.
- Triplas/arquétipos idênticos entre backends (4.1/4.2) e `equivalent=True`
  contra o oráculo nos quatro tamanhos (4.3).
- Curva tempo × volume medida nos dois backends, com boot separado do trabalho,
  e a conclusão declarada nos dois sentidos possíveis — inclusive "não houve
  ganho", que fecha o gate igual.
- Suíte `spark` verde no pre-push e no CI.

## Entregáveis

Backend Spark em `extractors/mongo.py` e `extractors/neo4j.py`, testes
`@pytest.mark.spark`, coluna `backend` nas tabelas e no dicionário de dados, a
tabela comparativa das três curvas, e o material do capítulo sobre a origem da
diferença de crescimento.

## Riscos da fase

**O ganho pode não existir** — a leitura é client-bound nos dois paradigmas
(`E1`), e se o gargalo for o driver, particionar não resolve; é resultado, mas
frustra quem esperava velocidade. **O boot da JVM domina os tamanhos pequenos**,
invertendo o sinal se não for separado. **A tipagem BSON atravessa o *pickle***,
e qualquer perda ali muda a assinatura silenciosamente — o único ponto onde a
fase pode quebrar a equivalência da Fase 2 sem alarme. **O CI fica mais lento e
mais pesado** (JVM + `pyspark`). E **o teto de Python 3.12** deixa de ser uma
restrição herdada sem contrapartida e passa a ter uso real — ou, se a fase
concluir que não há ganho, vira o argumento para remover o `pyspark` e subir o
teto.

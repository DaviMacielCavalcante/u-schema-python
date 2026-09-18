# Roadmap de portabilidade — porte fiel e completo do U-Schema para Python

**Escopo deste documento:** *apenas* a portabilidade da ferramenta. A contribuição original (metacamada de acesso) é **trabalho futuro** e não entra no caminho crítico.

> **Mudança de premissa (substitui a versão anterior).** O orientador definiu que **o foco do TCC é a portabilidade**, com a metacamada adiada. Isso inverte a lógica do roadmap anterior, que tratava o porte como infraestrutura e recomendava blackboxar o Java em Docker. Agora o **porte fiel e completo da inferência é a espinha do trabalho** — o Docker é rebaixado a andaime de desenvolvimento, e a metacamada sai para "trabalhos futuros".

---

## 1. Objetivo e bordas de escopo

**Objetivo:** reimplementar em Python, de forma **fiel e completa**, o pipeline de **extração + inferência + serialização** do U-Schema para os paradigmas **documento (MongoDB)** e **grafo (Neo4j)**.

Duas bordas, confirmadas com o orientador:

- **Bancos:** apenas **MongoDB + Neo4j**. Os outros quatro backends do repositório (`cassandra`, `hbase`, `redis`, `sql`) estão fora — não pertencem à pergunta de pesquisa (documento + grafo a partir do relacional).
- **Camadas:** o porte cobre o **pipeline que produz o modelo U-Schema/XMI**, não o ferramental Eclipse em volta dele. Ver §3 para o que isso inclui e exclui.

**Definição operacional de "fiel":** equivalência de **comportamento/estrutura**, não XMI idêntico byte a byte. O serializador do EMF tem convenções próprias de `xmi:id` e ordenação que o PyEcore não reproduz à risca; o modelo é o mesmo, mas os bytes diferem. Logo, o critério de aceite é **equivalência estrutural** (mesmas entidades, variações, atributos, agregados, referências e contagens), verificada contra o XMI gerado pela ferramenta Java patcheada (o **oráculo**).

---

## 2. Arquitetura-alvo

```
[MongoDB] ─ driver nativo ─ triplas ────────→ doc2uschema ──┐
                            {schema, count}   (núcleo 1.2)  ├─→ PyEcore ─→ XMI
[Neo4j]  ─ driver nativo ─ arquétipos ──→ neo4j_model ──────┘
                                          (núcleo próprio)

   o map-reduce da extração roda em Python (padrão) ou em Spark (Fase 4, opcional);
   a leitura é sempre por driver nativo — Spark nunca é camada de tipagem
```

**Dois núcleos de construção, não um.** O plano original desta seção era um núcleo
único: os dois extratores produziriam a tripla e alimentariam o `doc2uschema`
(`SchemaInference` + `USchemaModelBuilder`). A **Fase 2.2 leu o fonte e derrubou
a premissa** — `Neo4j2USchema.process` chama `Json2USchemaModel.processArchetypes`,
que usa quatro classes próprias do pacote `neo4j2uschema`. O grafo não gera
tripla e não passa pelo `BuildUSchema`.

O porte replica isso: `extractors/neo4j_model.py` é o segundo núcleo. **Os dois
paradigmas convergem no metamodelo PyEcore, não no código de inferência** —
unificá-los "por limpeza" quebraria a equivalência com o oráculo, que é o
critério de aceite.

O que sobrevive do plano: no documento o alvo é mesmo o `doc2uschema`, o caminho
canônico e rico, e não o `ModelDirector` do `mongodb2uschema.spark`, que não foi
portado.

---

## 3. O que entra, o que fica de fora

**Entra (precisa ser portado para "fiel e completo"):**
- Metamodelo via **PyEcore** carregando `uschema.ecore` (19 classes, puramente estrutural — substitui Factory/Package/Switch/AdapterFactory gerados pelo EMF).
- Modelos intermediários: `raw` (Composite: `SchemaComponent` e filhos) e `firsto` (`MultiValued`, `Ranged`, …).
- Núcleo `doc2uschema/process`: `SchemaInference`, `USchemaModelBuilder` e **todas** as estratégias (`AliasedAggregatedEntityJoiner`, `EVariationMerger`, `OptionalTagger`, `FeatureAnalyzer`, `ReferenceMatcher` + `Creator`, `StructuralVariationSorter`).
- Extratores `mongodb2uschema` e `neo4j2uschema` por **driver nativo** (ver a correção de premissa abaixo).

**Fica de fora — e por quê (status corrigido):**
- **OCL** — **ausente neste metamodelo**. O `uschema.ecore` não tem nenhuma constraint OCL (nem EAnnotations). Não-questão. (Se houvesse, seriam invariantes reescritas como checagens Python — não exige motor OCL.)
- **Codegen EMF** — existe `pyecoregen`, e o PyEcore reflexivo dispensa codegen. Não é barreira.
- **Sirius / editor visual** (`es.um.uschema.design`) — único item sem equivalente pronto em Python. **Está fora do escopo do porte fiel** (é UI) e, se um dia for desejado, é reconstruível com outra stack (web: React Flow / Cytoscape) — trabalho futuro, não impossibilidade.

> **Não há impossibilidade no objetivo.** Toda "limitação" é da forma "sem drop-in equivalente ao do Eclipse → reimplementar o comportamento ou usar outra stack". É esforço e tempo, não barreira técnica.

---

## 4. Fases

### Fase 0 — Fundação + oráculo · concluída
1. PyEcore carrega `uschema.ecore`; instanciar `EntityType`, `StructuralVariation`, `Aggregate`, `Attribute`, `Reference`.
2. Round-trip XMI: ler `model_northwind.xmi`, reserializar, validar.
3. **Harness de equivalência estrutural** (não textual): compara conjuntos de entidades, variações e contagens entre dois XMIs. É o critério de aceite de todas as fases seguintes.
4. **Docker como andaime:** a ferramenta Java patcheada, isolada em imagem (JDK 8 + Spark + patches #6/#7), serve só para **gerar os XMIs de referência** de forma reproduzível. Não é a entrega — é o gerador do oráculo.

### Fase 1 — Núcleo de inferência (`doc2uschema`) · a espinha · concluída
> O golden-master do **Northwind**, único item que atravessou a fase, fechou na
> **2.3** (`equivalent=True`, só não-fatais do #8 — a quantidade varia com a
> ordem de leitura; ver `bugs_originais.md` §#8).
- Modelos intermediários `raw`/`firsto` → `dataclasses` (Composite vira árvore recursiva).
- `SchemaInference.infer`: recursão JSON → `SchemaComponent`; **igualdade estrutural** e **ordenação de campos** replicadas fielmente (são o que torna o porte verificável); objetos aninhados viram entidades internas.
- Estratégias: `joiner` (une aliases), `merger` (funde variações equivalentes), `optionalTagger`/`featureAnalyzer` (opcionalidade entre variações), `referenceMatcher` (+`creator`; detecção de `Reference` por id, só entidades com variação raiz), `varSorter` (ordem determinística).
- `USchemaModelBuilder.build` + `fillEV`: `Attribute` / `Aggregate` / `Reference`; depois `sort` + `setOptionalProperties`.
- Guice **desaparece** (wiring por construtor, que já existe); `abstractjson` **desaparece** (`dict`/`bson` nativo).
- **Gate:** cada módulo reproduz o XMI-oráculo estruturalmente.

### Fase 2 — Extratores (MongoDB + Neo4j) · concluída
> **Correção de premissa:** conectores Spark oficiais viraram DataFrame-only;
> lê-se direto via `pymongo`/`neo4j` (drivers nativos), Spark opcional e futuro.
- Portar a função de assinatura (`Helpers`/`IdArchetypeMapping`) como funções Python puras — `extractors/mongo.py`, `extractors/neo4j.py` (+ núcleo de construção próprio do Neo4j, `extractors/neo4j_model.py`).
- **Gate atingido:** contagens == Java; XMI ≡ oráculo nos dois paradigmas (Northwind e os 4 XMIs Neo4j), extração Neo4j também confirmada contra banco real.

### Fase 3 — Ponta a ponta + volume · concluída
- Equivalência: Northwind (Sakila descartado em 02/08/2026).
- Volume: User Profiles (quatro tamanhos), reproduzindo a tendência da Tabela 4 do artigo.
- Bugs **#6/#7 corrigidos por construção** (tratar `_id` inteiro e array vazio desde o início, em vez de patch) — material direto para o capítulo de reprodutibilidade.

### Fase 4 — Spark como paralelizador da extração
> Guia detalhado: `fase4_spark.md`. Fecha o que a **2.0** deixou em aberto
> ("Spark, se entrar, é só paralelizador futuro") e responde a uma pergunta que
> a **3.2** levantou sem separar.

A 3.2 mediu que **a razão porte/oráculo cresce com o tamanho** (grafo: 26,3× de
crescimento no porte contra 5,25× do oráculo para 8× de dado), o que já
descarta constante de linguagem como explicação. Restaram duas causas
candidatas, e a Fase 3 não as separou: **algorítmica** (o número de esquemas
distintos cresce 13 → 421 no documento) e **arquitetural** (o oráculo distribui
o map-reduce em executores; o porte roda single-thread). A Fase 4 separa as
duas rodando o **mesmo** código de assinatura sob um paralelizador.

- **Backend Spark opcional** (`mapPartitions` + `reduceByKey`), com o Python como
  padrão. Começa pelo **MongoDB**; o **Neo4j fica condicional** ao Mongo mostrar
  ganho — sem ele, o grafo não se justifica (leitura client-bound e nove
  arquétipos em todos os tamanhos). A `reduce_pairs` já é comutativa e
  associativa — provado na 2.0 —, então a redução entra sem reescrever a lógica.
- **A leitura continua por driver nativo.** O Spark paraleliza o map-reduce,
  nunca a tipagem: passar pelo DataFrame destruiria `ObjectId`/`Int64` e mudaria
  a assinatura. A partição é por coleção / combinação de labels, subparticionada
  por faixa de `_id` (sem isso, o User Profiles dá duas partições úteis).
- **Determinismo sob partição é o ponto delicado:** o `count` é invariante à
  ordem, mas o **#8 não é** — mudar de backend move a subcontagem de lugar sem
  quebrar a equivalência, como já acontece entre ler o Northwind por arquivo e
  por cursor.
- **Nenhum ganho também fecha o gate**: joga a diferença de curva inteira para o
  lado algorítmico, e é a resposta mais forte das duas.

---

## 5. Riscos e pontos de atenção

O risco é **tempo**, não impossibilidade. Com a metacamada fora do caminho crítico, a estimativa de 4–6 pessoa-meses registrada na avaliação inicial (jun/2026) deixa de ser "não cabe" e passa a ser **plausível como o próprio TCC**, com dois autores em 6 meses.

- **Paralelização:** o núcleo de inferência (Fase 1) e os extratores + um paradigma (Fase 2) avançam **em paralelo**, encontrando-se pelo formato da tripla. O trabalho é **compartilhado** entre os dois autores, sem dono fixo por fase.
- **Watch-items de fidelidade** (reproduzíveis, exigem cuidado): o **Inflector** (capitalização/pluralização dos nomes de entidade tem de casar — há libs de inflection em Python, mas talvez seja preciso reproduzir as regras exatas); o **determinismo** (ordenação de campos, `equals` estrutural, ordem das variações); e a natureza **estrutural-não-byte** da comparação de XMI.

---

## 6. Resumo das fases

| Fase | Entrega | Gate de aceite | Status |
|---|---|---|---|
| 0 | PyEcore + round-trip + harness de equivalência + oráculo Java em Docker | round-trip do Northwind fecha | concluída |
| 1 | núcleo `doc2uschema` em Python (inferência completa) | cada módulo ≡ XMI-oráculo (estrutural) | concluída |
| 2 | extratores MongoDB + Neo4j (driver nativo, não PySpark) | contagens == Java; XMI ≡ oráculo | concluída |
| 3 | ponta a ponta, equivalência + volume, bugs corrigidos | Northwind ok; tendência Tabela 4 reproduzida | concluída |
| 4 | backend Spark opcional (Mongo; Neo4j condicional) + bateria comparativa | triplas idênticas entre backends; `equivalent=True`; curva medida nos dois (inclusive "sem ganho") | em aberto |

**Sequência:** 0 → 1 → 2 → 3 → 4. Metacamada: trabalho futuro. Sirius/UI: fora de escopo (reconstruível em outra stack se desejado).

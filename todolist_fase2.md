# TO-DO — Fase 2: Extratores MongoDB + Neo4j (driver nativo; Spark opcional como paralelizador)

**Projeto:** Porte fiel e completo do U-Schema (Java/Spark/EMF) → Python — MongoDB e Neo4j
**Autores:** Davi Cavalcante · João — CESUPA
**Base:** `fase2_extratores_pyspark.md` · **Contrato de costura:** `extractors/triple.py` (Fase 1.0) · **Bugs:** `bugs_originais.md` ·
**Pré-requisito:** Fase 1 (núcleo de inferência completo; golden-master do mintest com 0 divergências)

> **Organização por entrega.** Tarefas agrupadas por **entregável** (2.0–2.3), não por
> autor — trabalho compartilhado, sem dono fixo. Cada bloco define uma **Saída**
> que serve de critério de "pronto".
>
> **Ideia central.** *Bottom-up*, *test-alongside*, e — a diferença desta fase —
> **os extratores só produzem triplas.** Não têm inferência nem construção de
> modelo próprios: entregam `{schema, count, firstTimestamp, lastTimestamp}` ao
> **núcleo único** da Fase 1 (`BuildUSchema`). Isso é mais limpo que o original,
> onde cada extrator `.spark` trazia um `ModelDirector` (ver o achado abaixo).
>
> **Abrir o `.java` antes de afirmar.** As fontes estão nos commits pinados
> (`6dfd6b4a`/`0f8f58c3`, `oracle/Dockerfile`). O falso C8 e o mapeamento errado
> do `m2m` (1.4b) nasceram de diagnosticar pelo nome, sem abrir o fonte.

---

## Achado que corrige a spec: há DOIS extratores MongoDB, e o certo não é o que a spec cita

`fase2_extratores_pyspark.md` §2.1 aponta `ArchetypeMapping`/`JSONMapping`/
`ModelDirector` (pacote `mongodb2uschema.spark`) como referência. **Verificado no
fonte que esse é o extrator errado para a nossa arquitetura.** Existem dois:

| Extrator | Produz | Alimenta | Gera os XMIs de referência? |
|---|---|---|---|
| `mongodb2uschema` (**Helpers**) | tripla `{schema, count, ts}` | `SchemaInference`+`USchemaModelBuilder` (nosso núcleo) | **SIM** — mintest/northwind |
| `mongodb2uschema.spark` (ArchetypeMapping) | `{entity, properties}` | `ModelDirector` (construtor próprio, **não** o nosso núcleo) | não (tamanho/`up_b_larger`) |

**Consequência:** a 2.1 porta o caminho **`Helpers`**, não o `ArchetypeMapping` — portar o outro duplicaria a inferência que a Fase 1 já tem.

---

## Decisão de arquitetura: driver nativo, não conector oficial do Spark

Nem o MongoDB Spark Connector nem o Neo4j Connector for Apache Spark atuais expõem
mais a API RDD antiga que o oráculo usa (`MongoSpark.load(jsc)`/`neo4j.cypher(...).loadRowRdd()`)
— viraram DataFrame-only.

**Decisão:** ler direto via `pymongo`/`neo4j` (drivers nativos, já no `uv.lock`). Spark, se entrar, é só paralelizador futuro — nunca camada de tipagem.

**Duas armadilhas de fidelidade:**
- `ObjectId`/`int64` viram objetos agregados (`{"$oid": ...}`/`{"$numberLong": ...}`) direto no `dict`, nunca via `bson.json_util.dumps()`.
- Em Python `bson.Int64`/`bool` são subclasses de `int` (no Java não): despacho obrigatório `Int64` → `bool` → `int`.

---

## Cadeia de desbloqueio

```text
Fase 1 (BuildUSchema) ─┐
extractors/triple.py ─────┼─→ 2.0 (infra de leitura: pymongo/neo4j) ─→ 2.1 (Mongo/Helpers) ─→ 2.3 (golden-master Northwind, herdado da 1.7)
                          └─→ 2.2 (Neo4j) ────────────────────────────────────────────┘
```

| Etapa | Depende de | Libera | Verificado |
|---|---|---|---|
| **2.0** infra de leitura | Fase 1 | 2.1, 2.2 | `pymongo`/`neo4j` já são dep; `pyspark` fica só se decidirmos paralelizar |
| **2.1** Mongo | 2.0, tripla | 2.3 | `MongoDB2USchema.java:60-82` (Helpers) |
| **2.2** Neo4j | 2.0, tripla | grafo | `SparkProcess`/`IdArchetypeMapping` |
| **2.3** golden-master Northwind | 2.1 | gate integração da Fase 1 **e** 2 | `model_northwind.xmi`; as 8 não-fatais do #8 |

---

## 2.0 — Infra de leitura (drivers nativos)

> Substitui a infra de conector Spark que a versão anterior deste documento
> previa — ver a decisão de arquitetura acima.

- [x] Cliente `pymongo.MongoClient` de teste — rodado contra MongoDB Atlas real (`scripts/check_extraction_mongo.py`), fumaça, caso `Int64` e Northwind (17 coleções, mesmo resultado da 2.3) confirmados corretos.
- [x] Cliente `neo4j.GraphDatabase.driver` de teste — exercitado contra Neo4j Aura Free (`scripts/check_extraction_neo4j.py`).
- [x] **Decidido: Python agora, Spark opcional depois, sem retrabalho.** `reduce_pairs`/`build_triples` combina por `(min, max, soma)` — comutativo e associativo, então particionar não muda o resultado (provado empiricamente). `mapPartitions` cabe depois sem reescrever nada.
- [ ] Se Spark **entrar** como paralelizador, marcar os testes com `@pytest.mark.spark` (pre-push/CI, não pre-commit).
- [x] Conferir que `pymongo`/`neo4j` já resolvidos no `uv.lock` — estão (`4.17.0`/`6.2.0`).

**Saída:** clientes de teste reproduzíveis para Mongo e Neo4j via driver nativo. Papel do Spark: Python fechado para a 2.1/2.2; paralelização é extensão futura opcional.

---

## 2.1 — Extrator MongoDB (caminho `Helpers`, o que alimenta o núcleo)

> Referência **corrigida**: `mongodb2uschema/MongoDB2USchema.java` + `utils/Helpers.java`
> (**não** o `.spark`/`ArchetypeMapping`). Pipeline (`:73-83`):
> `mapToPair(generateDocumentPair) → reduceByKey(reducePairs) → put(_type, coll) → documentPairToJSONNode`.

- [x] Portar **`Helpers.simplify`** (`Helpers.java:18-57`) — `src/uschema/extractors/mongo.py`. `ObjectId`→`{"$oid": ...}`, `Int64`→`{"$numberLong": ...}`, despacho `Int64`→`bool`→`int`, tipo não suportado replica o `raise` do Java.
- [x] Portar `generateDocumentPair`/`reducePairs` com **#6 corrigido por construção** (timestamp só se `_id` for `ObjectId`).
- [x] `_type` = nome cru da coleção, anexado depois da agregação — `build_triples` em `mongo.py`.
- [x] **Bug #6 por construção**: `_id` lido genericamente (timestamp só se `ObjectId`) — pré-requisito do Northwind, cujo `_id` é inteiro.
- [x] **Bug #7 por construção**: array vazio não indexa elemento inexistente — confirmado contra dado real do Northwind (`orders.details: []`).
- [x] Conectar via `pymongo.MongoClient` — `extract_database_triples`/`extract_triples` em `mongo.py`, em Python.
- [x] Testes de `extract_database_triples` (puros, sem banco) + validação manual com `mongomock`.
- [x] Testes das funções de assinatura contra as fixtures-oráculo — reforçado pela 2.3 (Northwind real). `extract_triples` (abre `MongoClient` real) segue sem execução real até o script da 2.0 rodar.

**Gate 2.1:** a contagem de assinaturas por coleção é **idêntica** à do Java; a tripla, alimentada no núcleo da Fase 1, reproduz o `model_mintest.xmi` (0 divergências).

---

## 2.2 — Extrator Neo4j (paradigma grafo)

> **Achado que corrige a spec:** o Neo4j **não** alimenta o núcleo da Fase 1.
> `Neo4j2USchema.process` chama `Json2USchemaModel.processArchetypes`, que usa
> quatro classes próprias do `neo4j2uschema` — um segundo núcleo de construção
> de `USchema`. **Decisão: portar fiel**, núcleo próprio, sem encaixar no
> `BuildUSchema` da Fase 1. Invalida "Costura com a Fase 1" abaixo só pro Neo4j.

### Camada de extração (`SparkProcess.java:50-121`)

Duas *cypher*: `MATCH (n) RETURN DISTINCT labels(n)` lista combinações de labels; por combinação, `OPTIONAL MATCH (n)-[r]->(m)` traz nó+relacionamento. `IdArchetypeMapping`/`ReduceByIdArchetype`/`SplitMapping` montam e deduplicam o arquétipo por nó; `.countByValue()` conta variações de `EntityType` e, por construção do `Set`, fontes distintas de `RelationshipType` (não arestas brutas). `TypeUtils.obtainType` dá a sentinela de tipo por valor.

### Camada de construção do modelo (não é a Fase 1)

`Json2USchemaModel.processArchetypes` despacha `"node"`/`"relationship"` pro `StructuralVariationBuilder`; depois `AttributeOptionalsChecker.processOptionals()` e `IgnoreSimilarReferenceBoundsProcessor.joinVariationsIgnoringBounds()`. Pontos de atenção: `EntityType` sempre nasce `root=true`; nó multi-label ganha entidades-pai por label; variação de `RelationshipType` é compartilhada entre `EntityType`s quando as propriedades batem (sem incluir `refsTo` na chave); `IgnoreSimilarReferenceBoundsProcessor` iguala bounds ao **mínimo**, não sempre ilimitado, apesar do nome. `UModelRepository.addReferenceToCount`/`getReferenceCount` são código morto — não portados.

**Tarefas:**
- [x] Portar a camada de extração como funções puras — `node_archetype`/`reduce_archetypes_by_node`/`build_archetype_counts`/`extract_archetype_counts` em `extractors/neo4j.py`, 37 testes.
- [x] Portar `TypeUtils.obtainType`/`get_type_name`, incluindo os dois achados verificados contra o `org.json` real (sentinela de `Long` sempre vira `"integer"`; lista heterogênea/vazia sempre vira `"string[]"`).
- [x] Conectar via `neo4j.GraphDatabase.driver(...).execute_query(...)` — em Python, testado com driver falso.
- [x] **Achado catalogado:** assimetria "labels próprios ordenados vs. `refsTo` não ordenado" gera dois `EntityType` pro mesmo nó multi-label. **Confirmado com dado real** (Neo4j Aura) — `N1` em `bugs_originais.md`.
- [x] Portar `USchemaBuilder`+`StructuralVariationBuilder` — `extractors/neo4j_model.py`, núcleo próprio, sem tocar a Fase 1. `addReferenceToCount`/`getReferenceCount` não portados (código morto).
- [x] Portar `AttributeOptionalsChecker`/`IgnoreSimilarReferenceBoundsProcessor` como pós-processamento. Achado: as duas classes que "stringificam tipo" no oráculo **não são a mesma função** — uma não reconhece `PList`; portado fielmente como duas funções separadas.
- [x] Testes — `tests/unit/test_extractors_neo4j_model.py` (17 testes sintéticos).
- [x] **Comparação contra o oráculo real** — `tests/datasets/test_movies_min_golden_master.py`: os 4 XMIs Neo4j são o mesmo dataset "User Profile" em 4 tamanhos; arquétipos reconstruídos da estrutura do próprio `movies_min.xmi`. `compare()` devolve `equivalent=True` e zero divergências nos 4.
- [x] **Proveniência dos XMIs Neo4j:** não há Neo4j real acessível neste ambiente nem o dataset original versionado no repo Java — daí a reconstrução acima.

**Gate 2.2:** contagens idênticas ao Java; XMI ≡ oráculo (4 datasets, zero divergências); extração rodada contra Neo4j real (Aura), confirmando `N1`. **Fase 2.2 fechada.** `gen_userprofiles_neo4j.py` (gerador real do dataset) foi localizado no clone Java e trazido pra `scripts/`.

---

## 2.3 — Golden-master do Northwind (herdado da Fase 1.7)

> **Adiado da 1.7 por decisão**: o Northwind exige a tripla **real** do extrator,
> não reconstruível à mão. Desbloqueia quando a 2.1 existir.

- [x] Rodar a 2.1 sobre o Northwind — dados reais (17 arquivos), pipeline completo → `compare()` contra `model_northwind.xmi`. **Resultado: `equivalent=True`, só divergências não-fatais**, todas em `Orders`/`Purchase_orders`/`Products`/`Detail` — assinatura esperada do #8.
  - [x] **Corrigido em 31/07/2026: o número de divergências é ordem-dependente, não invariante.** A leitura por arquivo dá **15**; a leitura por `MongoClient` local dá **12**, com os mesmos 397 documentos e as mesmas 49 linhas de tripla — muda só a ordem do cursor (`orders` devolve `_id` `30,31,32,33…` no arquivo e `33,37,32,30…` no banco). A frase anterior ("reconfirmado via Atlas, **mesmo resultado exato**") descrevia uma coincidência de ordem de inserção, não uma propriedade. Evidência e tabela em `bugs_originais.md` §#8.
  - [x] **Invariante real** (é o que pode ser citado): `equivalent=True`, todas não-fatais, confinadas às mesmas 4 entidades, e **14/17** coleções fechando a contagem — sempre as três com campo array de tamanho variável falhando (`orders`, `products`, `purchase_orders`).
- [x] O `#8` é replicado nos dois lados (Java e porte, por decisão registrada em `bugs_originais.md`); as não-fatais vêm da ordem de processamento decidir qual variação sobrevive ao colapso, não de o porte não ter o bug.
- [x] Mistério do `_id`/`$oid` (achado da 1.7): **não é bug.** 15 das 17 coleções usam `_id` inteiro; `sales_reports`/`strings` usam ObjectId real (5+62=67, bate com o `count="67"` do XMI).

**Saída:** golden-master do Northwind fechando estruturalmente contra o oráculo (`equivalent=True`). **Decisão do usuário: não versionar** os JSONs nem criar teste permanente — fica documentado aqui.

---

## Costura com a Fase 1

> **Vale só para o Mongo.** O Neo4j tem seu próprio núcleo de construção
> (`extractors/neo4j_model.py`), que não produz `SchemaTriple` nem passa pelo
> `BuildUSchema` — os dois paradigmas nunca compartilharam núcleo no Java
> também.

O Mongo produz o formato de tripla (`extractors/triple.py`) e entrega ao `BuildUSchema`. O Neo4j monta o `USchema` direto, com builder próprio — os dois convergem no mesmo metamodelo `pyecore`, não no mesmo código de inferência.

## Gate de aceite da Fase 2 — atingido

Para os dois paradigmas: contagem de assinaturas idêntica ao Java **e** XMI final estruturalmente equivalente ao oráculo, com toda divergência fatal explicada por um bug catalogado. Os dois `compare()` reais fecharam — Northwind e os 4 XMIs Neo4j — e a extração Neo4j também rodou contra banco real, confirmando `N1`.

## Entregáveis

`extractors/mongo.py`, `extractors/neo4j.py` (extração) e `extractors/neo4j_model.py` (construção própria do Neo4j, não `inference/`), testes de assinatura/extração/construção, o golden-master do Northwind (2.3), e os scripts de verificação manual (`scripts/check_extraction_neo4j.py`, `scripts/check_extraction_mongo.py`).

## Riscos da fase

- **`count` de `RelationshipType`** conta fontes distintas, não arestas brutas.
- **`_id` genérico (#6) e array vazio (#7)** tratados na origem da assinatura.
- **Não portar o `ArchetypeMapping`/`ModelDirector`** por engano — duplicaria o núcleo da Fase 1.
- **Ordem de despacho `Int64`/`bool`/`int`** errada faz `long`/`bool` virarem `0` silenciosamente.
- **`$numberLong` sem fixture** de golden-master.
- **Spark no pre-commit** (se entrar): marcar com `@pytest.mark.spark`.
- **Neo4j: reusar `BuildUSchema`/`SchemaTriple` da Fase 1 por engano** — o oráculo usa núcleo próprio.
- **`getOrCreateVariation` de `RelationshipType`** chaveia só por tipo+propriedades, sem `refsTo`.
- **`IgnoreSimilarReferenceBoundsProcessor` iguala ao mínimo**, não "ignora" — ler o código, não o nome.
- **`UModelRepository.addReferenceToCount`/`getReferenceCount`** parecem código morto — confirmar antes de portar.

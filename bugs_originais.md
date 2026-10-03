# Defeitos herdados do U-Schema original

Catálogo dos defeitos encontrados na implementação Java de referência
([`modelum/uschema-inference`](https://github.com/modelum/uschema-inference) e
[`modelum/uschema`](https://github.com/modelum/uschema)) durante o porte para
Python.

**Por que existe.** O porte é *fiel*: reproduz o comportamento do oráculo, não o
comportamento ideal. Quando o oráculo erra, o porte precisa decidir — replicar,
corrigir, ou corrigir e documentar. Este arquivo registra cada decisão com
evidência, para que o capítulo de reprodutibilidade do TCC possa citá-la e para
que, **depois de concluído o porte**, seja possível propor correções upstream.

**Como ler.** Duas famílias, com numeração independente:

- **`#1`–`#8`** — a série de *patches do oráculo* (`oracle/patches/`), já
  estabelecida no projeto. `#1`–`#5` são compatibilidade de build; `#6`–`#8` são
  defeitos semânticos.
- **`C1`–`C7`** — achados **novos**, na árvore de comparadores
  (`es.um.uschema.utils/.../custom/compare/`), levantados ao portar o harness de
  equivalência (Fase 0.3). Não existiam catalogados.
- **`I1`–`I3`** — achados **novos**, no `Inflector` (`.../util/inflector/`),
  levantados ao portar a normalização de nomes (Fase 0.6). O `Inflector` é código
  versionado do **ModeShape**, então os defeitos são *upstream do upstream*.
- **`M1`** — achado **novo**, em `ObjectMetadata` (`.../metadata/`), levantado ao
  portar o modelo intermediário (Fase 1.1).
- **`M2`** — achado **novo**, em `SchemaInference`/`AliasedAggregatedEntityJoiner`,
  levantado e **confirmado por teste** ao portar a inferência (Fase 1.2).
- **`M3`**/**`M4`** — achados **novos**, em `DefaultStructuralVariationSorter`
  (`.../process/util/`), levantados ao portar as estratégias EMF (Fase 1.3b).
- **`M5`** — achado **novo**, em `DefaultEVariationMerger` (`.../process/util/`),
  levantado ao portar o merger (Fase 1.3a) e **confirmado por execução real do
  Java** (não só leitura — ver a entrada).
- **`M6`** — achado **novo**, em `DefaultReferenceMatcher` (`.../process/util/`),
  levantado ao portar as estratégias EMF (Fase 1.3b) e **confirmado por
  execução real do Java**.
- **`N1`** — achado **novo**, em `IdArchetypeMapping`, levantado na Fase 2.2 e
  **confirmado com dado real** (Neo4j Aura).
- **`E1`** — achado **novo** e de outra natureza: é o único defeito **do porte**
  neste catálogo, e o único já **corrigido** (08/08/2026). Levantado na Fase 3.2
  como suposto comportamento do servidor Neo4j; a investigação de 08/08 mostrou
  que a causa é a leitura *eager* do nosso extrator. O prefixo `E` era de
  ambiente e ficou por inércia — mantido para não quebrar as citações.

Todas as citações de linha referem-se ao `HEAD` do upstream, salvo indicação em
contrário.

---

## Sumário

| Id | Sítio | Natureza | Severidade | Estado no porte |
|---|---|---|---|---|
| #1 | `MongoDB2USchemaMain` | binding Guice ausente | build | patch no oráculo |
| #2 | `pom.xml` (doc2uschema) | versão de Jackson | build | patch no oráculo |
| #3 | runtime | JDK 8 (bytecode do Spark) | build | patch no oráculo |
| #4 | `MongoDB2USchema` et al. | `Path.of` → `Paths.get` | build | patch no oráculo |
| #5 | `Neo4j2USchemaMain` | hardcode/caminho Hadoop | build | patch no oráculo |
| **#6** | `Helpers.java:66` | `_id` assumido `ObjectId` | **crash** | corrigido por construção |
| **#7** | `USchemaModelBuilder.java:255` | array vazio indexado | **crash** | corrigido por construção |
| **#8** | `SchemaInference.java:207-211` | `meta` inteiro (count+timestamps) descartado no colapso de variações | **equivalência confirmada (Northwind + tamanho 800k); resultado ordem-dependente** | replicado (fiel) |
| **C1** | `CompareReference.java:38-41` | só compara `isFeaturedBy[0]` | equivalência | replicado (fiel) |
| **C2** | `CompareReference.java:27` | recursão de `opposite` sem guarda | crash latente | replicado (fiel) |
| **C3** | `CompareSchemaType.java:95-96` | `compareNames` sem guarda de nulo | crash latente | replicado (fiel) |
| **C4** | `CompareKey`, `CompareStructuralVariation`, `CompareAggregate` | casamento guloso sobre relação não-transitiva | equivalência | replicado (fiel) |
| C5 | `CompareSchemaType.java:98` | termo booleano morto | code smell | simplificado |
| C6 | `CompareReference.java:45` | guarda assimétrico de nulo | code smell | não portado |
| **C7** | `USchemaCompareMain.java:120` | casamento de variações não-injetivo | **falso positivo** | replicado + reporte |
| I1 | `Inflector.java:470-473` | guarda ordinal testa o número, não o resto | equivalência | replicado (fiel) |
| I2 | `Inflector.java:454-459` | `titleCase` sem guarda de nulo (NPE) | crash latente | não replicado (devolve `None`) |
| I3 | `Inflector.java:445` | javadoc do `titleCase` promete o que o código não faz | documentação | replicado (fiel ao **código**) |
| **M1** | `ObjectMetadata.java:55` | sentinela `0` só reconhecida de um lado | **equivalência** | replicado (fiel) |
| **M2** | `SchemaInference.java:100-104` | `innerCountAndTimestampsAdjust` estoura quando o Joiner funde uma entidade interna | **crash confirmado** | replicado (fiel) |
| **M3** | `DefaultStructuralVariationSorter.java:40` | `sortByCount` não ordena (`ECollections.sort` comentado) | **equivalência** | replicado (fiel) |
| **M4** | `DefaultStructuralVariationSorter.java:28,34,46` | comparadores devolvem só `-1`/`1`, nunca `0` — não são ordem total | **equivalência** | replicado (fiel) |
| **M5** | `DefaultEVariationMerger.java:132` | `homogeneousArraysMerge` indexa array vazio quando os dois lados colapsam vazios | **crash confirmado (Java e porte)** | replicado (fiel) |
| **M6** | `DefaultReferenceMatcher.java:34-50` | chave concatenada crua no regex, sem escape — metacaractere vira regex | **equivalência confirmada (Java e porte)** | replicado (fiel) |
| **N1** | `IdArchetypeMapping.java:60-62,100-104` | labels próprios ordenados, `refsTo` não — dois `EntityType` pro mesmo nó multi-label | **equivalência confirmada (dado real)** | replicado (fiel) |
| **E1** | `extractors/neo4j.py` — `_read_label_combination` (**nosso**, não do original) | leitura *eager* enche o buffer, fecha a janela TCP e derruba a vazão a ~47 KB/s em resultado grande | **medido (6,3× mais lento que streaming)** | **corrigido** em 08/08/2026 |

`C7` está numa família própria: os demais fazem o harness **reprovar** algo
válido ou explodir. `C7` faz o harness **aprovar** um modelo errado — o único
modo de falha que o instrumento de validação não pode ter.

Severidade: **crash** = exceção em dado real · **crash latente** = exceção
possível, não exercitada pelos dados do oráculo · **equivalência** = resultado
silenciosamente errado · **code smell** = sem efeito observável.

---

## #6 — `_id` assumido como `ObjectId`

**Sítio:** `es.um.uschema.mongodb2uschema/.../utils/Helpers.java:66`, em
`generateDocumentPair`.

```java
long timestamp = doc.getObjectId("_id").getTimestamp();
```

**Sintoma.** `getObjectId` lança `ClassCastException` quando `_id` não é um
`ObjectId`. Coleções importadas de origem relacional — como o Northwind, cujos
`_id` são inteiros — derrubam a extração inteira.

**Correção (patch no oráculo, por construção no porte).** Ler `_id`
genericamente e extrair timestamp só quando o tipo permitir:

```java
Object id = doc.get("_id");
long timestamp = (id instanceof ObjectId) ? ((ObjectId) id).getTimestamp() : 0L;
```

**Consequência semântica.** Documentos sem `ObjectId` passam a ter
`firstTimestamp = lastTimestamp = 0`. Isso é *aceitável* porque o timestamp não
participa da equivalência estrutural (nem `CompareStructuralVariation` nem o
`USchemaCompareMain` o comparam), mas **deve ser dito**: para um dataset de
origem relacional, os timestamps do modelo U-Schema são vazios por construção,
não por ausência de dados.

---

## #7 — array vazio indexado antes do teste de tamanho

**Sítio:** `es.um.uschema.doc2uschema/.../process/USchemaModelBuilder.java:255`.

```java
SchemaComponent inner = sc.getInners().get(0);
if (sc.size() == 0 || !(inner instanceof ObjectSC)) //TODO: Sospecho que no se entra nunca aqui. Ademas, si sc.size() == 0 entonces el inner de antes excepciona.
```

**Sintoma.** `IndexOutOfBoundsException` em qualquer documento com um array
vazio. Na Rota B do plano de tamanho, ~15% dos documentos têm `[]` em algum
campo.

**O autor sabia.** O comentário é dele, no código, e diz exatamente isto:
*"Suspeito que nunca se entra aqui. Além disso, se `sc.size() == 0` então o
`inner` de antes lança exceção."* O defeito foi diagnosticado e não corrigido.
É o achado mais citável do catálogo: não é uma sutileza que passou despercebida,
é dívida técnica declarada.

**Correção.** O `||` do Java já faz short-circuit; basta não materializar o
`inner` antes dele:

```java
if (sc.size() == 0 || !(sc.getInners().get(0) instanceof ObjectSC))
```

**No porte Python:** checar `len(inners) == 0` **antes** de tocar em
`inners[0]`. Ver `fase1_nucleo_inferencia.md`, seção 1.3.

---

## #8 — `meta` descartado ao colapsar variações

**Sítio:** `es.um.uschema.doc2uschema/.../process/SchemaInference.java:207-211`.

```java
if (entityVariations != null)
{
    Optional<SchemaComponent> foundSchema =
            entityVariations.stream().filter(schema::equals).findFirst();
    if (foundSchema.isPresent())
        retSchema = foundSchema.get();
    else
        entityVariations.add(schema);
}
```

**Sintoma.** Ao reconhecer uma variação estruturalmente igual a uma já
registrada, o código reaproveita a variação existente (`retSchema =
foundSchema.get()`) e descarta `schema` inteiro — sem combinar nenhum
metadado. O `ObjectMetadata` da ocorrência nova (`count`,
`firstTimestamp`, `lastTimestamp`) desaparece por completo, não só parte
dele. Como qualquer campo array aninhado também pende dessa árvore
descartada, o `upper_bounds` dele se perde pela mesma razão.

Exemplo: duas ocorrências da mesma entidade, campos idênticos,
`count`/`timestamps` diferentes — colapsam em 1 variação, e o `meta` final
é exatamente o da primeira ocorrência vista. `t1(count=3)` + `t2(count=2)`
→ `count` final = 3, não 5.

O gatilho de colapso é `ArraySC.equals` ignorando o tamanho do array
(`.../intermediate/raw/ArraySC.java:93-98`, comentário do próprio autor:
*"Another step is needed to reconcile zero size arrays with other
lengths"*) — sem essa igualdade frouxa, o colapso quase nunca ocorreria e
o #8 ficaria invisível.

**Decisão no porte: replicar.** `combine_metadata` não é chamado neste
ponto de colapso, de propósito — a igualdade frouxa do `ArraySC` é
load-bearing e deve ser preservada (sem ela o número de variações
explodiria e divergiria do oráculo), mas replicá-la **junto** com a
ausência de combinação é o que reproduz o #8 fielmente.

**Correção upstream (candidata, não aplicada).** Chamar `combineMetadata`
ao reaproveitar a variação, combinando `count`/`firstTimestamp`/
`lastTimestamp` da ocorrência nova na existente. Muda contagens e janelas
de tempo publicadas — requer dados de antes/depois antes de propor.

**Confirmado com dado real (golden-master do Northwind, Fase 2.3):** rodando o
pipeline real sobre os 17 arquivos do Northwind, `compare()` devolve
`equivalent=True` com 15 divergências não-fatais (pelo caminho de leitura por
arquivo — ver a ressalva abaixo), todas em entidades com campo array —
assinatura consistente com o #8 (a ocorrência que "sobrevive" ao colapso
depende da ordem de processamento, não da estrutura).

### Achado novo (31/07/2026): o resultado é **ordem-dependente**, não só subcontado

O #8 é mais severo do que "descarta contagem": ele faz o modelo publicado
depender da **ordem física** em que os documentos são lidos. Duas leituras do
**mesmo** Northwind — mesmos 397 documentos, mesmas **49 linhas de tripla** —
produzem modelos diferentes:

| Caminho de leitura | Divergências não-fatais | `Orders` | `Purchase_orders` | `Products` |
|---|---|---|---|---|
| Arquivos `.json` (ordem de linha) | **15** | 24 | 22 | 40 |
| `MongoClient` (ordem do cursor) | **12** | 38 | 23 | 40 |

Causa cravada: a coleção `orders` devolve `_id` na ordem `30, 31, 32, 33…` no
arquivo e `33, 37, 32, 30…` no cursor (banco carregado com inserção
não-ordenada) — mesmo **conjunto**, ordem diferente. Como o colapso reaproveita
a variação já registrada e descarta o `meta` da nova, **quem chega primeiro
define o `count` que sobrevive**.

**O que é estável** (e portanto o que pode ser citado como resultado):
`equivalent=True`; todas as divergências **não-fatais**; confinadas a
`Orders`/`Purchase_orders`/`Products`/`Detail`; e **14 de 17** coleções fechando
a contagem, sempre as mesmas três falhando — exatamente as que têm campo array
de tamanho variável.

**O que não é estável:** o número de divergências e as contagens sobreviventes.
Qualquer tabela de `count` do Northwind no texto precisa dizer **por qual
caminho** foi extraída.

### Confirmação em volume (User Profiles, 8 corridas, 31/07/2026)

Bateria completa sobre os oito bancos `up_{a,b}_{small,medium,large,larger}`
(dados em `results/`). `Movie` é o **controle**: mesma
corrida, mesmo pipeline, mas **sem array de tamanho variável**.

| Rota | Tamanho | `User` real | No modelo | Capturado | `Movie` |
|---|---|---|---|---|---|
| **A** (`_id` ObjectId) | small | 100.000 | 22.168 | 22,17% | 100% |
| | medium | 200.000 | 24.074 | 12,04% | 100% |
| | large | 400.000 | 21.789 | 5,45% | 100% |
| | larger | 800.000 | 21.013 | **2,63%** | 100% |
| **B** (`_id` inteiro, ~15% arrays vazios) | small | 100.000 | 31.280 | **31,28%** | 100% |
| | medium | 200.000 | 42.220 | 21,11% | 100% |
| | large | 400.000 | 58.317 | 14,58% | 100% |
| | larger | 800.000 | 91.468 | 11,43% | 100% |

Três leituras:

1. **A faixa do experimento original é reproduzida nas duas pontas.** O
   registrado era "~2,6%–31%"; medimos **2,63%** (A/larger) e **31,28%**
   (B/small). O intervalo inteiro, não uma aproximação.
2. **O que o #8 faz não é subcontar proporcionalmente — é travar num teto quase
   fixo.** Na Rota A a massa capturada é praticamente **constante** (22.168 →
   24.074 → 21.789 → 21.013) enquanto o volume real cresce **8×**. O percentual
   despenca só porque o denominador cresce e o numerador não. Essa é a
   caracterização mais precisa do bug que o projeto produziu — mais útil que
   "captura ~2,6%", que é um artefato do tamanho escolhido.
3. **`Movie` intacto em 100% nas oito corridas** isola o gatilho sem ambiguidade:
   a única diferença entre as duas entidades é `User` ter
   `watchedMovies`/`favoriteMovies` de tamanho variável. É o `ArraySC.equals`
   ignorando tamanho, e nada mais.

A **estrutura** sai correta em todas: `User` tem as 2 variações certas (o
gerador liga `postcode` e `surname`+`favoritos` no mesmo `i % 2`, então só
existem 2 perfis). No `up_a_larger`, 420 linhas de tripla colapsam nelas e
sobrevive só o `count` da primeira de cada grupo (954 e 20.059).

### O enunciado preciso: depende da **ordem de leitura**, não do sorteio do dado

Uma versão anterior desta seção advertia que os percentuais eram "uma amostra,
não constantes". **A bateria de 3 sementes (23, 69, 207) refutou isso** — os
mesmos experimentos, com dados sorteados independentemente, reproduzem os
percentuais dentro de **1,7% no pior caso**:

| Rota | Tamanho | seed 23 | seed 69 | seed 207 | Amplitude |
|---|---|---|---|---|---|
| A | small | 22,4% | 22,0% | 22,4% | 1,7% |
| A | larger | **2,6%** | **2,6%** | **2,6%** | 1,5% |
| B | small | **31,2%** | **31,0%** | **31,1%** | 0,5% |
| B | larger | 11,5% | 11,5% | 11,5% | 0,5% |

O número de linhas de tripla é **idêntico** entre as sementes (13/31/111/421 na
Rota A; 21/43/133/463 na B).

Os dois achados não se contradizem — eles isolam a variável:

- **Northwind:** *mesmo* dado, ordem de leitura *diferente* (arquivo vs. cursor) → resultado **diferente** (15 vs. 12 divergências).
- **User Profiles:** dado *diferente* (3 sementes), ordem de leitura *igual* (o gerador insere em `for i in range(n)` e o cursor devolve aproximadamente na ordem de inserção) → resultado **igual**.

Ou seja: **o que decide qual variação sobrevive ao colapso é a ordem em que as
ocorrências chegam, não quais valores elas têm.** Consequência prática para o
texto: os percentuais podem ser citados como propriedade estrutural; o que
precisa vir declarado junto é o **caminho de extração**, porque é ele que fixa
a ordem.

### Incerteza declarada, adjacente ao #8

`SchemaInference.java:94`, em `innerCountAndTimestampsAdjust`:

```java
// FIXME: I'm not sure this will work for n levels of aggregation
```

Não é um defeito confirmado — é um *known unknown* do autor sobre a propagação
de `count`/`timestamp` para entidades não-raiz com aninhamento profundo. Vale
construir um dataset com três níveis de agregação e comparar oráculo × porte
antes de afirmar qualquer coisa. **Não investigado.**

---

## C1 — `CompareReference` compara só o primeiro `isFeaturedBy`

**Sítio:** `es.um.uschema.utils/.../custom/compare/CompareReference.java:31-42`.

```java
if (r1.getIsFeaturedBy().isEmpty() ^ r2.getIsFeaturedBy().isEmpty())
  return false;

if (!r1.getIsFeaturedBy().isEmpty() && !r2.getIsFeaturedBy().isEmpty())
{
  if (r1.getIsFeaturedBy().size() != r2.getIsFeaturedBy().size())
    return false;

  if (r1.getIsFeaturedBy().get(0).getContainer() != null ^ r2.getIsFeaturedBy().get(0).getContainer() != null)
    return false;

  if (r1.getIsFeaturedBy().get(0).getContainer() != null && !CompareSchemaType.compareNames(r1.getIsFeaturedBy().get(0).getContainer(), r2.getIsFeaturedBy().get(0).getContainer()))
    return false;
}
```

**Sintoma.** Os tamanhos das listas são conferidos, mas apenas o elemento `[0]` é
comparado. Os elementos `1..n` são ignorados. Duas `Reference` cujos
`isFeaturedBy` são `[Order, Address]` e `[Order, Payment]` são declaradas
equivalentes.

Nos XMIs de referência do Neo4j, `isFeaturedBy` aparece 4 vezes por modelo — o
defeito é alcançável, embora não necessariamente exercitado com listas de
tamanho > 1.

**Correção proposta (Davi).** Trocar a comparação do `[0]` por um casamento de
multiset sobre a lista inteira, reusando o predicado que já existe:

```python
if not _match_bag(r1.isFeaturedBy, r2.isFeaturedBy, _same_container):
    return False
```

É order-insensitive (a ordem de `isFeaturedBy` não é semântica) e não desce nas
`features` — mantém a proteção contra ciclo que o `_same_container` dá. Torna o
check de tamanho redundante, como nos demais comparadores.

**Decisão no porte: replicar o `[0]`; a correção vai para o upstream.**

Vale ser preciso sobre o porquê, porque este caso é mais benigno que os outros.
Um harness *mais rígido* que o oráculo **não pode produzir falso positivo** — ele
nunca aprova um porte errado. Ele só reprova a mais, e só quando os
`isFeaturedBy[1..n]` de fato diferem, o que é uma diferença real entre os dois
modelos. Como **ferramenta de engenharia**, a versão com `_match_bag` é melhor.

O problema é outro: o harness é o **instrumento de medida** da tese. Se ele adota
uma noção de equivalência diferente da do oráculo, a afirmação "o porte é
equivalente ao original *no sentido em que o original define equivalência*" deixa
de ser verificável — vira "equivalente segundo um critério nosso", e o oráculo
deixa de ser oráculo. Fidelidade primeiro; correção depois, upstream.

*Se* quisermos visibilidade sem quebrar a premissa, o padrão já estabelecido
resolve: manter o veredito no `[0]` e emitir uma `Divergence(fatal=False)` quando
`1..n` divergirem — o mesmo "fiel + reporte extra" de `count`, `root` e das
variações órfãs.

Está travado por teste — ver `tests/unit/test_equivalence.py::test_compare_reference_only_compares_first_featured_by`,
que afirma `True` para `["Order", "Address"]` vs `["Order", "Payment"]`, e por
mutation testing (trocar o `[0]` por um casamento de multiset faz o teste falhar).

---

## C2 — recursão de `opposite` sem guarda de ciclo

**Sítio:** `CompareReference.java:27`.

```java
if (r1.getOpposite() != null && !new CompareFeature().compare(r1.getOpposite(), r2.getOpposite()))
  return false;
```

**Sintoma.** `Reference.opposite` é auto-referente (`Reference → Reference` no
`uschema.ecore`). Numa relação bidirecional bem-formada, `r.opposite.opposite ==
r`. A cadeia `CompareReference → CompareFeature → CompareLogicalFeature →
CompareReference` não tem conjunto de visitados: `StackOverflowError`.

Nenhum `Comparator` da árvore mantém estado entre chamadas — cada um instancia
`new CompareFeature()` a cada nível.

**Por que não explode.** Nenhum XMI de referência popula `opposite`:

```text
$ grep -c 'opposite=' resources/**/*.xmi
neo4j/*.xmi   : 0
mongodb/*.xmi : 0
```

O caminho existe no código e é inalcançável pelos dados que o oráculo produz.

**Decisão no porte: replicar, com o risco documentado** na docstring de
`compare_reference` (seção `Notes`). Inventar um guarda de visitados que o
oráculo não tem seria divergir por um caso que não ocorre — e mascararia o
defeito num eventual reporte upstream.

---

## C3 — `compareNames` desreferencia nome nulo

**Sítio:** `CompareSchemaType.java:94-100`.

```java
public static boolean compareNames(SchemaType e1, SchemaType e2) {
    String name1 = casec.apply(e1.getName());   // casec = n -> n.toLowerCase()
    String name2 = casec.apply(e2.getName());
    ...
}
```

**Sintoma.** `NullPointerException` se `getName()` for `null`.

O método é `static` e é chamado **diretamente** por `CompareAggregate`,
`CompareReference` e `USchemaCompareMain` — nenhum deles passa pelo guarda de
nome que existe em `CompareSchemaType.compare` (linhas 27-30).

`SchemaType.name` tem `lowerBound="1"` no `uschema.ecore`, ou seja, um
`SchemaType` sem nome é um modelo inválido. O EMF impõe isso na validação
explícita, não na atribuição; o PyEcore não impõe em lugar nenhum.

**Decisão no porte: replicar a falha, não mascará-la.** `compare_names` lê
`s1.name.lower()` sem `str()`, e levanta `AttributeError` no mesmo ponto em que
o Java levantaria `NullPointerException`. Documentado em `Raises`.

A alternativa rejeitada era `str(s1.name).lower()`, que converte `None` em
`"none"` — dois modelos inválidos passariam a casar entre si, silenciosamente.

---

## C4 — casamento guloso sobre uma relação não-transitiva

**Sítio:** o padrão `stream().filter(...).findFirst()` + remoção condicional, em
`CompareKey`, `CompareStructuralVariation`, `ComparePTuple` e `CompareAggregate`.
No porte, concentrado em `_match_bag`.

```java
for (StructuralVariation v1 : a1.getAggregates())
{
  Optional<StructuralVariation> toErase = s2Copy.stream()
      .filter(v2 -> ... CompareSchemaType.compareNames(v1.getContainer(), v2.getContainer()))
      .findFirst();
  if (toErase.isPresent())
    s2Copy.remove(toErase.get());
}
if (!s2Copy.isEmpty()) return false;
```

**Sintoma.** Isto é um pareamento bipartido resolvido por algoritmo guloso. Ele
só é correto quando o predicado é uma **relação de equivalência** (particiona os
elementos em classes), porque aí qualquer escolha serve.

`compareNames` **não** é uma relação de equivalência — não é transitiva:

```text
orderx ~ order   ->  True
order  ~ sorder  ->  True
orderx ~ sorder  ->  False
```

Logo, o guloso pode consumir um parceiro "errado" e falhar mesmo existindo um
pareamento perfeito. Reprodução, com o `_match_bag` do porte:

```text
items1 = [order, sorders]     items2 = [orders, orderx]

pareamento perfeito existe:  order ~ orderx  e  sorders ~ orders
_match_bag(items1, items2)  ->  False        <- falso negativo

_match_bag(items1, [orderx, orders])  ->  True   <- depende da ORDEM de items2
```

O resultado depende da ordem de serialização do XMI.

**Quais chamadores estão expostos.** Não basta olhar o predicado nominal: a
não-transitividade se propaga por despacho. `compare_feature` herda o defeito
sempre que compara dois `Aggregate` — verificado:

```text
addr(orderx) ~ addr(order)   ->  True
addr(order)  ~ addr(sorder)  ->  True
addr(orderx) ~ addr(sorder)  ->  False
```

O que salva `CompareKey`, `CompareStructuralVariation` e o bloco `attributes` de
`CompareReference` **não** é o predicado ser uma equivalência — é um invariante
dos dados. `CompareFeature` exige **nome idêntico** antes de despachar por tipo,
e os nomes de features dentro de uma variação são únicos (são as chaves do
documento). Cada elemento de `items1` tem, portanto, no máximo **um** candidato
em `items2`: o grafo bipartido tem grau ≤ 1 e o guloso é trivialmente ótimo.

Esse invariante não é imposto pelo `uschema.ecore` — `StructuralVariation.features`
não declara unicidade de nome. Ele vale porque o inferidor o produz assim.

`ComparePTuple` está a salvo de verdade: `compareDataType` é equalidade de forma
canônica (`Long≡Integer≡Number`), uma relação de equivalência legítima.

Sobra **`CompareAggregate`**: `aggregates` é uma lista de variações sem chave de
nome, casadas diretamente por `compareNames` sobre o `container`. Grau
arbitrário, predicado não-transitivo, guloso.

**Alcançabilidade.** Requer um `Aggregate` apontando para variações de duas ou
mais entidades cujos nomes se casem por substring e cujo pareamento guloso falhe.
Improvável com nomes de entidade reais (`Order`, `Address`, `Customer`), mas o
resultado ser **dependente da ordem** é, por si só, um defeito num instrumento de
validação: o mesmo par de modelos pode aprovar ou reprovar conforme a ordem em
que o XMI foi escrito.

**Decisão no porte: replicar.** A correção é trocar o guloso por um pareamento
máximo (Hopcroft–Karp, ou um Kuhn simples — as coleções são minúsculas). Isso
muda vereditos e portanto tem de ser proposto upstream, não introduzido
unilateralmente no porte.

---

## C5 — termo booleano morto em `compareNames`

`CompareSchemaType.java:98`:

```java
return (name1.equals(name2)) || name1.toLowerCase().equals(name2.toLowerCase()) || ...
```

`name1` e `name2` já vêm de `casec.apply(...)`, que é `n -> n.toLowerCase()`. O
segundo termo nunca pode ser verdadeiro quando o primeiro é falso.

Sem efeito observável. O porte colapsa os dois num `==` sobre nomes já
minusculizados, com comentário explicando a equivalência.

---

## C6 — guarda de nulo assimétrico em `CompareReference`

`CompareReference.java:45`:

```java
if (r1.getAttributes() != null && r2.getAttributes() != null)
{
    ... // compara
}
// se apenas UM for null, o bloco é pulado silenciosamente e o método segue
```

Todos os outros campos opcionais da mesma classe (`opposite`, `refsTo`) usam
`^` (XOR) e reprovam quando a presença difere. `attributes` usa `&&` e
simplesmente não compara.

Sem efeito observável: no EMF, uma `EList` de `upperBound="-1"` nunca é `null`.
O porte não traduz o guarda (coleções PyEcore também nunca são `None`).

---

## C7 — casamento de variações não-injetivo (falso positivo do harness)

**Sítio:** `es.um.uschema.doc2uschema.validation/.../main/USchemaCompareMain.java:106-133`,
no método privado `compareSchemaTypes`.

```java
private boolean compareSchemaTypes(SchemaType s1, SchemaType s2)
{
  boolean goodHit = true;

  if (s1.getVariations().size() != s2.getVariations().size())
  {
    warningLog.add("SchemaType warning: Variation lists sizes do not match: ...");
    goodHit = false;
  }

  for (StructuralVariation v1 : s1.getVariations())
  {
    Optional<StructuralVariation> varOption = s2.getVariations().stream()
        .filter(v2 -> varComparer.compare(v1, v2)).findAny();   // <-- sem remoção
    if (varOption.isPresent())
      hitLog.add("VariationType hit: ...");
    else
    {
      warningLog.add("VariationType warning: ...");
      goodHit = false;
    }
  }

  return goodHit;
}
```

**Sintoma.** O laço percorre apenas `s1 → s2`, e o `findAny` **não remove** a
variação casada do conjunto de candidatas. Duas variações de `s1` podem casar
com a **mesma** variação de `s2`. Como o único outro controle é a igualdade de
tamanhos, uma variação de `s2` pode nunca ser casada sem que nada seja
registrado.

**Reprodução** (simulando o algoritmo do Java sobre o `compare_variation` do
porte):

```text
s1 (porte)   = [A, A]      # duplicou a variação A, perdeu a B
s2 (oráculo) = [A, B]
tamanhos iguais? True
veredito              -> True

a variação B do oráculo nunca foi casada, e nenhum warning foi emitido.
```

**Por que é a pior categoria.** Todos os outros defeitos deste catálogo fazem o
harness reprovar algo válido, ou explodir. Este faz o harness **aprovar um
modelo errado**. Um porte que colapse variações a mais (ou a menos, desde que a
contagem final bata) passa na validação.

**Inconsistência interna.** A mesma base de código resolve o mesmo problema de
duas formas. `CompareStructuralVariation` casa `features` **com remoção** —
semântica de multiset, injetiva. `USchemaCompareMain.compareSchemaTypes` casa
variações **sem remoção** — semântica de conjunto. Não há comentário
justificando a diferença; parece descuido, não decisão.

**Decisão no porte: replicar o veredito, registrar a anomalia.** É a política
"fiel + reporte extra" já adotada para `count` e `root`. O
`_compare_schema_type_variations` reproduz o `findAny` sem remoção (o veredito
fatal continua idêntico ao do oráculo), **e** registra uma divergência
**não-fatal** quando o casamento não é injetivo — ou seja, quando alguma
variação de `s2` fica sem par. Assim o relatório mostra o buraco sem que o
harness passe a reprovar modelos que o Java aprova.

**Correção upstream.** Reusar a mesma mecânica do `CompareStructuralVariation`:
remover a variação casada de uma cópia de `s2.getVariations()` e exigir que a
cópia esvazie. Isso torna a comparação injetiva e sobrejetiva de uma vez, e
dispensa até o check de tamanho (que passa a ser consequência).

---

## I1 — `ordinalize` testa o número onde deveria testar o resto

`Inflector.java:468-483`:

```java
public String ordinalize(int number)
{
  int remainder = number % 100;            // calculado...
  String numberStr = Integer.toString(number);
  if (11 <= number && number <= 13)        // ...e ignorado: testa `number`
    return numberStr + "th";

  remainder = number % 10;                 // sobrescrito, sem nunca ter sido lido
  switch (remainder) { case 1: ... "st"; case 2: ... "nd"; case 3: ... "rd"; default: ... "th"; }
}
```

O `remainder` módulo 100 existe justamente para tratar a exceção do inglês:
11, 12 e 13 são `th` (`eleventh`, não `eleven-first`), e a regra vale para
**todas** as centenas — 111 é `111th`, 212 é `212th`. Mas o guarda testa
`number`, não `remainder`. A variável é escrita, nunca lida, e sobrescrita duas
linhas abaixo.

Resultado: a exceção só é aplicada aos números 11, 12 e 13 **literais**.

| entrada | `ordinalize` | correto |
|---|---|---|
| 11 | `11th` | `11th` |
| 111 | **`111st`** | `111th` |
| 112 | **`112nd`** | `112th` |
| 213 | **`213rd`** | `213th` |

O `InflectorTest` do upstream testa 1–39, 100–104, 200–204, 1000–1004,
10000–10004 e 100000–100004 — e **nenhum** `x11`–`x13` fora do primeiro. O bug
passa verde na suíte do próprio autor.

**Alcance.** Nenhum: `ordinalize` é código morto no pipeline. As três chamadas de
produção do Inflector usam só `capitalize` (`SchemaInference:183,188`;
`USchemaToDocumentDb:82,169`), `singularize` (`SchemaInference:233`;
`USchemaModelBuilder:194`; `ModelDirector:84,102`) e `pluralize`
(`DefaultReferenceMatcherCreator:26`). O defeito não toca nome de entidade
nenhum, e portanto não afeta a equivalência com o oráculo.

**Decisão no porte: replicar, com teste que fixa o valor errado.** Não porque
custe algo corrigir — não custa, é código morto —, mas porque a disciplina é a
mesma do resto do catálogo: o porte reproduz o oráculo e o desvio vai
documentado. Um teste afirma `ordinalize(111) == "111st"`, citando esta entrada;
se alguém "consertar" o método, o teste cai e obriga a leitura daqui.

**Correção upstream.** Trocar o guarda por `11 <= remainder && remainder <= 13`
(com `remainder` já sendo `number % 100`) e remover a reatribuição. Como o
`Inflector` é cópia versionada do ModeShape, a correção cabe **também** lá — e
o mesmo defeito deve estar em toda a linhagem de cópias dessa classe.

---

## I2 — `titleCase` é o único método da classe sem guarda de nulo

`Inflector.java:454-459`:

```java
public String titleCase(String words, String... removableTokens)
{
  String result = humanize(words, removableTokens);          // humanize(null) → null
  result = replaceAllWithUppercase(result, "\\b([a-z])", 1); // → Pattern.matcher(null) → NPE
  return result;
}
```

Todo método público da classe abre com o mesmo guarda — `pluralize`,
`singularize`, `camelCase`, `underscore`, `capitalize`, `humanize`,
`isUncountable` — e o contrato, uniforme, é **`null` entra, `null` sai**. O
`titleCase` é a única exceção, e não por decisão: ele não guarda porque delega ao
`humanize`, que guarda… e devolve o `null` que o `titleCase` então entrega ao
`replaceAllWithUppercase`. Lá, `Pattern.matcher(null)` estoura
`NullPointerException`.

Não é "entrada nula é erro" — se fosse, o método lançaria `IllegalArgumentException`
com mensagem, como faz o resto do ecossistema Java quando quer rejeitar nulo. É
uma linha faltando.

**Alcance.** Nenhum: `titleCase` é código morto no pipeline (ver o alcance de I1)
e o `InflectorTest` nunca o chama com `null` — a suíte do autor não exercita o
caminho.

**Decisão no porte: não replicar.** `title_case(None)` devolve `None`, alinhado
ao contrato uniforme dos outros nove métodos. É o único ponto do porte onde
**deliberadamente não se reproduz** o comportamento do oráculo, e a justificativa
é que não há comportamento a reproduzir: uma `NullPointerException` acidental não
é semântica, é ausência de semântica. Nenhuma equivalência estrutural depende
disso — o oráculo jamais chama `titleCase`, com nulo ou sem.

**Correção upstream.** Acrescentar ao `titleCase` o mesmo guarda dos irmãos:

```java
if (words == null)
  return null;
```

É a correção mais barata do catálogo: uma linha, sem mudança de comportamento em
nenhuma entrada válida, e fecha a única inconsistência de contrato da classe.

---

## I3 — o javadoc do `titleCase` documenta um método que não existe

`Inflector.java:444-445` (idêntico nas duas cópias versionadas):

```java
 *   inflector.titleCase("man from the boondocks")       #=> "Man From The Boondocks"
 *   inflector.titleCase("x-men: the last stand")        #=> "X Men: The Last Stand"
```

O segundo exemplo é **falso**. O `titleCase` delega ao `humanize`, e o `humanize`
não toca em hífen em passo nenhum: ele remove o `_id` final, remove os tokens
pedidos, troca `_+` por espaço e capitaliza. O `-` atravessa intacto. O valor real
é `"X-Men: The Last Stand"`.

**Causa.** O exemplo veio do `titleize` do Rails, que faz um passo a mais:

```ruby
def titleize(word)
  humanize(underscore(word)).gsub(/\b(?<!['’`])[a-z]/) { ... }
end
```

O `underscore` do Rails converte `-` em `_`, e só então o `humanize` o vira
espaço — daí `"X Men"`. O ModeShape copiou o **javadoc** do Rails mas escreveu o
método sem a chamada a `underscore`. A doc descreve o Rails; o código, outra
coisa.

**Alcance.** Nenhum em execução: `titleCase` é código morto no pipeline (ver o
alcance de I1) e o `InflectorTest` **não cobre o caso do hífen** — testa só
entradas com `_`, onde os dois comportamentos coincidem. Foi por isso que o
exemplo sobreviveu.

**Alcance como armadilha.** É o risco real deste achado, e o motivo de estar
catalogado apesar de não afetar comportamento: quem ler o javadoc e "consertar" o
`humanize` para tratar `-` **muda a nomeação de entidade**. O `humanize` não é
chamado pelo pipeline hoje, mas o `capitalize` — que é — compartilha com ele a
premissa de que separador não-`_` passa intacto. Uma coleção `order-details`
viraria `Order details` em vez de `Order-details`.

**Decisão no porte: replicar o código, não o javadoc.** `title_case("x-men: the
last stand")` devolve `"X-Men: The Last Stand"`, como o Java realmente faz. O
docstring do porte não repete o exemplo falso.

**Correção upstream.** Trocar o exemplo do javadoc pelo valor verdadeiro
(`"X-Men: The Last Stand"`) — **não** mexer no código para fazê-lo casar com a
doc: isso mudaria o comportamento de um método público por causa de um comentário
errado. Se alguém quiser a semântica do Rails, é `titleCase(underscore(word))`, no
chamador.

---

## M1 — a sentinela `0` de `combineMetadata` só vale de um lado

**Sítio:** `es.um.uschema.doc2uschema/.../metadata/ObjectMetadata.java:50-60`.

```java
public void combineMetadata(ObjectMetadata orig)
{
    count += orig.count;

    if (firstTimestamp == 0 || orig.firstTimestamp < firstTimestamp)  // :55
        firstTimestamp = orig.firstTimestamp;

    if (lastTimestamp == 0 || orig.lastTimestamp > lastTimestamp)     // :58
        lastTimestamp = orig.lastTimestamp;
}
```

O `0` é sentinela de *"ainda não sei"* — o construtor sem argumentos (`:10-12`)
deixa os três campos zerados, e é ele que o `infer` usa para todo objeto não-raiz
(`SchemaInference.java:198`). O teste `x == 0` reconhece essa sentinela **no
receptor**, mas nada a reconhece **no argumento**.

**Sintoma.** Com `firstTimestamp` já preenchido e um `orig` zerado, o segundo
termo decide: `0 < 1781470615` é verdadeiro, e o timestamp real é **substituído
por zero**. Um documento sem timestamp apaga a janela de quem tem.

```text
ObjectMetadata(5, 100, 200).combineMetadata(ObjectMetadata(1, 0, 0))
  → count=6, firstTimestamp=0, lastTimestamp=200
```

**Assimetria.** O `lastTimestamp` é imune por acidente aritmético, não por
desenho: `0 > x` é falso para qualquer `x` positivo, então o ramo nunca dispara.
As duas metades do método parecem simétricas e não são.

**Alcance.** Não é teórico — é o caminho normal do **#6**. Ali, `_id` que não é
`ObjectId` produz `timestamp = 0` (`Helpers.java:66`, já com o patch), e toda
coleção de origem relacional cai nesse caso. No Northwind, onde *nenhum*
documento tem `ObjectId`, os dois lados são zero e o defeito não se manifesta;
ele aparece em coleção **mista**, com parte dos documentos importada e parte
inserida pelo Mongo. Também é alcançado pelo `innerCountAndTimestampsAdjust`
(`SchemaInference.java:92-113`), que combina o meta de entidades internas — que
nascem zeradas — com o das ocorrências-raiz.

**Decisão no porte: replicar.** `combine_metadata` transcreve as duas condições
como estão. Corrigir mudaria `firstTimestamp` em toda variação de coleção mista,
e o harness da 0.3 acusaria divergência contra o oráculo em cada uma —
divergência causada por nós, no meio de uma fase cujo critério de aceite é
justamente a equivalência. O comportamento está travado por teste
(`tests/unit/test_metadata.py::test_zero_a_direita_sobrescreve_o_first_timestamp`),
com a asserção escrita ao contrário do desejável e comentada como tal.

**Correção upstream.** Guardar também o lado do argumento, tratando `0` como
ausência nos dois: só adotar `orig.firstTimestamp` se ele for diferente de zero.
É correção de uma linha e não altera nenhum caso em que ambos os lados são
válidos — mas só depois de a equivalência estar demonstrada.

---

## M2 — `innerCountAndTimestampsAdjust` estoura quando o Joiner funde uma entidade interna

**Sítio:** `es.um.uschema.doc2uschema/.../process/SchemaInference.java:100-104`,
e a costura com `DefaultAliasedAggregatedEntityJoiner.java:34`.

```java
// SchemaInference.java — sem guarda de null:
for (String innerSchema : innerSchemaNames)
{
    for (SchemaComponent schComponent : rawEntities.get(innerSchema))   // <-- NPE se innerSchema não existe mais
    ...
}
```

`infer(IAJArray rows)` roda `joiner.joinAggregatedEntities` **antes** de
`innerCountAndTimestampsAdjust` (`:138-139`). O Joiner, ao achar um alias,
remove a chave de `rawEntities`
(`DefaultAliasedAggregatedEntityJoiner.java:34`, `rawEntities.remove(iSchemaName)`)
— mas `innerSchemaNames` (o `Set`) **nunca** é atualizado, nem pelo Joiner nem
por ninguém depois. `innerCountAndTimestampsAdjust` continua iterando sobre o
`Set` inteiro e faz `rawEntities.get(innerSchema)` sem checar `null`.

**Sintoma confirmado (não só teórico).** Testado com o porte já feito da 1.2 e
1.3a: **toda vez** que o Joiner acha um match — ou seja, toda vez que ele não é
um no-op —, o passo seguinte estoura. Não é um caso extremo raro: é uma
consequência garantida de qualquer fusão bem-sucedida do Joiner. Reproduzido em
`tests/unit/test_schema_inference.py::test_joiner_bem_sucedido_estoura_key_error_no_passo_seguinte`.

**Alcance real, ainda não verificado.** Não sei se os datasets de teste
(Northwind, mintest) têm algum campo cujo nome bata com alguma das 10
`AggregateHintWords` (`has`, `with`, `set`, `list`, `setof`, `listof`, `array`,
`arrayof`, `collection`, `collectionof`) contra outra entidade já registrada.
Se nenhum bater, o Joiner nunca funde nada nesses dados, e este defeito fica
invisível neles — mas segue real e alcançável em qualquer dataset com um campo
assim nomeado.

**Decisão no porte (2026-07-21): replicar fielmente, sem guarda.** O
`KeyError` do Python é o análogo direto do `NullPointerException` do Java —
mesmo ponto, mesma causa, mesma ausência de tratamento. Não adicionar
`if innerSchema not in raw_entities: continue` — isso seria "consertar" antes
de demonstrar equivalência com o oráculo, e mudaria o comportamento observável
(o Java quebra; um porte que não quebra já divergiu).

**Correção upstream (candidata, não aplicada).** Remover de `innerSchemaNames`
qualquer nome que o Joiner tenha absorvido (ou filtrar em
`innerCountAndTimestampsAdjust` os nomes que ainda existem em `rawEntities`).
Precisa de dado antes/depois pra avaliar impacto — mesma cautela do #8/M1.

---

## M3 — `sortByCount` não ordena: `ECollections.sort` está comentado

**Sítio:** `es.um.uschema.doc2uschema/.../process/util/DefaultStructuralVariationSorter.java:36-42`.

```java
private void sortByCount(EList<StructuralVariation> variations)
{
    //ECollections.sort(variations, new CompareByCount());   // :40, comentado
    reOrderVariationIds(variations);
}
```

**Sintoma.** `sort` (`:13-24`) escolhe o critério em cascata —
`firstTimestamp` se algum for não-zero, senão `lastTimestamp`, senão `count`,
senão nº de propriedades. O terceiro ramo chama `sortByCount`, mas a única
linha que ordenaria de fato está comentada. O método só renumera
`variationId` sequencialmente (`:41`), na ordem em que as variações já
estavam — não ordena por `count` nenhuma.

**Alcance.** Dispara sempre que nenhuma variação tem `firstTimestamp` nem
`lastTimestamp` diferente de zero, mas ao menos uma tem `count != 0` — ou
seja, exatamente o caso de dados sem `ObjectId` (**#6**) processados sem
timestamp real, como o Northwind. É um caminho plausivelmente comum, não um
canto raro.

**Decisão no porte: replicar fielmente.** `sort_structural_variations` (ver
docstring) reproduz o no-op: no ramo de `count`, não chama `.sort(...)`
nenhum — só `_reorder_variation_ids`. Adicionar a ordenação que falta seria
"consertar" antes de demonstrar equivalência estrutural; e mudaria a ordem
observável de `variationId` em qualquer dataset que caia nesse ramo.

**Correção upstream (candidata, não aplicada).** Descomentar o
`ECollections.sort(variations, new CompareByCount())` — ou, no porte,
`variations.sort(key=functools.cmp_to_key(_compare_by_count))` com um
comparador análogo aos outros dois. Precisa de dado antes/depois (mesma
cautela do #8/M1/M2): muda a ordem de `variationId` publicada.

---

## M4 — os comparadores de `StructuralVariationSorter` não são uma ordem total

**Sítio:** `DefaultStructuralVariationSorter.java:26-30` (`compareByFirstTimestamp`),
`:32-36` (`compareByLastTimestamp`), `:44-48` (`compareByPropertyNumber`).

```java
private int compareByFirstTimestamp(StructuralVariation v1, StructuralVariation v2)
{
    if (v1.getFirstTimestamp() < v2.getFirstTimestamp())
        return -1;
    return 1;                              // nunca 0, mesmo se forem iguais
}
```

As outras duas repetem exatamente essa forma, cada uma sobre o campo que dá
nome ao método.

**Sintoma.** Um `Comparator<T>` só define uma ordem total se `compare(a, b)
== 0` sempre que `a` e `b` forem equivalentes pro critério. Aqui, dois
`StructuralVariation` com o mesmo `firstTimestamp` (ou `lastTimestamp`, ou nº
de propriedades) nunca empatam — o segundo argumento sempre "vence"
(`return 1`), inclusive quando `v1 == v2` de verdade (comparação de um
elemento consigo mesmo durante o sort, por exemplo). Não é um bug que lance
exceção: `Collections.sort`/`List.sort` do Java toleram comparadores
inconsistentes (ao contrário do `TimSort` de outras linguagens, que pode
lançar `IllegalArgumentException` ao detectar contrato violado) — o resultado
é só uma ordem entre "iguais" que depende de detalhes do algoritmo de
ordenação, não do critério declarado.

**Decisão no porte: replicar fielmente.** `_compare_by_first_timestamp`,
`_compare_by_last_timestamp` e `_compare_by_property_number` (ver
`strategies.py`) devolvem `-1`/`1` na mesma forma, nunca `0` — passadas a
`functools.cmp_to_key`, que aceita esse contrato quebrado sem reclamar (o
Python também não valida consistência de comparador). A ordem resultante
entre variações "empatadas" no critério ativo fica sujeita ao algoritmo de
ordenação (Timsort, em ambas as linguagens) — não é a mesma coisa que dizer
que a ordem é estável ou previsível; é só dizer que não estoura. **Deve ser
fixada por teste** o comportamento observado, não o que "deveria" ser.

**Correção upstream (candidata, não aplicada).** Cada comparador devolver
`0` quando os campos forem iguais:

```java
if (v1.getFirstTimestamp() < v2.getFirstTimestamp()) return -1;
if (v1.getFirstTimestamp() > v2.getFirstTimestamp()) return 1;
return 0;
```

Não muda o veredito de equivalência estrutural (`variationId` já não entra
em `CompareStructuralVariation`, ver "O que não é defeito"), mas pode mudar
a **ordem de listagem** das variações no XMI — outro caso que exige dado
antes/depois antes de propor.

---

## M5 — `homogeneousArraysMerge` indexa array vazio quando os dois lados colapsam vazios

**Sítio:** `es.um.uschema.doc2uschema/.../process/util/DefaultEVariationMerger.java:120-143`.

```java
private boolean homogeneousArraysMerge(String id, ArraySC toConsider, ArraySC sc)
{
    // Homogeneous arrays have either zero or one element
    // Not both of them can have zero elements, as they would have merged in the previous
    // phase, so find if any of them has zero size.
    if (toConsider.size() == 0 || sc.size() == 0
        || toConsider.getInners().get(0).equals(sc.getInners().get(0)))
    {
        int lowerBounds = Math.min(toConsider.getLowerBounds(), sc.getLowerBounds());

        // If this is the empty array, then it won't be empty
        if (sc.size() == 0)
            sc.add(toConsider.getInners().get(0));   // :132
        ...
```

**Sintoma.** O comentário do autor (`:122-124`) assume que os dois lados nunca
chegam vazios ao mesmo tempo — "não podem ambos ter zero elementos, pois já
teriam colapsado numa fase anterior". A suposição é falsa: `walkAndMerge`
(nível objeto, `:72-91`) compara campo a campo, em ordem, e para no primeiro
que não casar. Basta **outro** campo do mesmo par de variações reconciliar com
sucesso (por exemplo, um array cheio×vazio — o próprio caso que este método
trata) para o walk alcançar um **segundo** campo array, vazio nos dois lados:
`toConsider.size() == 0` já satisfaz o `||` da condição, o corpo do `if`
executa, e `sc.add(toConsider.getInners().get(0))` (`:132`) estoura
`IndexOutOfBoundsException` — `toConsider.getInners()` também está vazio.

**Confirmado por execução real do Java** (JDK 11, fontes do commit pinado via
`git show`): mesmo `IndexOutOfBoundsException`, mesma linha (`:132`); o porte
reproduz com `IndexError` no ponto equivalente.

**Decisão no porte: replicar.** Não adicionar guarda de tamanho antes de
`to_consider.inners[0]` — isso "consertaria" um crash que o próprio oráculo
tem, no mesmo cenário, contradizendo a premissa do autor. Travado por teste
(`tests/unit/test_strategies.py::test_merge_ambos_vazios_estoura_index_error`).

**Correção upstream (candidata, não aplicada).** Checar `toConsider.size() ==
0` explicitamente antes de indexar (ou reordenar a condição do `if` para não
avaliar `.get(0)` quando os dois lados estão vazios) — mesma família de
correção do #7, noutra classe.

---

## M6 — chave concatenada crua no regex do `ReferenceMatcher`, sem escape

**Sítio:** `es.um.uschema.doc2uschema/.../process/util/DefaultReferenceMatcher.java:34-50`.

```java
idRegexps = stream.flatMap(entry ->
    Affixes.stream().flatMap(affix ->
        Stream.concat(
            // prefix
            StopChars.stream().map(c ->
                MakePair.of(("^" + entry.getKey() + c + affix + ".*$").toLowerCase(), entry.getValue())),
            ...
```

**Sintoma.** `entry.getKey()` — o nome da entidade (e suas variantes plural/
singular, ver `create_reference_matcher`) — entra **cru** na string que vira
`Pattern`/`Optional<T>` via `.matches(...)`. Não há `Pattern.quote` nem
qualquer escape. Um metacaractere de regex no nome (`.`, `+`, `(`, `[`, `|`,
…) é interpretado como regex, não como texto literal: `"a.b"` como nome de
entidade faz o `.` casar **qualquer caractere**, não só um ponto.

**Confirmado por execução real do Java** (JDK 11, fontes do commit pinado via
`git show`): `"a.b"` casa `"aXb_id"` nos dois — o `.` não escapado vira
wildcard igual no oráculo e no porte.

**Origem prática.** O nome da entidade vem do marcador de tipo (`_type`) do
documento, via `Inflector.capitalize` (`SchemaInference.java:183,188`) — não
é uma string arbitrária de um atacante externo ao pipeline, mas também não é
validada contra caracteres especiais em nenhum ponto do original.

**Decisão no porte: replicar.** Não escapar `key` com `re.escape` antes de
compor os padrões — isso tornaria o porte mais restrito que o oráculo em
nomes de entidade com metacaracteres de regex, uma divergência de
comportamento, não uma correção neutra. Travado por teste
(`tests/unit/test_strategies_emf.py::test_reference_matcher_m6_chave_com_metacaractere_regex_vira_wildcard`).

**Correção upstream (candidata, não aplicada).** Escapar a chave com
`Pattern.quote(entry.getKey())` (ou, no porte, `re.escape(key)`) antes de
compor cada padrão. Não deveria mudar nenhum casamento em nomes de entidade
alfanuméricos comuns — só nomes com metacaracteres deixariam de casar de
forma incidental. Ainda assim, requer dado antes/depois pra confirmar que
nenhum dataset de referência depende do casamento incidental.

---

## N1 — labels próprios ordenados, `refsTo` não — dois `EntityType` pro mesmo nó

**Sítio:** `IdArchetypeMapping.java:60-62` (labels próprios, ordenados) vs.
`:100-104` (`addRelationships`/`targetLabels`, não ordenados).

**Sintoma.** Um nó multi-label lido em seu próprio direito ordena os labels
alfabeticamente pro nome do `EntityType`; o mesmo nó como `refsTo` de uma
relação usa a ordem crua do `labels()` — gerando dois `EntityType`
diferentes pro mesmo nó físico.

**Confirmado com dado real (Neo4j Aura):** nó `:Zebra:Apple` referenciado
por outro produziu `Apple_AND_Zebra` (variação real) e `Zebra_AND_Apple`
(placeholder vazio) como dois `EntityType` distintos.

**Decisão no porte: replicar** — `node_archetype` ordena, `_relationship_archetype` não, mesma assimetria do Java.

**Correção upstream (candidata, não aplicada):** ordenar `targetLabels` em `addRelationships` também.

---

## E1 — leitura *eager* do Neo4j colapsa a vazão do bolt em resultado grande

**Defeito do porte, corrigido em 08/08/2026.** Está catalogado aqui porque
durante seis dias foi diagnosticado errado, e o diagnóstico errado chegou a
virar decisão de projeto (o `--settle`) e a contaminar os tempos do paradigma
grafo. O prefixo `E` era de *ambiente* — a investigação mostrou que a causa é
**nossa**, não do servidor. A letra fica para não quebrar as citações.

### O que era

`_read_label_combination` (`extractors/neo4j.py`) lia com
`driver.execute_query`, que é **eager**: materializa a lista inteira antes de
devolver. No `large` são 2,66 milhões de registros, cada um com o nó completo.

Conforme a lista cresce, o processo passa mais tempo alocando memória e menos
drenando o socket. O buffer de recepção enche, o TCP fecha a janela — que é o
comportamento correto do controle de fluxo — e o servidor fica **impedido de
enviar**. A vazão desaba para ~47 KB/s, em loopback.

Visto de fora parece impasse: os dois lados ociosos (~2% de CPU), transação
`Running` há dezenas de minutos, memória de sobra na máquina. Não é impasse — é
rastejo. O `ss -tni` do lado servidor é o que fecha o diagnóstico:

```text
rwnd_limited: 1001379ms (100,0%)     snd_wnd: 9216     Recv-Q: 0
```

Cem por cento do tempo bloqueado pela janela do cliente.

### A medição que decidiu

Mesma *query*, mesmo servidor, mesmo instante, 200 mil registros:

| consumo | tempo | taxa | memória |
|---|---|---|---|
| streaming (`session.run`) | 6,3s | 31.549 rec/s | constante |
| eager (`driver.execute_query`) | 39,6s | 5.045 rec/s | 424 MB |

**6,3× mais lento**, e o problema se realimenta: quanto maior a lista, mais
devagar se lê. Daí o degrau — `small` e `medium` sempre passavam, `large` e
`larger` colapsavam.

### A correção

`_read_label_combination` virou **gerador** sobre `session.run()`. Mesma
*query*, mesmos registros, mesma ordem; o chamador
(`extract_archetype_counts`) já recebia `Iterator`. O `large` passou de "não
terminou em 15 min" para **121,0s**, com as contagens fechando (400.000 `User`
em 6 variações, 200.000 `Movie`, 9 arquétipos).

Os 121,0s batem com os 120,34s medidos em 02/08 para o mesmo tamanho: a correção
não deixa mais rápido que o melhor caso — **torna o melhor caso confiável**.

### O que o diagnóstico anterior afirmava, e por que errava

| afirmação anterior | o que a medição mostra |
|---|---|
| "comportamento do **servidor** Neo4j" | é do **cliente**; o servidor está ocioso esperando |
| "deleção massiva contamina a extração seguinte" | coincidência de ordem — as baterias vão `small`→`larger`, então a maior deleção sempre precede a maior extração. O que decide é o **tamanho do resultado** |
| "travou por 24 minutos" | não trava: rasteja a ~47 KB/s |
| contorno: `--settle` | esperar antes de começar não muda como o cliente consome depois. Falhou com 30s **e** com 60s |
| "não é código nosso nem do original" | é código nosso, e a correção é uma função |

**Lição de método, não de Neo4j:** "os dois lados ociosos com transação aberta"
foi lido como estado do servidor quando é a assinatura de *backpressure* — o
consumidor não consome. A pergunta que teria encurtado seis dias é `ss -tni`,
não `SHOW TRANSACTIONS`.

### Consequência para os números já publicados: menor do que parecia

**O `E1` é intermitente, não sistemático.** Quando não dispara, a extração roda
na velocidade correta. Medido em 08/08, seed 23, contra o arquivo de 02/08:

| tamanho | 02/08 (pré-correção, 3 sementes) | 08/08 (pós-correção) |
|---|---|---|
| `small` | 19,82 · 21,89 · 14,92 | 13,06 · 14,80 |
| `medium` | 38,48 · 38,69 · 38,23 | 36,55 · 36,90 |
| `large` | 120,84 · 121,09 · 131,38 | 120,90 · 123,60 |
| `larger` | 440,93 · 435,78 · 437,53 | 436,95 · 437,55 |

Comparando **mediana com mediana** (pré · pós): `small` 19,82 · 13,93 (−29,7%);
`medium` 38,48 · 36,72 (−4,6%); `large` 121,09 · 122,25 (+1,0%); `larger`
437,53 · 437,25 (−0,1%). Os dois conjuntos têm tamanhos diferentes (3 e 2
corridas), então a comparação é indicativa, não um teste.

**Os tempos publicados sobrevivem** — e a leitura é essa, não "as medianas
batem": nos dois tamanhos grandes elas batem (≤1%), e nos dois menores o
pós-correção é **mais rápido**. Um número publicado que o `E1` inflou erra para
cima, nunca para baixo, então nenhuma conclusão da tabela de 02/08 depende de
tempo que a correção teria encurtado. O `small`, com os 30% de diferença, é o
tamanho onde o `E1` mais pesava em termos relativos — 6 segundos de resíduo
sobre uma corrida de 14.

O `E1` aparece como **outlier isolado**, não como inflação difusa: o
`movies_min` a 56,44s contra 13–15s do normal, e o `large` a 267,43s de 01/08.
São as corridas em que ele pegou.

E é por isso que a **regra da mediana de 3** salvou as tabelas de 02/08 — ela
descartou exatamente as corridas contaminadas.

**A correção reduziu muito o `E1`, mas não o eliminou.** Antes dela, as três
tentativas de rodar a bateria travaram; depois, a suíte de três sementes rodou
inteira — mas ainda produziu um `small` a **156,09s** contra 13–14s do normal
(seed 207, logo após apagar o `larger` da 69). A leitura *eager* era a causa
**dominante**, não a única. O resíduo não foi investigado, e a regra da mediana
segue necessária.

> **Registro de correção.** Este documento afirmou, em 08/08, que os tempos de
> `large`/`larger` anteriores estavam "inflados em grau desconhecido". A
> remedição desmentiu. O que segue de pé é a causa, o mecanismo, a correção e o
> fato de o `--settle` nunca ter tido efeito; o que cai é o alcance do estrago.

### O que fica em aberto

**A memória ainda cresce com o número de nós.** O streaming resolveu o
transporte; `reduce_archetypes_by_node` continua agrupando por nó e manteve
**2.958 MB** vivos no `large`. É inerente ao algoritmo, não ao fetch. No
`larger` deve pedir ~6 GB — cabe na máquina de referência, mas é o número que
decide se o `mapPartitions` previsto na Fase 2.0 vira necessidade.

**O `--settle` foi removido** (08/08/2026), junto com o `SETTLE` da suíte: era
contorno de uma causa que não existia, e custava ~6 min por suíte de três
sementes. A limpeza do grafo segue acontecendo e vai para o log das baterias;
deixou de ser gravada em 15/08, junto com a tabela `cleanup.csv`.

### Reincidência sob streaming (03/10/2026)

No gate da 4.3 (`run_oracle_neo4j.py --engine spark`, semente 23), o
`up_larger` não terminou. Das 64 fatias (2 combinações de labels × 32), 63
acabaram; a última, de `User`, rastejou por 40 minutos e a corrida foi
interrompida. A leitura é a corrigida: no caminho Spark, o
`_read_label_combination` também itera `session.run()`.

Estado TCP capturado com `ss -tnio` antes de matar (cópia local em
`out/gate_4_3_neo4j/stall_tcp_snapshot.txt`, fora do git):

| lado | estado |
|---|---|
| servidor (`:7687`) | `rwnd_limited` em 100% dos 2.404 s, `snd_wnd: 9216`, timer `persist`, ~200 KB em `notsent`, 310 KB retransmitidos |
| cliente (worker do Spark) | `Recv-Q: 0`, processo parado em `poll` com ~2% de CPU, `rcvmss: 9216`, 2 pacotes fora de ordem |

Três amostras de 5 s deram ~45 KB/s — uma janela de 9.216 bytes a cada ~200 ms
de `persist`. O Neo4j também estava ocioso.

**O que difere do `E1` de 08/08.** Lá, o `Recv-Q: 0` registrado era o do socket
do servidor, que não diz nada sobre a janela do cliente. Aqui o do **cliente**
também é zero: o worker leu tudo o que chegou e está esperando, e mesmo assim a
janela anunciada não passa de 9 KB. "O consumidor não consome" não descreve este
estado — a janela não reabre com o buffer vazio. É da mesma família do resíduo
citado acima (o `small` a 156 s), mas é **uma ocorrência só**: a causa segue em
aberto, e nada aqui autoriza concluir mecanismo.

**`rwnd_limited` alto não é a assinatura.** A segunda tentativa do `up_larger`,
no mesmo dia, terminou normal (extração em 67 s), com o lado servidor gravado a
cada 15 s (`out/gate_4_3_neo4j/tcp_monitor_retry.txt`). As fatias saudáveis
também mostram `rwnd_limited` em ~97%, timer `persist` e `snd_wnd` de 5 a 39 KB
— é o normal de um cliente que é o gargalo. O que separa o travamento é o
cliente **ocioso** com `Recv-Q: 0` e a vazão: ~45 KB/s contra ~1 MB/s por
conexão.

---

## O que **não** é defeito

Registrado para evitar que uma leitura futura os "corrija":

- **`Comparator.checkNulls(null, null)` faz `compare` devolver `false`.** Dois
  nulos não são equivalentes. Contraintuitivo, mas deliberado e travado por
  teste no próprio JUnit do upstream
  (`CompareUSchemaTest`: `assertFalse(cSchema.compare(null, null))`).
  **Isto não torna o comparador não-reflexivo**, apesar da aparência: um tipo
  *ausente* só ocorre dentro de um contêiner, e **todo** contêiner
  (`ComparePList`, `ComparePSet`, `ComparePMap`, `CompareAttribute`) guarda o caso
  "ausente dos dois lados" com um `(x == null && y == null) ||` **antes** de
  delegar ao `CompareDataType`. A guarda com `or` só é alcançada quando não há
  contêiner que a proteja — o que o modelo não produz. Replicar a assimetria **no
  lugar certo** é o que mantém o veredito idêntico ao do oráculo.
- **`CompareAggregate` casa variações agregadas só pelo nome do `container`**,
  ignorando as `features` delas. É o que impede recursão infinita em agregado
  cíclico — o análogo do guarda que falta em C2. Deliberado.
- **A entidade agregada é unificada pelo nome do campo, não pelo caminho até ele
  — logo, coleções diferentes que embutem um campo homônimo colapsam numa
  entidade só.** Verificado no fonte (02/08/2026): `SchemaInference:233`
  singulariza o nome do elemento no caso do array (`details` → `detail`);
  `:188` define `schema.entityName` como esse nome capitalizado, **sem** a
  coleção de origem nem o caminho; e `:55`/`:201-217` acumulam tudo num
  `Map<String, List<SchemaComponent>>` **com chave só de nome**, onde cada
  esqueleto novo é comparado com as variações já presentes e reusado ou anexado.
  O tipo do mapa é a evidência de design. **Efeito no Northwind:**
  `orders.details` e `purchase_orders.details` viram um único `Detail` com
  **5 variações** em duas famílias disjuntas — v1-v3 com `unit_price`/`discount`/
  `status_id` (linha de venda), v4-v5 com `unit_cost`/`date_received`/
  `posted_to_inventory` (linha de compra) —, e cada `Aggregate` aponta só para o
  subconjunto que ocorre naquela variação-pai (`Orders` nunca alcança v4/v5).
  Desconfortável semanticamente, mas consistente e intencional: em outro dataset
  o mesmo mecanismo unifica um `address` embutido em `customers` e em
  `suppliers`, que é o efeito desejável. **Não perde dado — diferente do #8.**
- **`CompareStructuralVariation` ignora `variationId`, `count` e `timestamp`.**
  Há comentário explícito no upstream: *"Please note we do not compare
  variationId, count nor timestamp."* É a definição de equivalência
  **estrutural** adotada pelo projeto.
- **`ArraySC.equals` ignora o tamanho do array.** Ver #8: é load-bearing.
- **O `abstractjson` (Bridge Jackson/Gson, ~25 classes) não foi portado — é
  remoção deliberada, não porte incompleto** (Fase 1.5). O Bridge existe só para
  abstrair *duas* libs de JSON do Java; em Python a entrada já é `dict`/`list`
  nativo, então não há nada a abstrair. **Verificado que a remoção não perde
  semântica:** toda a decisão de tipo do Bridge está no `IAJIdentify`, com 7
  predicados (`isObject`/`isArray`/`isBoolean`/`isNumber`/`isNull`/`isTextual`/
  `isObjectId`), consumidos por `SchemaInference.infer:150-168`. Seis são
  triviais sobre `dict`/`list` nativo e estão reproduzidos 1-a-1 em
  `extractors/triple.py::classify` (mesma ordem do Java, `BOOLEAN` antes de
  `NUMBER` por causa do `bool`⊂`int`). O sétimo — `isObjectId` — é o **único**
  que o JSON nativo não distingue sozinho, e está resolvido na Fase 1.0: é o
  predicado `value == "oid"` (`JacksonElement:93`/`GsonElement:120`), a sentinela
  do map-reduce. Logo a remoção não altera comportamento observável. Não
  reintroduzir o Bridge "por completude" — ver a nota análoga do Inflector no
  `CLAUDE.md`.

---

## Depois do porte: contribuições propostas ao upstream

Ordem sugerida, do mais defensável ao mais invasivo:

1. **C7** — trocar o `findAny` sem remoção pela mecânica já usada em
   `CompareStructuralVariation`. Correção pequena, localizada num método
   privado, e é a única do catálogo que fecha um **falso positivo**. Deveria
   vir primeiro: enquanto ela não entra, o próprio harness de validação do
   upstream não sustenta as garantias que promete.
2. **#7** — correção trivial, o próprio autor diagnosticou no comentário, sem
   mudança de veredito em dado válido. Candidato óbvio a PR.
3. **M5** — mesma família do #7 (índice antes do teste de tamanho), noutra
   classe (`DefaultEVariationMerger`). Confirmado por execução real do Java,
   sem mudança de veredito em dado válido — só evita o crash.
4. **M6** — escapar a chave (`re.escape`/`Pattern.quote`) antes de compor os
   padrões do `ReferenceMatcher`. Não muda casamento em nomes alfanuméricos
   comuns; fecha um casamento incidental via metacaractere de regex. Requer
   dado antes/depois pra confirmar que nenhum dataset de referência depende
   do casamento incidental.
5. **I3** — corrigir o **exemplo do javadoc** do `titleCase` (o do hífen). Zero
   risco: não toca em código. Vem antes de I2 e I1 porque é a única do catálogo
   que, se ignorada, induz alguém a introduzir um defeito ao "consertar" o código
   para casar com a doc.
6. **I2** — guarda de nulo no `titleCase`. Uma linha, alinha o método ao contrato
   dos outros nove da classe, e não muda nenhuma entrada válida. A mais barata das
   que tocam em código.
7. **I1** — guarda ordinal sobre o resto, não sobre o número. Corrige valores
   objetivamente errados (`111st`), em método que ninguém do pipeline chama.
   Merece um teste novo: o `InflectorTest` do upstream não cobre `x11`–`x13` fora
   do primeiro. **Atenção ao alvo**: `I1`, `I2` e `I3` estão no `Inflector`, que é
   cópia versionada do **ModeShape** — o PR mais útil vai para lá, e o U-Schema só
   precisa reavaliar a cópia.
8. **C3** — guarda de nulo em `compareNames`, ou anotação `@NonNull`. Não muda
   comportamento em modelo válido.
9. **C6** — trocar `&&` por `^` em `attributes`, alinhando com os irmãos. Sem
   efeito hoje; previne regressão se a coleção puder ser nula no futuro.
10. **#6** — leitura genérica de `_id`. Muda o domínio de entrada aceito (aceita
    `_id` não-`ObjectId`), com o custo semântico de timestamps zerados. Requer
    discussão sobre o que o `firstTimestamp` deve significar aí.
11. **#8** — chamar `combineMetadata` no ponto de colapso inline
    (`SchemaInference.java:207-211`), combinando `count`/`firstTimestamp`/
    `lastTimestamp` da ocorrência nova na variação reaproveitada, em vez de
    simplesmente descartá-la. **Muda números publicados** (contagens e
    janelas de tempo de toda entidade cujas variações colapsam). Precisa vir
    acompanhado dos dados de antes/depois — que a Fase 3 **não** produz por
    decisão (o porte replica o #8; não há variante corrigida). O argumento mais
    forte que a Fase 3 entrega é outro, e é suficiente para justificar a
    proposta: o resultado atual é **ordem-dependente** (ver o achado de
    31/07/2026 na seção #8) — duas leituras do mesmo banco publicam contagens
    diferentes, o que torna o `count` do modelo não-reprodutível mesmo com o
    dado imutável.
12. **C1** — comparar todos os `isFeaturedBy` via multiset (correção já escrita,
    ver a seção C1). Muda vereditos do harness de validação do próprio upstream,
    mas só na direção segura: reprova a mais, nunca aprova a mais.
13. **C2** — guarda de ciclo em `opposite`. Exige decidir a semântica de
    comparação de referências mutuamente opostas antes de implementar.
14. **C4** — pareamento máximo no lugar do guloso. O mais invasivo: toca quatro
    comparadores. Alternativa mais barata: tornar `compareNames` uma relação de
    equivalência (por exemplo, comparando formas canônicas singularizadas pelo
    Inflector), o que resolveria a causa em vez do sintoma.

Nada disso deve entrar no porte antes de a equivalência estrutural com o oráculo
estar demonstrada. Um porte que "melhora" o original não pode ser validado
contra ele.

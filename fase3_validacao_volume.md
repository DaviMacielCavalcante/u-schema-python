# Fase 3 — Ponta a ponta + volume + correções por construção (guia detalhado)

**Parte de:** `roadmap_portabilidade.md`
**Entregável:** validação ponta a ponta + volume · **Pré-requisito:** Fases 1 e 2 com gates fechados

> **Premissas corrigidas pelas Fases 1 e 2 — ler antes de executar a fase.**
>
> - **Não há "log do Spark" para ler.** A 2.0 decidiu leitura por **driver
>   nativo** (`pymongo`/`neo4j`), Python; o tempo de inferência tem de ser
>   medido **em processo** (cronometrar o pipeline `extract_* → BuildUSchema`),
>   não extraído de log de executor. Spark segue como paralelizador opcional.
> - **O Northwind já fechou** na Fase 2.3 e foi re-executado em 31/07/2026
>   contra um MongoDB real (`equivalent=True`, só não-fatais do #8) — a 3.1
>   herda o resultado. A **reprodutibilidade**, que era o que faltava, foi
>   resolvida na própria Fase 3: os 17 JSONs estão versionados em
>   `resources/datasets/northwind/`, com a licença do dataset. **A quantidade de
>   divergências é ordem-dependente** (15 por arquivo, 12 por cursor): o
>   invariante é `equivalent=True` + não-fatais + 14/17 coleções. Ver
>   `bugs_originais.md` §#8.
> - **O Neo4j ponta a ponta fechou** em 31/07/2026 no tamanho `larger`:
>   `equivalent=True`, **zero divergências**, `soma User = 800.000` exata. Era a
>   lacuna que a 2.2 deixou (lá só a camada de construção foi validada).
> - **`scripts/gen_userprofiles.py` (Mongo, Rotas A/B)** existe em
>   `~/Documents/teste_uschema/`, fora do repo. **Não bloqueia a 3.2** — os 8
>   bancos `up_*` já estão materializados no MongoDB local —, mas precisa ser
>   trazido para o capítulo ser reproduzível por terceiros.
> - **O Neo4j não passa pelo núcleo da Fase 1** (achado da 2.2): a bateria do
>   grafo roda `extractors/neo4j.py` + `extractors/neo4j_model.py`.

## Objetivo

Validar o porte completo de ponta a ponta — extrator por driver nativo → núcleo de inferência → PyEcore → XMI — em **equivalência** (datasets reais) e **tamanho** (datasets sintéticos), confirmando que os bugs do código original ficam tratados **por construção**. É a fase de avaliação experimental do TCC. (No grafo o caminho é outro — `extractors/neo4j.py` + `extractors/neo4j_model.py`, sem passar pelo núcleo da Fase 1; ver o banner no topo.)

---

## 3.1 Equivalência (datasets reais)

### Northwind (relacional → documento)
17 coleções, `_id` inteiros, `order_details` embutido como `details` em `orders`. Casos que o porte tem de reproduzir (confirmados no XMI-oráculo de 109262 bytes):
- **19 `EntityType`** (17 raiz + 2 não-raiz: `_id` e `Detail`).
- **`Aggregate` aninhado**: `Detail` ligada a `Orders`/`Purchase_orders` por `Aggregate` (`upperBound="-1"`, `optional="true"`).
- **Variação estrutural** sobre o aninhado.
- **Contagens iguais às do oráculo nas 17 coleções** — o que inclui a subcontagem do #8 em `orders`/`products`/`purchase_orders`, replicada, **não** corrigida (ver 3.3).

**Tarefas:** rodar o pipeline Python sobre o Northwind; comparar o XMI com o oráculo pelo harness; diagnosticar e fechar divergências. **Já executado** na Fase 2.3 e re-executado em 31/07/2026 contra MongoDB real: `equivalent=True`, só divergências não-fatais, todas na assinatura do #8 (a quantidade varia com a ordem de leitura — o invariante é **14/17** coleções fechando).

### Sakila (segundo dataset real) — **DESCARTADO em 02/08/2026**
Era para replicar o protocolo (documento e/ou grafo) e servir de segundo ponto de equivalência, reduzindo o risco de *overfitting* ao Northwind.

**Não entra.** O levantamento do que existe publicado (`todolist_fase3.md` §3.1) achou versão MongoDB licenciada — `lilhuss26/sakila25`, MIT —, mas **em grafo não existe dataset nenhum**: o único candidato é um pipeline acadêmico que exige montar o Sakila no MySQL e converter, o que produziria um grafo **nosso**, não de terceiros. E o `sakila25` não é o Sakila clássico: é o esquema repovoado em 2025 com dados da API do TMDB.

**A consequência é limitação assumida, não lacuna a fechar:** a equivalência usa **um único dataset real**, o Northwind, e só no paradigma documento — o grafo é validado inteiramente sobre dado sintético gerado por script nosso. O *overfitting* ao Northwind não está descartado, e isso tem de aparecer na avaliação.

## 3.2 Tamanho (datasets sintéticos)

Reproduzir o experimento de volume do artigo (Tabelas 3/4), confirmando que o porte preserva a propriedade de crescimento. (Em **Python** — a 2.0 dispensou o Spark; ver o banner no topo.)

**Geradores (já existentes):** `gen_userprofiles.py` (MongoDB, Rotas A/B) e `gen_userprofiles_neo4j.py` (grafo). Quatro tamanhos: 100k/200k/400k/800k `User` (50k/100k/200k/400k `Movie`).

**MongoDB — duas rotas:**
- **Rota A** (`_id` ObjectId nativo) e **Rota B** (`_id` inteiro + ~15% arrays vazios — o cenário relacional→NoSQL). Referência i9 (ferramenta original): A ~0,47→4,09 s; B ~0,43→3,48 s.

**Neo4j — grafo:** `address` achatado; `watchedMovies`/`favoriteMovies` como arestas `WATCHED {stars}`/`FAVORITE`; ~15% users isolados. Referência i9: inferência ~3,83→34,86 s (a geração do grafo, ~180 s no maior, é mais cara que a inferência — assimetria do paradigma). **Medido no porte (31/07/2026): a assimetria é dependente de tamanho.** No `large` a geração domina (176,95s contra 123,05s de extração); no `small` é o contrário (13,47s contra 16,29s). E o gerador **não tinha semente** — regerar dava um sorteio novo, que foi o que produziu as 7 divergências não-fatais nos tamanhos regerados. **Resolvido em 01/08/2026:** com `--seed` no gerador e o **oráculo Java rodado sobre a mesma instância semeada**, o `compare()` dá `equivalent=True` com **zero** divergências. As 7 eram diferença de dataset, não do porte.

**Tarefas:**
- [ ] Rodar o porte nos quatro tamanhos, Rotas A/B (Mongo) e grafo (Neo4j); **cronometrar em processo** (não há log de executor — ver o banner no topo).
- [ ] Confirmar **leitura integral** (soma dos `count` = volume gerado) **no grafo**, onde ela é esperada (núcleo próprio, o #8 não passa por lá). No **documento** ela **não** fecha por causa do #8 replicado: medir a diferença e documentá-la como resultado, não como falha.
- [ ] Comparar a **tendência** de crescimento com a do oráculo (não o tempo absoluto — Python vs. JVM diferem; o que importa é a curva). **Medido em 3 sementes (31/07/2026): a direção se preserva, o fator de crescimento não** — e a precisão difere por paradigma. Extração, `small`→`larger` (8× de dado): Mongo Rota A **19,1×/19,4×/19,5×** contra 8,7× do oráculo; Neo4j **24,8×/29,7×/33,6×** contra 9,1×. O documento mede com dispersão < 2%; o grafo, 35%. **Citar o grafo como faixa, não como ponto.** Ver `todolist_fase3.md` §3.2.
- [ ] Sem estouro de memória (`MemoryError`). Se ocorrer, `mapPartitions` entra sem reescrever a lógica — ver 2.0.

## 3.3 Bugs: #6/#7 por construção, #8 replicado (regressão + tamanho)

> **Correção desta seção (31/07/2026).** A versão anterior listava o **#8**
> entre os "corrigidos por construção" e fixava números-alvo *com a correção*
> (50/50 no User Profiles, 17/17 no Northwind). **Isso contradizia a decisão de
> fidelidade do projeto** e foi removido: `bugs_originais.md` §#8 registra
> **"Decisão no porte: replicar"**, e `oracle/patches/` não tem um `0008` — nem
> o oráculo corrige. O porte **reproduz** a subcontagem; não há alvo corrigido
> nesta fase.

São **dois** os casos que o Java corrigiu por patch e o porte trata nativamente
— o terceiro é replicado de propósito. Cada um vira **teste de regressão** com
dataset mínimo dedicado **e** é exercitado em volume.

| Bug | No oráculo | Cenário | Critério no porte |
|---|---|---|---|
| **#6** `_id` inteiro | patch `0006` | origem relacional (Northwind, Rota B) | não assume `ObjectId`; roda os 800k da Rota B sem `ClassCastException`/`TypeError` |
| **#7** array vazio | patch `0007` | ~15% dos docs na Rota B | não indexa elemento inexistente; roda sem `IndexOutOfBounds`/`IndexError` |
| **#8** contagem sob array de tamanho variável | **sem patch** | `watchedMovies`/`favoriteMovies`; `details[]`, `supplier_ids[]` no Northwind | **replicar**: a subcontagem do porte casa com a do oráculo (é o que mantém `compare()` equivalente) |

**O que o #8 custa, para contexto — não é alvo do porte.** No experimento
*original*, com a correção aplicada, o User Profiles dividia 50/50 entre as duas
variações de `User` e as 17 coleções do Northwind batiam (contra 14 sem a
correção; as 3 que erravam — `orders`, `products`, `purchase_orders` — eram
exatamente as com campo array de tamanho variável). **O porte não persegue esses
números**: perseguí-los quebraria a equivalência com o oráculo, que é o critério
da fase. Medir a correção exigiria dados antes/depois que esta fase não produz —
fica como proposta upstream (`bugs_originais.md`, "contribuições propostas").

**Tarefas:**
- [ ] Teste de regressão #6 (dataset com `_id` inteiro), #7 (dataset com `[]`), #8 (dataset com array de tamanho variável).
- [ ] Medir e **documentar** a subcontagem do #8 no porte nos 4 tamanhos, mostrando que casa com a do oráculo.
- [ ] Documentar como capítulo de reprodutibilidade: o porte corrige **#6 e #7** por design onde o original precisou de patch — e replica o **#8** deliberadamente, porque um porte que melhora o original não pode ser validado contra ele.

## 3.4 Coleta e análise

Coletar as métricas e compará-las com o artigo. As quatro tabelas de `results/` (esquema em `dicionario_de_dados.md`) e uma tabela de comparação: `normalized` do porte, do oráculo e a Table 4, por paradigma e tamanho. Detalhe em `todolist_fase3.md` §3.4.

## Gate de aceite da Fase 3

- Northwind: equivalência estrutural com o oráculo, no paradigma **documento** —
  único dataset real da fase (Sakila foi descartado, §3.1). No **grafo**, a
  equivalência é sobre o dataset sintético do User Profiles, contra o oráculo
  Java rodado na mesma instância.
- Volume: tendência de crescimento reproduzida nos quatro tamanhos, leitura integral confirmada **no grafo**, sem estouro de memória.
- Bugs **#6/#7**: tratados por construção (rodam sem patch em 800k), com testes de regressão verdes.
- Bug **#8**: subcontagem **replicada** e medida, casando com a do oráculo. Um porte que aqui "acertasse" o volume real estaria **fora** do gate, não dentro dele.

## Entregáveis

Scripts de execução das baterias (equivalência + volume), suíte de regressão dos bugs, CSVs de resultados, a tabela de comparação com o artigo e o material do capítulo de avaliação experimental (equivalência, volume, correções por construção).

## Riscos da fase

Python mais lento que a JVM (**parcialmente** irrelevante — H1 é sobre tendência, mas a bateria de 31/07/2026 mostrou que o porte é **superlinear** onde o oráculo é quase linear, então "a curva se preserva" precisa ser afirmado com ressalva, não como dado); custo de **geração** do grafo Neo4j dominando o tempo de bateria em volume grande (medido: 176,95s no `large`; é da materialização, não da inferência — e no `small` a extração custa mais que a geração); **geradores sem semente**, o que impede reconstruir a instância exata que produziu os XMIs de referência; divergência estrutural residual em dataset real (o harness aponta a categoria; voltar à Fase 1/2 conforme o módulo); **perfil de memória** diferente do oráculo, já que a leitura roda em Python, no próprio processo, e não distribuída em executores.

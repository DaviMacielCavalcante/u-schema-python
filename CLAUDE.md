# uschema

Porte **fiel e completo** do pipeline de inferência do **U-Schema** (extração →
inferência → serialização) do Java para **Python**, nos paradigmas **documento
(MongoDB)** e **grafo (Neo4j)**. É o objeto de um TCC (repositório `arco-do-tc-2`).
A entrega é **Python puro**; o oráculo Java em Docker (`oracle/`) existe só para
gerar os XMIs de referência.

"Fiel" = **equivalência estrutural** de comportamento (mesmas entidades,
variações, atributos, agregados, referências e contagens) contra o XMI gerado
pela ferramenta Java patcheada — **não** XMI idêntico byte a byte.

O plano completo está nos `.md` da raiz: `roadmap_portabilidade.md` e
`fase0`…`fase3`. Leia-os antes de trabalhar em qualquer módulo — eles são a
fonte da verdade sobre fidelidade.

`dicionario_de_dados.md` (raiz) é o **esquema das tabelas de evidência** da Fase
3 — o que cada coluna de `results/*.csv` significa, quem produziu o número e
quais leituras estão erradas. Consulte antes de escrever qualquer análise sobre
os CSVs: várias colunas são ordem-dependentes ou de sujeitos diferentes, e o
documento existe porque isso não é legível no dado sozinho.

`bugs_originais.md` (raiz) cataloga os **defeitos herdados do Java**, com
evidência e citação de linha: os patches `#1`–`#8` já conhecidos e os achados
`C1`–`C7` da árvore de comparadores. Consulte-o antes de "corrigir" um
comportamento estranho do original — vários são deliberados, e os que não são só
devem ser corrigidos **depois** da equivalência estar demonstrada. Um porte que
melhora o original não pode ser validado contra ele.

## Project layout

Layout `src/` (pacote `uschema`). Cada subpacote mapeia uma fase do roadmap:

- `src/uschema/metamodel/` — **Fase 0.1/0.2**: PyEcore sobre `resources/uschema.ecore`; round-trip de XMI.
- `src/uschema/naming/` — **Fase 0.6**: Inflector fiel ao Java (capitalização/pluralização dos nomes de entidade).
- `src/uschema/validation/` — **Fase 0.3**: harness de equivalência estrutural, espelhando `USchemaCompareMain`.
- `src/uschema/intermediate/` — **Fase 1.1**: modelo `raw` (Composite) + `metadata`, como `dataclasses`. O pacote `firsto` do Java **não** foi portado: é código morto (nenhuma referência fora do próprio pacote).
- `src/uschema/inference/` — **Fase 1.2–1.7**: núcleo `doc2uschema` (`schema_inference`, `strategies`, `builder`), o `m2m/USchemaToDocumentDb` (1.4b) e a fachada `build_uschema` (1.7).
- `src/uschema/extractors/` — **Fase 2**: extratores por **driver nativo** (`mongo` via `pymongo`, `neo4j` via `neo4j`; PySpark ficou como paralelizador futuro, ver 2.0) + `triple` (o contrato de costura, **Fase 1.0**) + `neo4j_model` (núcleo de construção **próprio do Neo4j**, ver a nota abaixo).

Fora do pacote: `resources/` (`.ecore` + XMIs de referência + o dataset
Northwind em `datasets/`), `oracle/` (Dockerfile + `patches/`), `scripts/`
(baterias de equivalência e volume + geradores + `output.py`, que grava as
tabelas), `results/` (os CSVs medidos, fora do git), `out/` (XMIs gerados, fora
do git), `tests/` (`unit/`, `regression/`, `datasets/`).

**Estado real do pacote** (mantenha esta lista honesta ao avançar): **Fases 0 a 3
fechadas** — todos os subpacotes acima estão implementados e cobertos por
teste; **não** há stubs com `NotImplementedError`. A **3.4** (análise e redação)
concluiu em 17/09/2026, com a prévia do TC qualificada: ver `todolist_fase3.md` e
`scripts/README.md`. Em aberto está a **Fase 4** — backend Spark opcional para os
extratores, para separar o que na diferença de crescimento contra o oráculo é
arquitetura e o que é algoritmo (`fase4_spark.md`, `todolist_fase4.md`).

Uma ausência real, para não ser confundida com lacuna de porte:

- **`cli.py` + `[project.scripts]` não existem.** Estavam previstos para a 1.7,
  que fechou sem eles; hoje o pipeline é chamado por API
  (`inference.build_uschema.BuildUSchema`) e pelos scripts de `scripts/`. A
  Fase 3 **decidiu não criar** o CLI: as baterias são um script por bateria,
  com `argparse`. Dívida em aberto, sem fase atribuída.

Implemente bottom-up, test-alongside (ver `fase1_nucleo_inferencia.md`).

## Tooling

This project uses **uv** as the package and project manager. Always use uv for
dependency operations — never edit `pyproject.toml` dependencies by hand, and
never invoke `pip` directly.

Comandos comuns:

- `uv sync` — instala/atualiza tudo a partir do lockfile
- `uv add <package>` / `uv add --dev <package>` — adiciona dep de runtime / dev
- `uv run <command>` — roda dentro do virtualenv do projeto
- `uv run pytest` — suíte de testes
- `uv run ruff check .` — lint · `uv run ruff format .` — formata
- `uv run mypy .` — type-check

## Key dependencies

- **pyecore** — metamodelo/serialização: carrega `uschema.ecore` e lê/grava XMI (substitui Factory/Package/Switch do EMF via API reflexiva). **Não distribui `py.typed`** — ver o aviso sobre `mypy` em *Coding conventions*.
- **pyspark** — dependência declarada, **hoje não usada em runtime**. A Fase 2.0 decidiu ler por driver nativo: nenhum conector oficial (Mongo/Neo4j) expõe mais a API RDD que o oráculo usa — viraram DataFrame-only. O `reduce_pairs`/`build_triples` combina por `(min, max, soma)`, comutativo e associativo, então `mapPartitions` entra depois sem reescrever nada. **Fixa o teto de Python em 3.12.**
- **pymongo** — leitura de `dict`/`bson` do MongoDB (o `_id` é lido genericamente — bug #6).
- **neo4j** — driver do paradigma grafo (`GraphDatabase.driver(...).execute_query(...)`; o conector Spark não é usado — ver acima).
- **pydantic** — validação/esquemas de configuração.
- **loguru** — logging estruturado.

## Coding conventions

- **Type hints obrigatórios** em toda assinatura. `mypy` roda estrito; código novo passa sem `# type: ignore` salvo necessidade real.

> **`mypy` não protege nada que atravesse a fronteira do PyEcore.** A lib não
> distribui `py.typed`, então `EObject` é `Any`: qualquer atributo "existe", com
> qualquer tipo, e casa com qualquer assinatura. Na Fase 0.3, **seis** erros
> passaram por `mypy --strict` e só apareceram em execução — três typos
> (`f1.optionl`, `r1.opoosite`, `r2.refTo`), um acesso a campo inexistente
> (`variation.name`; quem tem `name` é o `SchemaType`), um `EObject` passado onde
> se pedia `Iterable[EObject]`, e uma leitura de `.optional` içada para fora do
> braço do `match` (estoura `AttributeError` em `Key`, que não tem o campo).
>
> Consequências práticas: (1) **anote o local** ao ler do PyEcore
> (`nome: str = obj.name`) — não pega typo, mas documenta a intenção e faz o
> mypy checar o uso a jusante; (2) **todo acesso a campo precisa ser exercitado
> por teste ao menos uma vez** — inclusive os que só rodam no caminho de erro,
> como as f-strings de mensagem de diagnóstico, que é onde eles se escondem; (3)
> use `obj.eClass.name` (string) para saber o tipo concreto, nunca `isinstance`.

- **Docstrings em estilo NumPy** (`Parameters`/`Returns`/`Raises`/`Examples`), não Google/Sphinx.
- **Linha de 100** colunas (ruff).
- **Imports ordenados pelo ruff** (isort). Não reordene à mão; rode `uv run ruff check --fix .`.
- **Composição sobre herança** para código não-trivial; funções puras para transformações, classes só para estado real ou design por protocolo.
- **Nomes**: snake_case (funções/variáveis), PascalCase (classes), UPPER_SNAKE (constantes).

### Docstring example (NumPy style)

```python
def infer(self, triples: list[SchemaTriple]) -> dict[str, list[SchemaComponent]]:
    """Inferir as árvores raw por entidade a partir das triplas.

    Parameters
    ----------
    triples : list of SchemaTriple
        Saída dos extratores (Fase 2), no contrato compartilhado.

    Returns
    -------
    dict of str to list of SchemaComponent
        Variações estruturais por nome de entidade.
    """
```

## Testing

- Testes em `tests/`, organizados por tipo: `unit/`, `regression/` (JUnit portado), `datasets/` (golden-master).
- Arquivos `test_*.py`; funções `test_*`. Rode com `uv run pytest` (`-x` para first-failure, `-k` para filtrar, `-vv` verboso).
- Prefira `@pytest.mark.parametrize` a loops e fixtures a setup/teardown.
- **Ordem de validação (do roadmap):** regressão portada (localiza o módulo) → golden-master de dataset → harness de equivalência contra o oráculo.

## Quality gates (pre-commit + CI)

Convenções de qualidade são impostas automaticamente, não só documentadas, em
três camadas (`.pre-commit-config.yaml` + `.github/workflows/ci.yml`):

| Camada | O que roda | Quando |
| --- | --- | --- |
| **pre-commit** | higiene (whitespace, EOF, TOML/YAML, merge conflicts) + `ruff check --fix` + `ruff format` + `mypy` + testes **rápidos** (`pytest -m "not spark and not integration"`) | todo commit |
| **pre-push** | suíte **completa** (`pytest`, inclui os testes de Spark) | todo push |
| **CI** | `ruff check` + `ruff format --check` + `mypy` + `pytest` em Python 3.12 | todo push/PR |

`ruff`/`mypy`/`pytest` rodam via `uv run` (versões do lockfile). CI é a porta
real — hooks locais são puláveis com `--no-verify`.

Instale os hooks uma vez (ativa os dois tipos):

```bash
uv run pre-commit install --hook-type pre-commit --hook-type pre-push
```

`uv run pre-commit run --all-files` roda tudo manualmente.

**Markers do pytest** (registrados no `pyproject.toml`, `--strict-markers`
ativo): marque cada teste como `unit`, `spark` ou `integration`. Só os `unit`
(rápidos, puros) rodam no pre-commit; `spark`/`integration` ficam para pre-push
e CI. Hoje **toda a suíte é `unit`** — os extratores da Fase 2 são Python
(driver nativo), então nenhum teste é `spark`. Se o Spark entrar como
paralelizador, os testes dele levam `@pytest.mark.spark`.

Não relaxe essas checagens para "passar o commit" — corrija o código. A
tolerância ao "nenhum teste coletado" (exit 5) que existia enquanto a suíte
estava vazia **foi removida** do CI e dos hooks — uma suíte ausente ou não
coletada agora derruba o gate, que é o ponto.

### Fluxo de PR (branch `main` protegida)

`main` é protegida no GitHub — **não se commita direto nela** (o dono pode dar
bypass em emergência; os demais, não). Todo trabalho vai por **pull request**:

- 1 review aprovado (aprovação obsoleta é descartada em novo push);
- status check `quality` (o CI) verde e branch atualizada com a `main`;
- conversas resolvidas (inclui as threads do CodeRabbit); histórico linear
  (sem merge commits — use squash/rebase); sem force-push.

Merge é **por squash ou rebase** (merge-commit desligado por causa do histórico
linear); a branch é apagada automaticamente após o merge.

**CodeRabbit** (`.coderabbit.yaml`) revisa cada PR com as regras de fidelidade do
projeto. **SonarQube Cloud** (`sonar-project.properties`, job `sonar` no CI) roda
análise estática + cobertura e posta o quality gate como check obrigatório do PR.
**Mergify** (`.mergify.yml`) faz o merge automático por fila quando as condições
batem (1 review + checks `quality`/Sonar verdes + conversas resolvidas), sempre
por squash. Os apps precisam estar instalados no repo
(<https://github.com/apps/coderabbitai> · <https://github.com/apps/mergify> ·
SonarQube Cloud via <https://sonarcloud.io>).

## Adding a new dependency

1. `uv add <package>` (runtime) ou `uv add --dev <package>` (tooling/testes).
2. Cite a adição no commit/PR — para quê e por quê.
3. Atualize **Key dependencies** se for uma dep de runtime significativa.

## What to avoid

- Não introduza outros formatadores/linters (black, flake8, pylint, isort) — o ruff cobre tudo.
- Não fixe versões exatas no `pyproject.toml` sem incompatibilidade conhecida; o lockfile cuida da reprodutibilidade.
- **Não subir para Python 3.14** enquanto o PySpark não suportar — o projeto está pinado em 3.12 por isso.
- Não commite `.venv/`, `__pycache__/` nem caches.

## Project-specific notes

- **Determinismo é load-bearing.** Ordenação de campos, `__eq__`/`__hash__` estrutural e ordem das variações têm de casar com o Java — divergência aqui quebra a equivalência com o oráculo. Cubra com testes desde já.
- **`ArraySC.__eq__` ignora o tamanho do array** (decisão deliberada do autor, origem do bug #8) — o colapso de variações em `SchemaInference.java:207-211` não chama `combineMetadata`; o `meta` inteiro (count+timestamps) da ocorrência descartada some. **Não adicionar `combine_metadata` nesse ponto do porte** — ver `bugs_originais.md` #8.
- **Bugs corrigidos por construção** (o original corrigia por patch): **#6** `_id` genérico (não assumir `ObjectId`), **#7** array vazio (checar `len==0` antes de `inners[0]`). Onde um teste JUnit codificava o bug, afirme o valor **corrigido**. **#8 não entra nessa lista** — é replicado fielmente.
- **O Inflector é reimplementação, não lib.** Nenhuma lib Python (`inflection`, `inflect`) reproduz o Inflector do ModeShape, que o Java versiona no próprio fonte: as regras são uma lista **ordenada** com semântica de inserção-na-frente, e a saída depende dessa ordem (`pluralize("human")` → `"humen"`). Trocar por lib renomearia `EntityType` e quebraria a equivalência. Não reintroduza a dependência.
- **O `abstractjson` (Bridge Jackson/Gson) desaparece por construção** (Fase 1.5). As ~25 classes de `util/abstractjson/` existem só para abstrair duas libs de JSON do Java; a entrada do porte já é `dict`/`list` nativo, então não há o que abstrair. A **única** decisão de tipo do Bridge é o `IAJIdentify` (7 predicados), e o único predicado que o JSON nativo não distingue sozinho — `isObjectId` — está resolvido na 1.0 (`extractors/triple.py::classify`, sentinela `value == "oid"`). Remoção **estrutural** deliberada, sem perda de comportamento observável — não reintroduzir "por completude". Detalhe e verificação em `bugs_originais.md` ("O que não é defeito").
- **O Neo4j não passa pelo núcleo da Fase 1** (achado da 2.2, corrige a spec). `Neo4j2USchema.process` chama `Json2USchemaModel.processArchetypes`, que usa quatro classes próprias do `neo4j2uschema` — um **segundo** núcleo de construção de `USchema`. Portado fiel em `extractors/neo4j_model.py`: o grafo **não** produz `SchemaTriple` nem chama `BuildUSchema`. Os dois paradigmas convergem no metamodelo PyEcore, não no código de inferência. Encaixar o Neo4j no `BuildUSchema` "por unificação" quebraria a equivalência.
- **Há dois extratores MongoDB no original, e o certo é o `Helpers`** (achado da 2.1). `mongodb2uschema` (caminho `Helpers`) produz a tripla e alimenta o nosso núcleo — é o que gera os XMIs de referência. `mongodb2uschema.spark` (`ArchetypeMapping`/`ModelDirector`) tem construtor próprio e **não** foi portado: duplicaria a Fase 1.
- **Fora de escopo** (não portar): backends `cassandra`/`hbase`/`redis`/`sql`; OCL (ausente no metamodelo); codegen EMF; editor Sirius (UI); `mongodb2uschema.spark`/`ModelDirector` (acima). A **metacamada** é trabalho futuro.
- **Paralelismo do time:** a Fase 1 (núcleo de inferência) e a Fase 2 (extratores) avançam em paralelo e se encontram no formato da tripla (`extractors/triple.py`); o trabalho é compartilhado entre os autores, sem dono fixo por fase.

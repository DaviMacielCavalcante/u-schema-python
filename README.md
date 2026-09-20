# u-schema-python

A Python reimplementation of the **U-Schema** schema inference pipeline —
extraction, inference and serialization — for the **document** (MongoDB) and
**graph** (Neo4j) paradigms.

The metamodel is handled by [PyEcore](https://github.com/pyecore/pyecore), which
loads `resources/uschema.ecore` and reads/writes XMI through its reflective API.
There is **no Eclipse dependency**: no EMF codegen, no Sirius editor, no OSGi
bundle resolution. The pipeline runs as an ordinary Python package.

The reference implementation is the Java one published by
[ModelUM](https://github.com/modelum): `modelum/uschema` and
`modelum/uschema-inference`. Both are pinned by commit and built inside a
container (`oracle/`) that serves as the **oracle** for this port:

| Upstream repository | Commit |
| --- | --- |
| `modelum/uschema` | `6dfd6b4a6c04c67e49a80fb6cb6da9dd0f0f0f8c` |
| `modelum/uschema-inference` | `0f8f58c31f7661ce9be7333a1f34b9a05321a993` |

Those commits are pinned in `oracle/Dockerfile`, together with five patches that
make the Java sources build and run outside Eclipse. The oracle exists solely to
produce reference XMI files; it is not part of the deliverable.

Fidelity here means **structural equivalence** of behaviour — same entities,
variations, attributes, aggregates, references and counts — not byte-identical
XMI. Known defects of the original implementation are reproduced deliberately;
they are catalogued, with line-level evidence, in `bugs_originais.md`.

> **Documentation language.** This README is in English. The design documents,
> roadmap and data dictionary (`roadmap_portabilidade.md`, `fase0`…`fase3`,
> `dicionario_de_dados.md`, `bugs_originais.md`, the `todolist_*.md` files and
> the per-directory `README.md` files) are written in **Portuguese**, as the
> project is the subject of an undergraduate thesis.

## Requirements

| Component | Version | Notes |
| --- | --- | --- |
| Python | **3.12** | `requires-python = ">=3.12"`; the upper bound is fixed at 3.12 because PySpark is a declared dependency |
| [uv](https://docs.astral.sh/uv/) | any recent release | the only supported package/project manager for this repository |
| MongoDB | 8.0.31 | version used in the canonical run; `localhost:27017`, no authentication |
| Neo4j | 2026.07.1 Community | version used in the canonical run; `bolt://localhost:7687`, no authentication |
| Docker | any recent engine | **only** needed to build and run the Java oracle; the base image is digest-pinned in `oracle/Dockerfile` |
| JDK | **17 or 21** | needed by the `spark`-marked tests (Phase 4); `JAVA_HOME` must point at it — see below |

<!-- TODO: the minimum supported MongoDB and Neo4j versions have never been
     determined. The versions above are the ones the canonical battery was
     measured on, not a tested floor. -->

Neither database is required to run the test suite, and neither is required for
the file-based Northwind equivalence path — the 17 JSONL files are versioned
under `resources/datasets/northwind/`.

## Installation

```bash
git clone https://github.com/DaviMacielCavalcante/u-schema-python.git
cd u-schema-python
uv sync
```

`uv sync` installs runtime and development dependencies from `uv.lock`. Never
edit `pyproject.toml` dependencies by hand and never invoke `pip` directly; use
`uv add` / `uv add --dev`.

Run the test suite and the static checks with:

```bash
uv run pytest
uv run ruff check .
uv run mypy .
```

### `JAVA_HOME` for the Spark-backed tests

`uv run pytest` runs the whole suite, which since Phase 4 includes tests marked
`spark`. Those boot a real JVM, and **PySpark 4.x requires Java 17 or 21** — it
does not run on Java 8, and newer JDKs are not supported yet. If your default
`java` is an older one (check with `java -version`), point `JAVA_HOME` at a
compatible JDK before running them:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64   # adjust to your install
```

Without it the `spark` tests fail with a message naming this requirement. To run
everything else in the meantime:

```bash
uv run pytest -m "not spark"
```

The pre-commit hook already excludes them; the pre-push hook and CI do not, so
the variable has to be set in the environment those run in.

Quality gates are enforced by pre-commit hooks and by CI. Install the hooks once:

```bash
uv run pre-commit install --hook-type pre-commit --hook-type pre-push
```

## Usage

There is **no command-line entry point**. The package has no `cli.py` and
declares no `[project.scripts]`; the pipeline is invoked through its Python API,
and the measurement batteries under `scripts/` are individual `argparse`
scripts. This is an acknowledged gap, not an oversight of this README.

Document paradigm (MongoDB) — extraction produces schema triples, which the
Phase 1 inference core turns into a `USchema` model:

```python
from pymongo import MongoClient

from uschema.extractors.mongo import extract_database_triples
from uschema.extractors.triple import triples_from_rows
from uschema.inference.build_uschema import BuildUSchema
from uschema.metamodel.registry import load_metamodel
from uschema.metamodel.xmi import save_model

pkg = load_metamodel()

client = MongoClient("mongodb://localhost:27017")
database = client["northwind"]
collections = sorted(database.list_collection_names())

rows = extract_database_triples(database, collections)
model = BuildUSchema(pkg).build_from_rows("northwind", triples_from_rows(rows))

save_model(model, "out/porte/mongo_northwind_database.xmi")
```

See `scripts/run_northwind.py` for the same call chain with timing, comparison
against the reference XMI, and both read paths (files and cursor).

Graph paradigm (Neo4j) — the graph **does not** go through the Phase 1 inference
core. The original Java implementation has a second, independent model-building
core inside `neo4j2uschema`, ported faithfully in
`src/uschema/extractors/neo4j_model.py`. The two paradigms converge at the
PyEcore metamodel, not at the inference code:

```python
from neo4j import GraphDatabase

from uschema.extractors.neo4j import extract_database_archetype_counts
from uschema.extractors.neo4j_model import build_uschema_from_archetypes
from uschema.metamodel.registry import load_metamodel
from uschema.metamodel.xmi import save_model

pkg = load_metamodel()

driver = GraphDatabase.driver("bolt://localhost:7687")
rows = extract_database_archetype_counts(driver)

model = build_uschema_from_archetypes(pkg, "movies_min", rows)
save_model(model, "out/porte/neo4j_movies_min.xmi")
```

## Reproducing the measurement battery

The full suite chains five batteries sequentially — Northwind equivalence, the
oracle chain for both paradigms, and the size battery for both paradigms —
writing a fresh log to `logs/` and appending to the CSV tables in `results/`.

Prerequisites: `mongod` and `neo4j` running, and the oracle image built.

```bash
systemctl is-active mongod neo4j
docker build -t extrator-uschema oracle/     # once

git checkout de33a53
uv sync
./scripts/run_suite.sh                       # no argument = seed 23
```

**Seed.** With no argument the suite passes no `--seed` and the default applies:
`DEFAULT_SEED = 23`, defined in `scripts/output.py`. All four battery scripts
import it from there, so the seed is not duplicated in the shell wrapper. Pass a
different seed positionally (`./scripts/run_suite.sh 69`).

The seed matters: without it, every run of the dataset generators produces a
different instance — same totals, different split across structural variations —
and the equivalence comparison against the reference XMI files reports spurious
`count` divergences.

**The suite is destructive.** It empties the `up_*` MongoDB databases and the
Neo4j graph and regenerates each from the seed. The `northwind` database is never
touched, because it is real versioned data and is not seed-reproducible.

The suite **refuses to re-run a seed already present** in `results/runs.csv`
(exit code 3): the batteries append, so repeating a seed would silently duplicate
rows rather than overwrite them. Archive the directory before repeating:

```bash
mv results results_$(date +%d-%m)
```

Running the whole suite takes roughly 25 minutes on the default seed. Individual
batteries, and what each one measures, are documented in `scripts/README.md`.

## The canonical battery — 2026-08-27

**The numbers published in the article come from one specific run: the suite
executed on 2026-08-27, 11:10:58 → 12:07:49 (-03:00).** That run, and only that
run, is versioned in this repository. Any other execution present on a local
disk is deliberately excluded.

This matters for anyone checking the article against this repository. The
batteries append to the CSV tables and are re-runnable by design, so a
repository containing several runs would leave the reader unable to tell which
rows the published tables were computed from. `.gitignore` therefore whitelists
this battery **file by file** — not by directory and not by glob — so that a
future run, even on the same seed, cannot enter a commit on its own.

Provenance of the canonical battery:

| Item | Value |
| --- | --- |
| Port commit | `de33a53` |
| Seed | `23` (the suite ran with no `--seed`) |
| Northwind dataset | `sha256:3700157bd0bcca944b5a869dbfdd2065ae180bc538ec1e816a94e0f86c7271ae` |
| MongoDB | 8.0.31 |
| Neo4j | 2026.07.1 Community |
| Spark | 3.0.1 — inside the oracle container only; the port does not use Spark at runtime |
| JDK | Eclipse Temurin 8 (`maven:3.9-eclipse-temurin-8`, digest pinned) |
| Oracle image | `extrator-uschema:latest`, `sha256:ca377aee…f126f7`, built 2026-08-01 |
| Python | 3.12.3 |

The full provenance record, including why the measured code predates the commit
that carries it, is in `results/README.md`.

## Versioned artifacts

Three directories hold the canonical battery — 39 artifacts in total. `results/`
and `out/` are otherwise ignored by git.

| Path | Contents |
| --- | --- |
| `results/runs.csv` | one row per **port** run: extraction, inference, write and query times, plus the normalized time (26 rows) |
| `results/oracle.csv` | one row per **Java oracle** run: container wall-clock time only (12 rows) |
| `results/comparisons.csv` | one equivalence verdict per (subject × reference) pair (22 rows) |
| `results/divergences.csv` | one row per individual divergence, with category and message (129 rows) |
| `results/README.md` | provenance, reproduction instructions, anonymization record and limitations of this battery |
| `out/oraculo/*.xmi` | 12 XMI models produced by the Java oracle over our seeded data |
| `out/porte/*.xmi` | 22 XMI models produced by the Python port |
| `logs/baterias_20260827_111058.log` | raw suite output, **anonymized** (see below) |

Reference XMI files produced by the original U-Schema authors over *their*
dataset live in `resources/` and are immutable. The three producers are kept in
three separate directories on purpose: a reference XMI and a port-generated XMI
are indistinguishable by content, and confusing them invalidates any comparison.
The convention is documented in `resources/README.md`.

**Do not read the CSV tables without `dicionario_de_dados.md`.** It records what
each column means, which producer generated each number, and which readings are
wrong — several columns are order-dependent or describe different subjects, and
that is not legible from the data alone.

## Limitation: Neo4j port XMI files in this battery

**`out/porte/neo4j_*_seed23.xmi` come from the size battery, not from the oracle
chain.**

`run_oracle_neo4j.py` and `run_size_neo4j.py` wrote to the same path,
`out/porte/neo4j_{schema}_seed{seed}.xmi`. Both run within the same suite and the
size battery runs later, so it overwrote the file the oracle chain had left
behind. The file mtimes of this battery show it: the four `out/oraculo/neo4j_*.xmi`
files date from 11:18–11:34 (the `run_oracle_neo4j` window, 11:17:37–11:34:42),
while the four `out/porte/neo4j_*.xmi` files date from 11:38–12:07 (the
`run_size_neo4j` window, 11:36:43–12:07:49).

**The equivalence verdict is not affected.** Both scripts compare the
freshly-built in-memory `USchema` object against the reference; neither re-reads
the file it has just written. `comparisons.csv` is correct. What was wrong is the
file left on disk, whose name implied a provenance it did not have.

Consequently, these four files must **not** be used as the "port XMI" behind the
`oracle_chain-neo4j-*` rows of `comparisons.csv`. To obtain those, run
`run_oracle_neo4j.py` on its own.

The name collision is **fixed** in `scripts/run_size_neo4j.py`, which now writes
`neo4j_{schema}.xmi` without the seed suffix, matching what `run_size_mongo.py`
already did. The fix applies to future runs: **the artifacts in this repository
were not regenerated** and remain as described here.

## Log anonymization

`logs/baterias_20260827_111058.log` is **not literal raw output**. Three textual
substitutions were applied before versioning, all of them to machine identity,
none to a measured number:

| Original | Placeholder | Occurrences |
| --- | --- | --- |
| the machine hostname | `<HOSTNAME>` | 12 |
| the user's *home* directory, prefix of the repository path | `<HOME>` | 1 |
| the machine's LAN IP address | `<LAN_IP>` | 498 |

All three occur in Spark infrastructure log lines (`WARN Utils: Your hostname…
resolves to a loopback address`, and the `TID … on <LAN_IP> (executor driver)`
lines), plus the Northwind `dataset:` line.

Verified after substitution: the line count is unchanged (2916); every modified
line contains one of the three placeholders; and the 28
`Job N finished: …, took X s` lines — the source of the Spark job-time figure —
are byte-for-byte identical to the original (`md5` of the set:
`64a69a2260d782cb2a940b9dcb4ebc64`). None of them contained a hostname, path or
IP address.

One residual detail: the `<HOME>` line still contains the repository's **former**
directory name, `arco-do-tc-2`, since the repository was renamed to
`u-schema-python` after this battery ran. The log is measurement evidence and was
not edited afterwards — editing it would invalidate the line-count and checksum
verification above for a purely cosmetic gain.

No file in the battery contains a credential, token, key or connection URI.
MongoDB and Neo4j ran on `localhost` without authentication, so the connection
strings hold no secret; the XMI files contain only the model (entity names,
attributes, counts) and the CSV tables only run identifiers and numbers.

## Repository layout

```text
src/uschema/
  metamodel/     PyEcore over resources/uschema.ecore; XMI round-trip
  naming/        Inflector, reimplemented faithfully from the Java source
  validation/    structural equivalence harness (mirrors USchemaCompareMain)
  intermediate/  raw Composite model + metadata, as dataclasses
  inference/     doc2uschema core, m2m, and the build_uschema façade
  extractors/    native-driver extractors (mongo, neo4j), the triple contract,
                 and neo4j_model — the graph paradigm's own building core
resources/       .ecore, reference XMI files, versioned Northwind dataset
oracle/          Dockerfile + patches for the Java oracle
scripts/         measurement batteries, dataset generators, table writers
results/ out/ logs/   canonical battery artifacts (see above)
tests/           unit/, regression/ (ported JUnit), datasets/ (golden master)
```

Out of scope, and not ported: the `cassandra` / `hbase` / `redis` / `sql`
backends, OCL, EMF codegen, the Sirius editor, and the
`mongodb2uschema.spark` extractor (the `Helpers` path is the one that produces
the reference XMI files). The metalayer is future work.

## License

MIT — see `LICENSE`. Copyright (c) 2026 Davi Cavalcante.

The versioned Northwind dataset under `resources/datasets/northwind/` carries its
own license (BSD 2-Clause), included alongside the data.

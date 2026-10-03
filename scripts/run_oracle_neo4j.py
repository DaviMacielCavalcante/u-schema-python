"""Bateria porte x oráculo semeado, paradigma grafo (Fase 3.1).

Por tamanho: limpa, regera o grafo com semente fixa, roda o **porte** (driver
nativo -> núcleo próprio do Neo4j) e o **oráculo Java em Docker** sobre a
**mesma instância**, e compara os dois XMIs. Também compara contra o XMI de
`resources/neo4j/`, que veio de uma instância de semente desconhecida — a
diferença entre as duas comparações é o resultado que a fase quer registrar.

Isto é o padrão-ouro da equivalência do grafo: mesma entrada, duas implementações.
A comparação contra `resources/` acusa divergências não-fatais de `count` que
são ruído de amostragem do gerador, não defeito do porte (`todolist_fase3.md`
§3.1).

Destrutiva: cada tamanho apaga o grafo anterior, e o Community tem um banco só,
então os tamanhos não coexistem nem podem ser paralelizados. Exige a imagem
`extrator-uschema` buildada (`docker build -t extrator-uschema oracle/`).

    uv run python scripts/run_oracle_neo4j.py --seed 23
    uv run python scripts/run_oracle_neo4j.py --seed 23 --sizes larger

Engine do porte (Fase 4.3): ``--engine spark`` extrai pela engine Spark, com
``--slices`` fatias por combinação de labels (padrão: uma por núcleo). A sessão
abre uma vez, antes do laço, para que o boot da JVM fique fora do tempo de
extração. Até a 4.4 criar a coluna ``engine`` nos CSVs, a corrida Spark **não**
grava em ``results/`` — ficaria indistinguível da Python, com o mesmo
``run_id``. Passe outro ``--output-dir``:

    uv run python scripts/run_oracle_neo4j.py --seed 23 --engine spark --output-dir out/gate_4_3
"""

import argparse
import subprocess
import sys
import time
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import TYPE_CHECKING, Any

from neo4j import GraphDatabase
from pyecore.ecore import EPackage

from baseline import neo4j_query_time
from output import (
    DEFAULT_SEED,
    PORT,
    RESOURCES,
    SEEDED_ORACLE,
    Results,
    format_query_time,
    format_seconds,
    fraction,
    modeled_counts,
    normalized,
    run_id,
)
from runs import Neo4jOracleRun
from uschema.extractors.neo4j import (
    extract_archetype_counts_from_uri,
)
from uschema.extractors.neo4j_model import build_uschema_from_archetypes
from uschema.metamodel.registry import load_metamodel
from uschema.metamodel.xmi import load_model, save_model
from uschema.validation.equivalence import compare

if TYPE_CHECKING:
    # Só para anotação: a engine Python não pode carregar o pyspark.
    from pyspark.sql import SparkSession

ENGINES = ["python", "spark"]

SCHEMA_BY_SIZE = {
    "small": "movies_min",
    "medium": "up_medium",
    "large": "up_large",
    "larger": "up_larger",
}

DEFAULT_SIZES = ["small", "medium", "large", "larger"]

LABELS = ("User", "Movie")

ROOT = Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "scripts" / "gen_userprofiles_neo4j.py"
CLEANER = ROOT / "scripts" / "clean_databases.py"
PORT_OUTPUT = ROOT / "out" / "porte"
ORACLE_OUTPUT = ROOT / "out" / "oraculo"
IMAGE = "extrator-uschema"


def generate(size: str, uri: str, seed: int) -> tuple[float, float]:
    """Limpa o grafo anterior e regera o do tamanho pedido.

    Returns
    -------
    tuple of (float, float)
        Tempo de limpeza e tempo de geração.
    """
    start = time.perf_counter()

    subprocess.run(
        [sys.executable, str(CLEANER), "--only", "neo4j", "--uri-neo4j", uri],
        check=True,
    )

    t_cleanup = time.perf_counter() - start

    start = time.perf_counter()

    subprocess.run(
        [sys.executable, str(GENERATOR), "--size", size, "--uri", uri, "--seed", str(seed)],
        check=True,
    )

    return t_cleanup, time.perf_counter() - start


def run_oracle(schema: str, seed: int) -> tuple[float, Path]:
    """Roda o extrator Java no container sobre o grafo já materializado.

    O `--db` é o nome do **schema**, não do banco a conectar: o
    `Neo4j2USchema.java` só o repassa para `Json2USchemaModel`, e o conector lê
    sempre o banco padrão (`oracle/README.md`). Ele precisa casar com o nome que
    o porte usa — divergência de `SCHEMA_NAME` é fatal no harness.

    Parameters
    ----------
    schema : str
        Nome do schema no modelo, e do arquivo de saída (`movies_min`, ...).
    seed : int
        Semente da instância medida; entra no nome do XMI preservado.

    Returns
    -------
    tuple of (float, Path)
        Tempo de parede do container e o XMI já renomeado por semente.
    """
    ORACLE_OUTPUT.mkdir(parents=True, exist_ok=True)

    start = time.perf_counter()

    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network=host",
            "-v",
            f"{ORACLE_OUTPUT}:/output",
            IMAGE,
            "--db",
            schema,
            "--kind",
            "neo4j",
        ],
        check=True,
    )

    elapsed = time.perf_counter() - start

    # O container escreve como root; renomear só exige permissão no diretório,
    # que é nosso. A semente no nome evita que a próxima corrida sobrescreva a
    # evidência desta.
    written = ORACLE_OUTPUT / f"{schema}.xmi"
    target = ORACLE_OUTPUT / f"neo4j_{schema}_seed{seed}.xmi"
    written.replace(target)

    return elapsed, target


def sum_by_label(rows: list[dict[str, Any]], label: str) -> int:
    """Soma os counts dos arquétipos de nó com exatamente este label."""
    return sum(
        row["count"]
        for row in rows
        if row["archetype"]["entity"] == "node" and row["archetype"]["labels"] == [label]
    )


def measure(
    size: str,
    uri: str,
    seed: int,
    pkg: EPackage,
    *,
    spark: "SparkSession | None" = None,
    slices: int | None = None,
) -> Neo4jOracleRun:
    """Roda porte e oráculo sobre a mesma instância e compara os três XMIs.

    Com ``spark``, o porte extrai pela engine Spark (Fase 4.3); sem, pela Python
    de sempre. A sessão vem aberta de fora: o boot da JVM não entra no
    ``t_extraction``.
    """
    schema = SCHEMA_BY_SIZE[size]

    with GraphDatabase.driver(uri, auth=None) as driver:
        actual = {
            label: int(
                driver.execute_query(f"MATCH (n:{label}) RETURN count(n) AS total").records[0][
                    "total"
                ]
            )
            for label in LABELS
        }

        start = time.perf_counter()

        rows: list[dict[str, Any]] = extract_archetype_counts_from_uri(
            uri, auth=None, spark=spark, slices=slices
        )

        t_extraction = time.perf_counter() - start

    start = time.perf_counter()

    port = build_uschema_from_archetypes(pkg, schema, rows)

    t_inference = time.perf_counter() - start

    start = time.perf_counter()

    if spark is not None:
        save_model(port, PORT_OUTPUT / f"neo4j_{schema}_seed{seed}_spark.xmi")
    else:
        save_model(port, PORT_OUTPUT / f"neo4j_{schema}_seed{seed}.xmi")

    t_write = time.perf_counter() - start

    t_oracle, oracle_xmi = run_oracle(schema, seed)

    oracle = load_model(oracle_xmi, pkg)

    # A query de referência roda por último, depois do porte E do oráculo.
    with GraphDatabase.driver(uri, auth=None) as driver:
        t_query = neo4j_query_time(driver)

    in_port = modeled_counts(port)
    in_oracle = modeled_counts(oracle)

    return Neo4jOracleRun(
        size=size,
        schema=schema,
        t_extraction=t_extraction,
        t_inference=t_inference,
        t_write=t_write,
        t_oracle=t_oracle,
        t_query=t_query,
        counts={
            label: (actual[label], in_port.get(label, 0), in_oracle.get(label, 0))
            for label in LABELS
        },
        vs_oracle=compare(oracle, port),
        vs_resources=compare(load_model(ROOT / "resources" / "neo4j" / f"{schema}.xmi", pkg), port),
    )


def record(tables: Results, seed: int, run: Neo4jOracleRun) -> None:
    """Distribui a corrida pelas tabelas de resultado.

    O porte vai para `runs`, o oráculo para `oracle`. Grava **duas**
    comparações: o mesmo modelo do porte confrontado com o oráculo semeado e com
    o XMI publicado em `resources/`.
    """
    key = run_id("oracle_chain", "neo4j", run.schema, seed=seed)

    tables.add_run(
        {
            "run_id": key,
            "experiment": "oracle_chain",
            "size": run.size,
            "paradigm": "neo4j",
            "target": run.schema,
            "origin": "database",
            "total_time": format_seconds(run.total),
            "extraction_time": format_seconds(run.t_extraction),
            "inference_time": format_seconds(run.t_inference),
            "write_time": format_seconds(run.t_write),
            "query_time": format_query_time(run.t_query),
            "normalized": normalized(run.total, run.t_query),
        }
    )

    tables.add_oracle(key, run.t_oracle, run.t_query)

    tables.add_comparison(key, PORT, SEEDED_ORACLE, run.vs_oracle)
    tables.add_comparison(key, PORT, RESOURCES, run.vs_resources)


def main() -> None:
    """Roda a cadeia porte x oráculo nos tamanhos pedidos e acumula as tabelas."""
    ap = argparse.ArgumentParser(description="Porte x oráculo semeado, Neo4j (Fase 3.1)")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--uri", default="bolt://localhost:7687")
    ap.add_argument("--sizes", nargs="+", choices=list(SCHEMA_BY_SIZE), default=DEFAULT_SIZES)
    ap.add_argument("--output-dir", type=Path, default=ROOT / "results")
    ap.add_argument("--engine", choices=ENGINES, default="python")
    ap.add_argument("--slices", type=int, default=None)

    args = ap.parse_args()

    if args.engine == "spark" and args.output_dir.resolve() == (ROOT / "results").resolve():
        ap.error("--output-dir precisa ser outro diretório")

    pkg = load_metamodel()

    PORT_OUTPUT.mkdir(parents=True, exist_ok=True)

    context_engine: AbstractContextManager[SparkSession | None]

    if args.engine == "spark":
        from uschema.extractors.spark import local_session

        context_engine = local_session()
    else:
        context_engine = nullcontext()

    with Results(args.output_dir) as tables, context_engine as context:
        for size in args.sizes:
            print(f"\n=== seed {args.seed} | tamanho {size} | {args.engine} ===", flush=True)

            # Limpeza e geração são cronometradas só para o log: nenhuma das
            # duas é medida do porte nem do oráculo, e as duas saíram dos CSVs.
            t_cleanup, t_generation = generate(size, args.uri, args.seed)

            run = measure(size, args.uri, args.seed, pkg, spark=context, slices=args.slices)

            print(
                f"  limpeza={t_cleanup:.2f}s"
                f"  geracao={t_generation:.2f}s"
                f"  extracao={run.t_extraction:.2f}s"
                f"  inferencia={run.t_inference:.2f}s"
                f"  escrita={run.t_write:.2f}s"
                f"  porte={run.total:.2f}s"
                f"  oraculo={run.t_oracle:.2f}s"
                f"  query={run.t_query:.2f}s"
                f"  norm={normalized(run.total, run.t_query)}x",
                flush=True,
            )

            for entity, (actual, in_port, in_oracle) in run.counts.items():
                print(
                    f"    {entity}: real={actual}"
                    f"  porte={in_port} ({fraction(in_port, actual)})"
                    f"  oraculo={in_oracle} ({fraction(in_oracle, actual)})"
                )

            print(
                f"    vs oráculo semeado: equivalente={run.vs_oracle.equivalent}"
                f" divergências={len(run.vs_oracle.divergences)}\n"
                f"    vs resources/:      equivalente={run.vs_resources.equivalent}"
                f" divergências={len(run.vs_resources.divergences)}",
                flush=True,
            )

            record(tables, args.seed, run)


if __name__ == "__main__":
    main()

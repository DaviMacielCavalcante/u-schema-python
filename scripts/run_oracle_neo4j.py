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

Engine do porte (Fases 4.3 e 4.4): ``--engine spark`` extrai pela engine
Spark, com ``--slices`` fatias por combinação de labels (padrão: uma por
núcleo), e grava o boot da sessão em ``boot_time``. Uma corrida Spark por
processo — um tamanho por vez; ver `engines.py`:

    uv run python scripts/run_oracle_neo4j.py --seed 23 --engine spark --sizes small
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from engines import SINGLE_RUN, engine_session
from neo4j import GraphDatabase
from pyecore.ecore import EPackage

from baseline import neo4j_query_time
from output import (
    DEFAULT_SEED,
    ENGINES,
    ORACLE_XMI_DIR,
    PORT,
    PORT_XMI_DIR,
    PYTHON,
    RESOURCES,
    RESULTS_DIR,
    ROOT,
    SEEDED_ORACLE,
    SPARK,
    Results,
    format_boot,
    format_query_time,
    format_seconds,
    fraction,
    modeled_counts,
    normalized,
    run_id,
)
from runs import Neo4jOracleRun
from uschema.extractors.neo4j import extract_archetype_counts_from_uri
from uschema.extractors.neo4j_model import build_uschema_from_archetypes
from uschema.metamodel.registry import load_metamodel
from uschema.metamodel.xmi import load_model, save_model
from uschema.validation.equivalence import compare

SCHEMA_BY_SIZE = {
    "small": "movies_min",
    "medium": "up_medium",
    "large": "up_large",
    "larger": "up_larger",
}

DEFAULT_SIZES = ["small", "medium", "large", "larger"]

LABELS = ("User", "Movie")

GENERATOR = ROOT / "scripts" / "gen_userprofiles_neo4j.py"
CLEANER = ROOT / "scripts" / "clean_databases.py"
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


def run_oracle(schema: str, target: Path) -> tuple[float, Path]:
    """Roda o extrator Java no container sobre o grafo já materializado.

    O `--db` é o nome do **schema**, não do banco a conectar: o
    `Neo4j2USchema.java` só o repassa para `Json2USchemaModel`, e o conector lê
    sempre o banco padrão (`oracle/README.md`). Ele precisa casar com o nome que
    o porte usa — divergência de `SCHEMA_NAME` é fatal no harness.

    Parameters
    ----------
    schema : str
        Nome do schema no modelo, e do arquivo de saída (`movies_min`, ...).
    target : Path
        Onde preservar o XMI do oráculo — nomeado pelo ``run_id`` da corrida,
        para que a seguinte não sobrescreva a evidência desta.

    Returns
    -------
    tuple of (float, Path)
        Tempo de parede do container e o XMI já em ``target``.
    """
    ORACLE_XMI_DIR.mkdir(parents=True, exist_ok=True)

    start = time.perf_counter()

    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network=host",
            "-v",
            f"{ORACLE_XMI_DIR}:/output",
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
    # que é nosso.
    written = ORACLE_XMI_DIR / f"{schema}.xmi"
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
    pkg: EPackage,
    key: str,
    *,
    engine: str = PYTHON,
    slices: int | None = None,
) -> Neo4jOracleRun:
    """Roda porte e oráculo sobre a mesma instância e compara os três XMIs.

    A sessão Spark abre e fecha em volta da extração do porte: o boot sai
    cronometrado à parte, e a JVM do porte já caiu quando o container do oráculo
    sobe. Os dois XMIs gerados se chamam pelo ``key``.
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

    with engine_session(engine) as (spark, t_boot):
        start = time.perf_counter()

        rows: list[dict[str, Any]] = extract_archetype_counts_from_uri(
            uri, spark=spark, slices=slices
        )

        t_extraction = time.perf_counter() - start

    start = time.perf_counter()

    port = build_uschema_from_archetypes(pkg, schema, rows)

    t_inference = time.perf_counter() - start

    start = time.perf_counter()

    save_model(port, PORT_XMI_DIR / f"{key}.xmi")

    t_write = time.perf_counter() - start

    t_oracle, oracle_xmi = run_oracle(schema, ORACLE_XMI_DIR / f"{key}.xmi")

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
        engine=engine,
        t_boot=t_boot,
        t_oracle=t_oracle,
        t_query=t_query,
        counts={
            label: (actual[label], in_port.get(label, 0), in_oracle.get(label, 0))
            for label in LABELS
        },
        vs_oracle=compare(oracle, port),
        vs_resources=compare(load_model(ROOT / "resources" / "neo4j" / f"{schema}.xmi", pkg), port),
    )


def record(tables: Results, key: str, run: Neo4jOracleRun) -> None:
    """Distribui a corrida pelas tabelas de resultado.

    O porte vai para `runs`, o oráculo para `oracle`. Grava **duas**
    comparações: o mesmo modelo do porte confrontado com o oráculo semeado e com
    o XMI publicado em `resources/`.
    """
    tables.add_run(
        {
            "run_id": key,
            "experiment": "oracle_chain",
            "size": run.size,
            "paradigm": "neo4j",
            "target": run.schema,
            "origin": "database",
            "engine": run.engine,
            "total_time": format_seconds(run.total),
            "extraction_time": format_seconds(run.t_extraction),
            "inference_time": format_seconds(run.t_inference),
            "write_time": format_seconds(run.t_write),
            "boot_time": format_boot(run.t_boot),
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
    ap.add_argument("--output-dir", type=Path, default=RESULTS_DIR)
    ap.add_argument("--engine", choices=ENGINES, default=PYTHON)
    ap.add_argument("--slices", type=int, default=None)

    args = ap.parse_args()

    if args.engine == SPARK and len(args.sizes) > 1:
        ap.error(SINGLE_RUN)

    pkg = load_metamodel()

    PORT_XMI_DIR.mkdir(parents=True, exist_ok=True)

    with Results(args.output_dir) as tables:
        for size in args.sizes:
            print(f"\n=== seed {args.seed} | tamanho {size} | {args.engine} ===", flush=True)

            # Limpeza e geração são cronometradas só para o log: nenhuma das
            # duas é medida do porte nem do oráculo, e as duas saíram dos CSVs.
            t_cleanup, t_generation = generate(size, args.uri, args.seed)

            key = run_id(
                "oracle_chain", "neo4j", SCHEMA_BY_SIZE[size], seed=args.seed, engine=args.engine
            )

            run = measure(size, args.uri, pkg, key, engine=args.engine, slices=args.slices)

            print(
                f"  limpeza={t_cleanup:.2f}s"
                f"  geracao={t_generation:.2f}s"
                f"  boot={format_boot(run.t_boot) or '-'}"
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

            record(tables, key, run)


if __name__ == "__main__":
    main()

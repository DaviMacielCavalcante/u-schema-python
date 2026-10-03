"""Bateria por tamanho do paradigma documento (Fases 3.2 e 4.4).

Por rota (A/B) e tamanho: regera o banco com semente fixa, extrai as triplas
pelo driver nativo, alimenta o núcleo da Fase 1 e mede quanto da massa real
sobrevive ao bug #8. Grava nas tabelas de `scripts/output.py`.

Não há `compare()` aqui: não existe XMI-oráculo do User Profiles em documento,
então esta bateria não produz linha em `comparisons.csv`. O gate de equivalência
do paradigma é o Northwind (`run_northwind.py`); o confronto com o Java sobre
este mesmo dado é o `run_oracle_mongo.py`.

Engine do porte (Fase 4.4): ``--engine spark`` extrai pela engine Spark, com
``--slices`` fatias por coleção (padrão: uma por núcleo), e grava o boot da
sessão em ``boot_time``. Uma corrida Spark por processo — uma rota e um tamanho
por vez; ver `engines.py`.

Destrutiva: cada corrida dropa e regera o banco alvo. Não rode concorrente com
a bateria do Neo4j. Contexto em `todolist_fase3.md` §3.2 e §3.3.

    uv run python scripts/run_size_mongo.py --seed 23
    uv run python scripts/run_size_mongo.py --seed 69 --routes A --sizes small
    uv run python scripts/run_size_mongo.py --seed 23 --engine spark --routes A --sizes small
"""

import argparse
import subprocess
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from engines import SINGLE_RUN, engine_session
from pyecore.ecore import EPackage
from pymongo import MongoClient

from baseline import mongo_query_time
from output import (
    DEFAULT_SEED,
    ENGINES,
    PORT_XMI_DIR,
    PYTHON,
    RESULTS_DIR,
    ROOT,
    SPARK,
    Results,
    entity_name,
    format_boot,
    format_query_time,
    format_seconds,
    fraction,
    modeled_counts,
    normalized,
    run_id,
)
from runs import MongoSizeRun
from uschema.extractors.mongo import extract_triples
from uschema.extractors.triple import triples_from_rows
from uschema.inference.build_uschema import BuildUSchema
from uschema.metamodel.registry import load_metamodel
from uschema.metamodel.xmi import save_model

DEFAULT_SIZES = ["small", "medium", "large", "larger"]
DEFAULT_ROUTES = ["A", "B"]

GENERATOR = ROOT / "scripts" / "gen_userprofiles.py"


def database_name(route: str, size: str) -> str:
    """Monta o nome do banco no padrão já usado no repositório."""
    return f"up_{route.lower()}_{size}"


def generate(route: str, size: str, uri: str, seed: int) -> float:
    """Regera o banco da combinação pedida e devolve o tempo gasto."""
    start = time.perf_counter()

    subprocess.run(
        [
            sys.executable,
            str(GENERATOR),
            "--route",
            route,
            "--size",
            size,
            "--db",
            database_name(route, size),
            "--uri",
            uri,
            "--seed",
            str(seed),
            "--drop",
        ],
        check=True,
    )

    return time.perf_counter() - start


def measure(
    route: str,
    size: str,
    uri: str,
    pkg: EPackage,
    xmi: Path,
    *,
    engine: str = PYTHON,
    slices: int | None = None,
) -> MongoSizeRun:
    """Extrai as triplas, constrói o USchema e mede o alcance do #8.

    A sessão Spark abre e fecha em volta da extração, que é a única etapa que a
    usa: o boot sai cronometrado à parte, e a JVM não disputa CPU com a
    inferência, a escrita e a query de referência.
    """
    database = database_name(route, size)

    # Mapping, não dict: Database é invariante no parâmetro de tipo.
    client: MongoClient[Mapping[str, Any]] = MongoClient(uri)

    try:
        db = client[database]

        collections = sorted(db.list_collection_names())

        actual = {name: db[name].count_documents({}) for name in collections}

    finally:
        client.close()

    with engine_session(engine) as (spark, t_boot):
        start = time.perf_counter()

        rows: list[dict[str, Any]] = extract_triples(
            uri, database, collections, spark=spark, slices=slices
        )

        t_extraction = time.perf_counter() - start

    start = time.perf_counter()

    port = BuildUSchema(pkg).build_from_rows(database, triples_from_rows(rows))

    t_inference = time.perf_counter() - start

    start = time.perf_counter()

    save_model(port, xmi)

    t_write = time.perf_counter() - start

    # A query de referência roda por último, depois de tudo que é medido: antes
    # ela aqueceria o cache e aceleraria a extração. Ver `baseline.py`.
    client = MongoClient(uri)
    try:
        t_query = mongo_query_time(client[database])
    finally:
        client.close()

    in_model = modeled_counts(port)

    # Chaveado pelo nome do `EntityType` no XMI, não pelo da coleção — é o
    # vocabulário que `entities` compartilha com `divergences`.
    return MongoSizeRun(
        database=database,
        route=route,
        size=size,
        t_extraction=t_extraction,
        t_inference=t_inference,
        t_write=t_write,
        engine=engine,
        t_boot=t_boot,
        t_query=t_query,
        counts={
            entity_name(name): (actual[name], in_model.get(entity_name(name), 0))
            for name in collections
        },
    )


def record(tables: Results, key: str, run: MongoSizeRun) -> None:
    """Grava a linha de corrida.

    É a única tabela que esta bateria produz: não há XMI-oráculo do User
    Profiles em documento para confrontar, e a contagem por entidade vai para o
    log, não para CSV.
    """
    tables.add_run(
        {
            "run_id": key,
            "experiment": "size",
            "size": run.size,
            "paradigm": "mongodb",
            "route": run.route,
            "target": run.database,
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


def main() -> None:
    """Roda a bateria nas combinações pedidas e acumula as tabelas."""
    ap = argparse.ArgumentParser(description="Bateria por tamanho do MongoDB (Fases 3.2 e 4.4)")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--routes", nargs="+", choices=DEFAULT_ROUTES, default=DEFAULT_ROUTES)
    ap.add_argument("--sizes", nargs="+", choices=DEFAULT_SIZES, default=DEFAULT_SIZES)
    ap.add_argument("--output-dir", type=Path, default=RESULTS_DIR)
    ap.add_argument("--engine", choices=ENGINES, default=PYTHON)
    ap.add_argument("--slices", type=int, default=None)

    args = ap.parse_args()

    if args.engine == SPARK and len(args.routes) * len(args.sizes) > 1:
        ap.error(SINGLE_RUN)

    pkg = load_metamodel()

    PORT_XMI_DIR.mkdir(parents=True, exist_ok=True)

    with Results(args.output_dir) as tables:
        for route in args.routes:
            for size in args.sizes:
                print(
                    f"\n=== seed {args.seed} | rota {route} | {size} | {args.engine} ===",
                    flush=True,
                )

                # A geração é cronometrada só para o log: não é medida do porte
                # nem do oráculo, e saiu do esquema dos CSVs.
                t_generation = generate(route, size, args.uri, args.seed)

                key = run_id(
                    "size",
                    "mongodb",
                    database_name(route, size),
                    seed=args.seed,
                    engine=args.engine,
                )

                run = measure(
                    route,
                    size,
                    args.uri,
                    pkg,
                    PORT_XMI_DIR / f"{key}.xmi",
                    engine=args.engine,
                    slices=args.slices,
                )

                print(
                    f"  geracao={t_generation:.2f}s"
                    f"  boot={format_boot(run.t_boot) or '-'}"
                    f"  extracao={run.t_extraction:.2f}s"
                    f"  inferencia={run.t_inference:.2f}s"
                    f"  escrita={run.t_write:.2f}s"
                    f"  total={run.total:.2f}s"
                    f"  query={run.t_query:.2f}s"
                    f"  norm={normalized(run.total, run.t_query)}x",
                    flush=True,
                )

                for entity, (actual, model) in run.counts.items():
                    print(f"    {entity}: real={actual} modelo={model} ({fraction(model, actual)})")

                record(tables, key, run)


if __name__ == "__main__":
    main()

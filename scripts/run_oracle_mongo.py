"""Bateria porte x oráculo semeado, paradigma documento (Fase 3.1).

Equivalente do `run_oracle_neo4j.py` para o MongoDB, e fecha a assimetria que
existia entre os dois paradigmas: a comparação porte x oráculo sobre **entrada
idêntica e mesma máquina** só existia no grafo. No documento, o "8,7x do
oráculo" citado em §3.2 vem das tabelas do artigo — outra máquina, medida que
ninguém aqui reproduziu.

Por rota e tamanho: regera o banco com semente fixa, roda o **porte** (driver
nativo -> núcleo da Fase 1) e o **oráculo Java em Docker** (`--kind mongodb`)
sobre **o mesmo banco**, e compara os dois XMIs.

Não há comparação contra `resources/`: não existe XMI-oráculo publicado do User
Profiles em documento — é justamente o que esta bateria produz.

Custo desconhecido: o Spark do container nunca rodou sobre 800 mil documentos,
só sobre os 397 do Northwind (Fase 0.5). Comece pelos tamanhos menores.

    uv run python scripts/run_oracle_mongo.py --seed 23 --sizes small
    uv run python scripts/run_oracle_mongo.py --seed 23 --routes A --sizes small medium

Engine do porte (Fase 4.3): ``--engine spark`` extrai pela engine Spark, com
``--slices`` fatias por coleção (padrão: uma por núcleo). A sessão abre uma vez,
antes do laço, para que o boot da JVM fique fora do tempo de extração. Até a
4.4 criar a coluna ``engine`` nos CSVs, a corrida Spark **não** grava em
``results/`` — ficaria indistinguível da Python, com o mesmo ``run_id``. Passe
outro ``--output-dir``:

    uv run python scripts/run_oracle_mongo.py --seed 23 --engine spark --output-dir out/gate_4_3
"""

import argparse
import subprocess
import sys
import time
from collections.abc import Mapping
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pyecore.ecore import EObject, EPackage
from pymongo import MongoClient

from baseline import mongo_query_time
from output import (
    DEFAULT_SEED,
    PORT,
    SEEDED_ORACLE,
    Results,
    entity_name,
    format_query_time,
    format_seconds,
    fraction,
    modeled_counts,
    normalized,
    run_id,
)
from runs import MongoOracleRun
from uschema.extractors.mongo import extract_triples
from uschema.extractors.triple import triples_from_rows
from uschema.inference.build_uschema import BuildUSchema
from uschema.metamodel.registry import load_metamodel
from uschema.metamodel.xmi import load_model, save_model
from uschema.validation.equivalence import compare

if TYPE_CHECKING:
    # Só para anotação: a engine Python não pode carregar o pyspark.
    from pyspark.sql import SparkSession

DEFAULT_SIZES = ["small", "medium", "large", "larger"]
DEFAULT_ROUTES = ["A", "B"]
ENGINES = ["python", "spark"]

ROOT = Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "scripts" / "gen_userprofiles.py"
PORT_OUTPUT = ROOT / "out" / "porte"
ORACLE_OUTPUT = ROOT / "out" / "oraculo"
IMAGE = "extrator-uschema"


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


def run_oracle(database: str, collections: list[str], uri: str, seed: int) -> Path:
    """Roda o extrator Java no container sobre o banco já materializado.

    Diferente do caminho Neo4j, aqui o `--db` **é** o banco a conectar: o
    `entrypoint.sh` o escreve como `MONGO_DATABASE` num `config.properties`, e o
    `MongoDB2USchemaMain` lê de lá. As coleções vão por `MONGO_COLLECTIONS`
    (lista separada por vírgula) porque são lista, não flag.

    O nome do banco também vira o nome do schema no modelo, e tem de casar com o
    que o porte usa — divergência de `SCHEMA_NAME` é fatal no harness.

    Parameters
    ----------
    database : str
        Banco a extrair, e nome do schema no XMI resultante.
    collections : list of str
        Coleções a ler, na ordem em que serão passadas ao Java.
    uri : str
        URI do MongoDB, alcançável de dentro do container (`--network=host`).
    seed : int
        Semente da instância medida; entra no nome do XMI preservado.

    Returns
    -------
    Path
        O XMI do oráculo, já renomeado por semente.
    """
    ORACLE_OUTPUT.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network=host",
            "-v",
            f"{ORACLE_OUTPUT}:/output",
            "-e",
            f"MONGO_URL={uri}",
            "-e",
            f"MONGO_COLLECTIONS={','.join(collections)}",
            IMAGE,
            "--db",
            database,
            "--kind",
            "mongodb",
        ],
        check=True,
    )

    # O container escreve como root; renomear só exige permissão no diretório.
    written = ORACLE_OUTPUT / f"{database}.xmi"
    target = ORACLE_OUTPUT / f"mongo_{database}_seed{seed}.xmi"
    written.replace(target)

    return target


def measure(
    route: str,
    size: str,
    uri: str,
    seed: int,
    pkg: EPackage,
    *,
    spark: "SparkSession | None" = None,
    slices: int | None = None,
) -> MongoOracleRun:
    """Roda porte e oráculo sobre o mesmo banco e compara os dois XMIs.

    Com ``spark``, o porte extrai pela engine Spark (Fase 4.3); sem, pela Python
    de sempre. A sessão vem aberta de fora: o boot da JVM não entra no
    ``t_extraction``.
    """
    database = database_name(route, size)

    # Mapping, não dict: Database é invariante no parâmetro de tipo.
    client: MongoClient[Mapping[str, Any]] = MongoClient(uri)

    try:
        db = client[database]

        collections = sorted(db.list_collection_names())

        actual = {name: db[name].count_documents({}) for name in collections}

        start = time.perf_counter()

        rows: list[dict[str, Any]] = extract_triples(
            uri, db.name, collections, spark=spark, slices=slices
        )

        t_extraction = time.perf_counter() - start

    finally:
        client.close()

    start = time.perf_counter()

    port = BuildUSchema(pkg).build_from_rows(database, triples_from_rows(rows))

    t_inference = time.perf_counter() - start

    start = time.perf_counter()

    if spark is not None:
        save_model(port, PORT_OUTPUT / f"mongo_{database}_seed{seed}_spark.xmi")
    else:
        save_model(port, PORT_OUTPUT / f"mongo_{database}_seed{seed}.xmi")

    t_write = time.perf_counter() - start

    start = time.perf_counter()

    oracle_xmi = run_oracle(database, collections, uri, seed)

    t_oracle = time.perf_counter() - start

    oracle = load_model(oracle_xmi, pkg)

    # A query de referência roda por último, depois do porte E do oráculo:
    # antes, aqueceria o cache dos dois. Ver `baseline.py`.
    client = MongoClient(uri)
    try:
        t_query = mongo_query_time(client[database])
    finally:
        client.close()

    return MongoOracleRun(
        database=database,
        route=route,
        size=size,
        t_extraction=t_extraction,
        t_inference=t_inference,
        t_write=t_write,
        t_oracle=t_oracle,
        t_query=t_query,
        counts=side_by_side(collections, actual, port, oracle),
        result=compare(oracle, port),
    )


def side_by_side(
    collections: list[str], actual: dict[str, int], port: EObject, oracle: EObject
) -> dict[str, tuple[int, int, int]]:
    """Alinhar real, porte e oráculo por entidade.

    Chaveado pelo nome do ``EntityType`` no XMI, não pelo da coleção: é o
    vocabulário de ``divergences``, e o que o log imprime tem de casar com ele.
    """
    in_port = modeled_counts(port)
    in_oracle = modeled_counts(oracle)

    return {
        entity_name(name): (
            actual[name],
            in_port.get(entity_name(name), 0),
            in_oracle.get(entity_name(name), 0),
        )
        for name in collections
    }


def record(tables: Results, seed: int, run: MongoOracleRun) -> None:
    """Distribui a corrida pelas tabelas de resultado.

    O porte vai para `runs`, o oráculo para `oracle` — tabelas separadas porque
    o container devolve um número só, o relógio de parede do `docker run`.
    """
    key = run_id("oracle_chain", "mongodb", run.database, seed=seed)

    tables.add_run(
        {
            "run_id": key,
            "experiment": "oracle_chain",
            "size": run.size,
            "paradigm": "mongodb",
            "route": run.route,
            "target": run.database,
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

    tables.add_comparison(key, PORT, SEEDED_ORACLE, run.result)


def main() -> None:
    """Roda a cadeia porte x oráculo nas combinações pedidas e acumula as tabelas."""
    ap = argparse.ArgumentParser(description="Porte x oráculo semeado, MongoDB (Fase 3.1)")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--routes", nargs="+", choices=DEFAULT_ROUTES, default=DEFAULT_ROUTES)
    ap.add_argument("--sizes", nargs="+", choices=DEFAULT_SIZES, default=DEFAULT_SIZES)
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

    with Results(args.output_dir) as tables, context_engine as spark:
        for route in args.routes:
            for size in args.sizes:
                print(
                    f"\n=== seed {args.seed} | rota {route} | {size} | {args.engine} ===",
                    flush=True,
                )

                # A geração é cronometrada só para o log: não é medida de
                # produtor nenhum, e saiu do esquema dos CSVs.
                t_generation = generate(route, size, args.uri, args.seed)

                run = measure(
                    route, size, args.uri, args.seed, pkg, spark=spark, slices=args.slices
                )

                print(
                    f"  geracao={t_generation:.2f}s"
                    f"  extracao={run.t_extraction:.2f}s"
                    f"  inferencia={run.t_inference:.2f}s"
                    f"  escrita={run.t_write:.2f}s"
                    f"  porte={run.total:.2f}s"
                    f"  oraculo={run.t_oracle:.2f}s"
                    f"  query={run.t_query:.2f}s"
                    f"  norm={normalized(run.total, run.t_query)}x"
                    f"  equivalente={run.result.equivalent}"
                    f"  divergencias={len(run.result.divergences)}",
                    flush=True,
                )

                for entity, (actual, in_port, in_oracle) in run.counts.items():
                    print(
                        f"    {entity}: real={actual}"
                        f"  porte={in_port} ({fraction(in_port, actual)})"
                        f"  oraculo={in_oracle} ({fraction(in_oracle, actual)})"
                    )

                record(tables, args.seed, run)


if __name__ == "__main__":
    main()

"""Equivalência do Northwind pelos dois caminhos de leitura (Fase 3.1).

Roda o mesmo dataset por **arquivo** (os 17 JSONs, sem banco) e por **banco**
(o MongoDB carregado, pelo cursor do `pymongo`), constrói o USchema em cada
caminho e compara com `resources/mongodb/model_northwind.xmi`. Grava nas
tabelas de `scripts/output.py`, com a coluna `origin` (`file`/`database`)
distinguindo os dois.

Por que os dois caminhos: o bug #8 é sensível à **ordem de leitura**, então o
número de divergências muda conforme a origem do dado. O invariante citável não
é esse número, e sim `equivalent=True` mais a fração de coleções cujo `count`
fecha — que este script mede e imprime.

Os JSONs são JSONL em extended JSON (`{"$date": ...}`), então a leitura usa
`bson.json_util.loads`: com `json.load` puro o `$date` viraria um objeto
aninhado e o modelo ganharia uma entidade que o oráculo não tem.

Os JSONs estão versionados em `resources/datasets/northwind/` (BSD 2-Clause,
proveniência no README de lá), então o caminho de arquivo roda sem banco e sem
dependência externa.

Roda só na engine Python (Fase 4.4): o caminho de arquivo não tem banco a
fatiar, e o Northwind é gate de equivalência, não de volume. A coluna ``engine``
sai ``python`` e o ``boot_time``, vazio.

    uv run python scripts/run_northwind.py
    uv run python scripts/run_northwind.py --json-dir /outro/caminho
"""

import argparse
import hashlib
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from bson import json_util
from pyecore.ecore import EPackage
from pymongo import MongoClient

from output import (
    PORT,
    PORT_XMI_DIR,
    PYTHON,
    RESOURCES,
    RESULTS_DIR,
    ROOT,
    Results,
    entity_name,
    format_seconds,
    modeled_counts,
    run_id,
)
from runs import NorthwindRun
from uschema.extractors.mongo import build_triples, extract_database_triples
from uschema.extractors.triple import triples_from_rows
from uschema.inference.build_uschema import BuildUSchema
from uschema.metamodel.registry import load_metamodel
from uschema.metamodel.xmi import load_model, save_model
from uschema.validation.equivalence import compare

ORACLE_XMI = ROOT / "resources" / "mongodb" / "model_northwind.xmi"
DEFAULT_JSON_DIR = ROOT / "resources" / "datasets" / "northwind"

SCHEMA_NAME = "northwind"


def digest(json_dir: Path) -> str:
    """SHA-256 do conteúdo dos JSONs, em ordem de nome.

    Prende a corrida a uma versão do dataset — que é versionado no repo mas não
    tem outro identificador de conteúdo.
    """
    sha = hashlib.sha256()

    for path in sorted(json_dir.glob("*.json")):
        sha.update(path.name.encode())
        sha.update(path.read_bytes())

    return sha.hexdigest()


def read_files(json_dir: Path) -> tuple[list[dict[str, Any]], dict[str, int], float]:
    """Ler os JSONs do disco e montar as triplas, sem banco no meio."""
    rows: list[dict[str, Any]] = []
    actual: dict[str, int] = {}

    start = time.perf_counter()

    for path in sorted(json_dir.glob("*.json")):
        documents = [
            json_util.loads(line) for line in path.read_text().splitlines() if line.strip()
        ]

        actual[path.stem] = len(documents)
        rows.extend(build_triples(documents, path.stem))

    return rows, actual, time.perf_counter() - start


def read_database(uri: str, name: str) -> tuple[list[dict[str, Any]], dict[str, int], float]:
    """Ler o banco pelo cursor do `pymongo` e montar as triplas."""
    # Mapping, não dict: Database é invariante no parâmetro de tipo.
    client: MongoClient[Mapping[str, Any]] = MongoClient(uri)

    try:
        database = client[name]

        collections = sorted(database.list_collection_names())

        actual = {name: database[name].count_documents({}) for name in collections}

        start = time.perf_counter()

        rows = extract_database_triples(database, collections)

        return rows, actual, time.perf_counter() - start

    finally:
        client.close()


def evaluate(
    pkg: EPackage,
    origin: str,
    rows: list[dict[str, Any]],
    actual: dict[str, int],
    t_extraction: float,
) -> NorthwindRun:
    """Construir o USchema a partir das triplas e comparar com o oráculo."""
    start = time.perf_counter()

    port = BuildUSchema(pkg).build_from_rows(SCHEMA_NAME, triples_from_rows(rows))

    t_inference = time.perf_counter() - start

    start = time.perf_counter()

    save_model(port, PORT_XMI_DIR / f"{northwind_key(origin)}.xmi")

    t_write = time.perf_counter() - start

    in_model = modeled_counts(port)

    # Chaveado pelo nome do `EntityType` no XMI (`Orders`), não pelo da coleção
    # (`orders`): é o vocabulário de `divergences`, e sem isso as duas tabelas
    # não cruzam.
    return NorthwindRun(
        origin=origin,
        t_extraction=t_extraction,
        t_inference=t_inference,
        t_write=t_write,
        counts={
            entity_name(name): (total, in_model.get(entity_name(name), 0))
            for name, total in actual.items()
        },
        result=compare(load_model(ORACLE_XMI, pkg), port),
    )


def northwind_key(origin: str) -> str:
    """O ``run_id`` de um caminho de leitura — também o nome do XMI dele."""
    return run_id("equivalence", "mongodb", SCHEMA_NAME, origin=origin, engine=PYTHON)


def record(tables: Results, run: NorthwindRun) -> None:
    """Distribui a corrida pelas tabelas de resultado.

    Só o porte produz linhas aqui: esta bateria compara com o XMI publicado em
    `resources/`, não roda o oráculo.
    """
    key = northwind_key(run.origin)

    tables.add_run(
        {
            "run_id": key,
            "experiment": "equivalence",
            "paradigm": "mongodb",
            "target": SCHEMA_NAME,
            "origin": run.origin,
            "engine": PYTHON,
            "total_time": format_seconds(run.total),
            "extraction_time": format_seconds(run.t_extraction),
            "inference_time": format_seconds(run.t_inference),
            "write_time": format_seconds(run.t_write),
        }
    )

    tables.add_comparison(key, PORT, RESOURCES, run.result)


def report(run: NorthwindRun) -> None:
    """Imprimir o veredito, a fração que fecha e as coleções que não fecham."""
    print(f"\n=== origem: {run.origin} ===")
    print(
        f"  extracao={run.t_extraction:.2f}s"
        f"  inferencia={run.t_inference:.2f}s"
        f"  escrita={run.t_write:.2f}s"
        f"  total={run.total:.2f}s"
    )
    print(f"  equivalente={run.result.equivalent}  divergências={len(run.result.divergences)}")
    print(f"  coleções fechando a contagem: {run.closing}/{len(run.counts)}")

    for name, (actual, model) in sorted(run.counts.items()):
        if actual != model:
            print(f"    {name}: real={actual} modelo={model}")


def main() -> None:
    """Rodar os dois caminhos e acumular as tabelas."""
    ap = argparse.ArgumentParser(description="Equivalência do Northwind (Fase 3.1)")
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--db", default=SCHEMA_NAME)
    ap.add_argument("--json-dir", type=Path, default=DEFAULT_JSON_DIR)
    ap.add_argument("--output-dir", type=Path, default=RESULTS_DIR)

    args = ap.parse_args()

    if not args.json_dir.is_dir():
        raise SystemExit(f"--json-dir não existe: {args.json_dir}")

    pkg = load_metamodel()

    PORT_XMI_DIR.mkdir(parents=True, exist_ok=True)

    print(f"dataset: {args.json_dir}\nsha256:  {digest(args.json_dir)}")

    file_rows, file_actual, t_file = read_files(args.json_dir)
    database_rows, database_actual, t_database = read_database(args.uri, args.db)

    runs = [
        evaluate(pkg, "file", file_rows, file_actual, t_file),
        evaluate(pkg, "database", database_rows, database_actual, t_database),
    ]

    with Results(args.output_dir) as tables:
        for run in runs:
            report(run)
            record(tables, run)


if __name__ == "__main__":
    main()

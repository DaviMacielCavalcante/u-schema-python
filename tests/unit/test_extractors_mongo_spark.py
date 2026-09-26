"""Gate da 4.1: o backend Spark do extrator MongoDB contra o backend Python.

O gate (``todolist_fase4.md``): para o mesmo banco, o conjunto de triplas do
backend Spark é **idêntico** ao do Python — mesmos schemas, mesmos ``count``,
mesmos timestamps. A comparação é **como conjunto**, não como lista: a ordem do
``collect()`` sai do hash do ``reduceByKey``, e igualar a ordem é a 4.3.

Por que banco de verdade, e não as coleções falsas de ``test_extractors_mongo.py``:
o ``_read_partition`` roda nos workers Python do Spark, que são **processos
separados**. Um *fake* injetado no processo do teste não chega a eles; cada
partição abre o próprio ``MongoClient`` pela URI.

Cada teste recebe um banco temporário de nome aleatório, apagado no fim — nada
aqui depende dos ``up_*`` que as baterias geram. Sem ``mongod`` no ar, os testes
são **pulados**, com o motivo no resumo do pytest.

Marcados ``spark`` (sobem a JVM) e ``integration`` (precisam do ``mongod``):
rodam no pre-push e no CI, nunca no pre-commit.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Generator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from bson import ObjectId, json_util
from bson.int64 import Int64
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import PyMongoError
from pyspark.sql import SparkSession

from uschema.extractors.mongo import SIMPLE_DEFAULT_LONG, TYPE_FIELD, extract_triples
from uschema.extractors.spark import local_session

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_URI = "mongodb://localhost:27017"
#: Fatias por coleção nos testes: pouco, para que dezenas de documentos já
#: se dividam em várias partições e o reduce tenha de combinar entre elas.
_SLICES = 4
_NORTHWIND = Path(__file__).resolve().parents[2] / "resources" / "datasets" / "northwind"


# --- infraestrutura ----------------------------------------------------------


@pytest.fixture(scope="module")
def mongo() -> Generator[MongoClient[Mapping[str, Any]], None, None]:
    """Cliente do ``mongod`` local; pula o módulo se ele não responder."""
    client: MongoClient[Mapping[str, Any]] = MongoClient(_URI, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except PyMongoError as e:
        client.close()
        pytest.skip(f"mongod não responde em {_URI}: {e}")
    yield client
    client.close()


@pytest.fixture
def database(
    mongo: MongoClient[Mapping[str, Any]],
) -> Generator[Database[Mapping[str, Any]], None, None]:
    """Banco temporário de nome aleatório, apagado ao fim do teste."""
    name = f"uschema_test_{uuid4().hex[:8]}"
    try:
        yield mongo[name]
    finally:
        mongo.drop_database(name)


@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession, None, None]:
    """Uma sessão para o módulo inteiro: o boot da JVM é pago uma vez só."""
    with local_session(cores=4) as session:
        yield session


def _load_northwind(database: Database[Mapping[str, Any]]) -> list[str]:
    """Carregar os 17 JSONs do Northwind no banco; devolve os nomes das coleções.

    Os arquivos são JSONL em extended JSON (``{"$date": ...}``), por isso o
    ``json_util.loads`` — mesmo motivo de ``scripts/run_northwind.py``.
    """
    names = []
    for path in sorted(_NORTHWIND.glob("*.json")):
        documents = [
            json_util.loads(line) for line in path.read_text().splitlines() if line.strip()
        ]
        database[path.stem].insert_many(documents)
        names.append(path.stem)
    return names


def _as_set(rows: list[dict[str, Any]]) -> Counter[str]:
    """Transformar as linhas de triplas num conjunto comparável (com repetição).

    Dict não é *hashable*, então cada linha vira texto; ``sort_keys`` faz a ordem
    das chaves não contar. ``Counter`` em vez de ``set`` para que uma linha
    repetida num backend e não no outro também apareça como diferença.

    Parameters
    ----------
    rows : list of dict of str to Any
        Saída de :func:`~uschema.extractors.mongo.extract_triples`.

    Returns
    -------
    collections.Counter of str
        Cada linha serializada, com quantas vezes aparece.
    """
    return Counter(json.dumps(row, sort_keys=True) for row in rows)


def _both_backends(
    database: Database[Mapping[str, Any]], collections: list[str], spark: SparkSession
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Rodar os dois backends sobre o mesmo banco: ``(python, spark)``."""
    python_rows = extract_triples(_URI, database.name, collections)
    spark_rows = extract_triples(_URI, database.name, collections, spark=spark, slices=_SLICES)
    return python_rows, spark_rows


# --- os casos do gate --------------------------------------------------------


def test_northwind_iguais_como_conjunto(
    database: Database[Mapping[str, Any]], spark: SparkSession
) -> None:
    """O dataset real: ``_id`` inteiro (Rota B), 17 coleções, datas e floats.

    Com ``slices=4``, as coleções maiores se dividem em várias fatias e as menores
    que 4 documentos (``orders_tax_status``, ``privileges``...) ficam com uma só.
    """
    collections = _load_northwind(database)
    total = sum(database[name].count_documents({}) for name in collections)

    python_rows, spark_rows = _both_backends(database, collections, spark)

    assert _as_set(spark_rows) == _as_set(python_rows)
    # Sem isto, dois resultados vazios seriam "iguais" e o gate passaria em falso.
    assert sum(row["count"] for row in spark_rows) == total


def test_objectid_em_varias_fatias_combina_timestamps(
    database: Database[Mapping[str, Any]], spark: SparkSession
) -> None:
    """``_id`` ObjectId (Rota A) espalhado por várias fatias: o ``reduce_pairs``
    tem de combinar os timestamps **entre partições**, não só dentro de uma.
    """
    base = datetime(2026, 1, 1, tzinfo=UTC)
    # Um segundo diferente por documento: o `from_datetime` zera o resto do
    # ObjectId, então é o segundo que o torna único. 40 documentos com 4 fatias
    # dão 4 partições de 10.
    documents = [
        {"_id": ObjectId.from_datetime(base + timedelta(seconds=i)), "nome": f"n{i}"}
        for i in range(40)
    ]
    database["pessoas"].insert_many(documents)

    python_rows, spark_rows = _both_backends(database, ["pessoas"], spark)

    [row] = spark_rows
    assert row["count"] == 40
    assert row["firstTimestamp"] == int(base.timestamp())
    assert row["lastTimestamp"] == int((base + timedelta(seconds=39)).timestamp())
    assert _as_set(spark_rows) == _as_set(python_rows)


def test_colecoes_com_mesmo_schema_nao_se_fundem(
    database: Database[Mapping[str, Any]], spark: SparkSession
) -> None:
    """A coleção faz parte da chave do ``reduceByKey``.

    As fatias de todas as coleções dividem um RDD só; sem a coleção na chave, dois
    documentos de mesmo schema em coleções diferentes virariam uma tripla só.
    """
    database["a"].insert_one({"_id": 1, "nome": "x"})
    database["b"].insert_one({"_id": 1, "nome": "x"})

    python_rows, spark_rows = _both_backends(database, ["a", "b"], spark)

    assert sorted(row["schema"][TYPE_FIELD] for row in spark_rows) == ["a", "b"]
    assert _as_set(spark_rows) == _as_set(python_rows)


def test_tipos_bson_iguais_nos_dois_backends(
    database: Database[Mapping[str, Any]], spark: SparkSession
) -> None:
    """Os tipos que a assinatura distingue atravessam o Spark sem mudar.

    ``Int64`` e ``bool`` são as armadilhas (subclasses de ``int`` em Python); a
    ordem de chave não pode separar grupo, e a ordem de lista pode.
    """
    database["tipos"].insert_many(
        [
            {
                "_id": 1,
                "longo": Int64(5),
                "flag": True,
                "nada": None,
                "aninhado": {"a": 1, "b": "x"},
                "lista": [1, "dois", 3.0],
                "vazia": [],
            },
            # Mesmos campos, ordem de chave trocada: o mesmo grupo.
            {"_id": 2, "x": 1, "y": "a"},
            {"_id": 3, "y": "b", "x": 2},
            # Mesma lista, ordem de itens trocada: grupos diferentes.
            {"_id": 4, "l": [1, "a"]},
            {"_id": 5, "l": ["a", 1]},
        ]
    )

    python_rows, spark_rows = _both_backends(database, ["tipos"], spark)

    assert _as_set(spark_rows) == _as_set(python_rows)

    by_field = {
        field: [r for r in spark_rows if field in r["schema"]] for field in ("longo", "x", "l")
    }
    [tipos] = by_field["longo"]
    # O Int64 chega como objeto `$numberLong`, e o bool como `False` — não como 0.
    assert tipos["schema"]["longo"] == SIMPLE_DEFAULT_LONG
    assert tipos["schema"]["flag"] is False
    [xy] = by_field["x"]
    assert xy["count"] == 2
    assert sorted(r["count"] for r in by_field["l"]) == [1, 1]


def test_colecao_vazia_e_lista_vazia(
    database: Database[Mapping[str, Any]], spark: SparkSession
) -> None:
    """Os dois vazios: coleção sem documento e nenhuma coleção pedida."""
    database.create_collection("vazia")

    python_rows, spark_rows = _both_backends(database, ["vazia"], spark)

    assert python_rows == []
    assert spark_rows == []
    # Nenhuma coleção: o backend devolve antes do `parallelize`, que com zero
    # partições dividiria por zero.
    assert extract_triples(_URI, database.name, [], spark=spark) == []

"""Gate da 4.2: a engine Spark do extrator Neo4j contra a engine Python.

O gate (``todolist_fase4.md``): contagens **idênticas** às da engine Python,
incluindo o ``count`` de ``RelationshipType`` — fontes distintas, não arestas
brutas, que é o que uma partição mal feita quebra primeiro. A comparação é
**como conjunto** de ``(arquétipo, contagem)``: a ordem sai do hash do
``reduceByKey`` (4.3).

Sem Neo4j de verdade, de propósito: o Community tem **um banco só**, e um teste
não pode apagar nem sujar os dados das baterias. O grafo aqui é falso, e o
driver falso aplica o filtro de fatia (``id(n) % $slices = $slice_index``) do
mesmo jeito que o servidor aplicaria.

O driver falso precisa chegar aos **workers** do Spark, que são processos
separados e não conseguem importar este módulo pelo nome. Por isso o módulo é
registrado no ``cloudpickle`` para ser serializado **por valor**
(``register_pickle_by_value``) — sem isso, o *pickle* por referência falha no
worker.

Os testes que sobem a JVM são ``spark``; os que só olham a *query* e o
``_merge_counts`` são ``unit`` e rodam no pre-commit.
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from collections.abc import Generator, Iterator
from dataclasses import dataclass, field
from functools import partial
from typing import Any, cast

import pytest
from neo4j import GraphDatabase
from pyspark import cloudpickle
from pyspark.sql import SparkSession

from uschema.extractors.neo4j import (
    _canonical_key,
    _extract_archetype_counts_spark,
    _merge_counts,
    _read_label_combination,
    extract_archetype_counts_from_uri,
    extract_database_archetype_counts,
)
from uschema.extractors.spark import local_session

_SLICES = 4
_URI = "bolt://fake:7687"
_AUTH = ("neo4j", "senha-de-teste")

# --- o grafo falso -----------------------------------------------------------


@dataclass
class _Node:
    """Nó falso: a superfície de ``_NodeLike`` (``element_id``, ``labels``,
    ``keys()``, ``__getitem__``) mais um ``id`` inteiro para o filtro de fatia."""

    id: int
    labels: list[str]
    properties: dict[str, Any] = field(default_factory=dict)

    @property
    def element_id(self) -> str:
        return f"4:fake:{self.id}"

    def keys(self) -> list[str]:
        return list(self.properties)

    def __getitem__(self, key: str) -> Any:
        return self.properties[key]


@dataclass
class _Relationship:
    """Aresta falsa: ``type``, ``keys()``, ``__getitem__`` e o alvo."""

    type: str
    target: _Node
    properties: dict[str, Any] = field(default_factory=dict)

    def keys(self) -> list[str]:
        return list(self.properties)

    def __getitem__(self, key: str) -> Any:
        return self.properties[key]


@dataclass
class _Graph:
    nodes: list[_Node]
    edges: dict[int, list[_Relationship]] = field(default_factory=dict)


class _EagerResult:
    def __init__(self, records: list[Any]) -> None:
        self.records = records


_LABELS_IN_QUERY = re.compile(r"MATCH \(n((?::`[^`]+`)+)\)")
_SLICE_CLAUSE = " AND id(n) % $slices = $slice_index"


class _Session:
    def __init__(self, driver: _GraphDriver) -> None:
        self._driver = driver

    def __enter__(self) -> _Session:
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def run(self, query: str, **params: Any) -> Iterator[list[Any]]:
        self._driver.runs.append((query, params))
        match = _LABELS_IN_QUERY.search(query)
        assert match is not None, f"query inesperada: {query!r}"
        wanted = re.findall(r"`([^`]+)`", match.group(1))

        sliced = "slices" in params or "slice_index" in params
        # A query com fatia tem a cláusula; a sem fatia, não — o caminho Python
        # não pode mudar.
        assert (_SLICE_CLAUSE in query) == sliced, f"cláusula de fatia errada: {query!r}"

        for node in self._driver.graph.nodes:
            if sorted(node.labels) != sorted(wanted) or len(node.labels) != params["n_labels"]:
                continue
            if sliced and node.id % params["slices"] != params["slice_index"]:
                continue
            outgoing = self._driver.graph.edges.get(node.id, [])
            if not outgoing:
                yield [node, None, None]
            for rel in outgoing:
                yield [node, rel, list(rel.target.labels)]


class _GraphDriver:
    """Driver falso sobre um :class:`_Graph`: as duas *cypher* do extrator.

    Guarda as chamadas a ``session.run`` em ``runs``, para os testes da *query*.
    """

    def __init__(self, graph: _Graph) -> None:
        self.graph = graph
        self.runs: list[tuple[str, dict[str, Any]]] = []

    def __enter__(self) -> _GraphDriver:
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def execute_query(self, query: str, **kwargs: Any) -> _EagerResult:
        assert query == "MATCH (n) RETURN DISTINCT labels(n)", f"query inesperada: {query!r}"
        seen: list[list[str]] = []
        for node in self.graph.nodes:
            if node.labels not in seen:
                seen.append(node.labels)
        return _EagerResult([[labels] for labels in seen])

    def session(self, **kwargs: Any) -> _Session:
        return _Session(self)


def _sample_graph() -> _Graph:
    """60 nós em três combinações de labels, com os casos que a fatia pode quebrar.

    - ``User`` com 0, 1 ou 3 arestas; parte delas **repetidas** (mesmo tipo,
      alvo e propriedades), que o oráculo conta **uma vez por nó de origem**;
    - propriedades de tipos diferentes na aresta (variações de relacionamento);
    - um multi-label ``Zebra:Apple`` (labels próprios ordenados, ``refsTo`` não
      — o ``N1`` de ``bugs_originais.md``), também como alvo.
    """
    movies = [_Node(i, ["Movie"], {"title": "t", "year": 2000}) for i in range(0, 20)]
    zebras = [_Node(i, ["Zebra", "Apple"], {"name": "z"}) for i in range(20, 24)]
    users = [_Node(i, ["User"], {"name": "u", "age": 30}) for i in range(24, 60)]
    edges: dict[int, list[_Relationship]] = {}
    for user in users:
        k = user.id % 4
        if k == 0:
            continue  # sem aresta de saída
        movie = movies[user.id % len(movies)]
        watched = _Relationship("WATCHED", movie, {"rating": 5})
        if k == 1:
            edges[user.id] = [watched]
        elif k == 2:
            # Três arestas idênticas: contam como uma referência, uma vez.
            edges[user.id] = [watched, _Relationship("WATCHED", movie, {"rating": 5}), watched]
        else:
            edges[user.id] = [
                _Relationship("WATCHED", movie, {"rating": 4.5}),
                _Relationship("LIKES", zebras[user.id % len(zebras)]),
            ]
    return _Graph(movies + zebras + users, edges)


def _as_set(rows: list[dict[str, Any]]) -> Counter[tuple[str, int]]:
    """``(arquétipo canônico, contagem)`` com repetição — a comparação do gate."""
    return Counter((_canonical_key(row["archetype"]), row["count"]) for row in rows)


def _fake_neo4j_driver(uri: str, *, auth: tuple[str, str] | None = None) -> _GraphDriver:
    """Substituto do ``GraphDatabase.driver`` para a porta de entrada por URI.

    Roda no processo do teste (a listagem de labels) **e** nos workers (uma vez
    por partição), então não pode depender de nenhum objeto criado no teste.
    """
    assert uri == _URI
    assert auth == _AUTH
    return _GraphDriver(_sample_graph())


# --- infraestrutura Spark ----------------------------------------------------


@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession, None, None]:
    """Uma sessão para o módulo; registra este módulo para *pickle* por valor."""
    module = sys.modules[__name__]
    cloudpickle.register_pickle_by_value(module)  # type: ignore[no-untyped-call]
    try:
        with local_session(cores=4) as session:
            yield session
    finally:
        cloudpickle.unregister_pickle_by_value(module)  # type: ignore[no-untyped-call]


# --- unit: a query e o combine -----------------------------------------------


@pytest.mark.unit
def test_query_sem_fatia_fica_como_era() -> None:
    driver = _GraphDriver(_sample_graph())

    list(_read_label_combination(driver, None, ["User"], 1.0))  # type: ignore[arg-type]

    [(query, params)] = driver.runs
    assert query == (
        "MATCH (n:`User`) WHERE size(labels(n)) = $n_labels "
        "WITH n OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, labels(m)"
    )
    assert params == {"n_labels": 1}


@pytest.mark.unit
def test_query_com_fatia_filtra_n_antes_do_optional_match() -> None:
    driver = _GraphDriver(_sample_graph())

    rows = list(
        _read_label_combination(driver, None, ["User"], 1.0, slice_index=1, slices=_SLICES)  # type: ignore[arg-type]
    )

    [(query, params)] = driver.runs
    # A mesma query de sempre, só com a cláusula de fatia no primeiro WHERE.
    assert _SLICE_CLAUSE in query
    assert query.index(_SLICE_CLAUSE) < query.index("OPTIONAL MATCH")
    assert query.replace(_SLICE_CLAUSE, "") == (
        "MATCH (n:`User`) WHERE size(labels(n)) = $n_labels "
        "WITH n OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, labels(m)"
    )
    # Parâmetros, nunca valores colados no texto.
    assert params == {"n_labels": 1, "slices": _SLICES, "slice_index": 1}
    assert rows
    assert all(cast(_Node, node).id % _SLICES == 1 for node, _, _ in rows)


@pytest.mark.unit
def test_merge_counts_soma_e_mantem_o_arquetipo() -> None:
    archetype = {"labels": ["User"], "entity": "node", "properties": {}, "references": []}

    merged = _merge_counts((archetype, 2), (dict(archetype), 3))

    assert merged == (archetype, 5)


@pytest.mark.unit
@pytest.mark.parametrize("taxa", [0.0, -0.5, 1.5])
def test_sampling_rate_invalido_nas_duas_engines(taxa: float) -> None:
    # Sem spark: o caminho Python valida (driver do neo4j é preguiçoso, não conecta).
    with pytest.raises(ValueError, match="Sampling rate"):
        extract_archetype_counts_from_uri("bolt://localhost:1", sampling_rate=taxa)
    # Com spark: valida antes de tocar no Spark — a sessão nem é usada.
    with pytest.raises(ValueError, match="Sampling rate"):
        extract_archetype_counts_from_uri(
            "bolt://localhost:1",
            sampling_rate=taxa,
            spark=object(),  # type: ignore[arg-type]
        )


# --- spark: o gate -----------------------------------------------------------


@pytest.mark.spark
def test_engines_iguais_como_conjunto(spark: SparkSession) -> None:
    graph = _sample_graph()

    python_rows = extract_database_archetype_counts(_GraphDriver(graph))  # type: ignore[arg-type]
    spark_rows = _extract_archetype_counts_spark(
        spark,
        partial(_GraphDriver, graph),  # type: ignore[arg-type]
        None,
        1.0,
        _SLICES,
    )

    assert _as_set(spark_rows) == _as_set(python_rows)
    # Dois resultados vazios seriam "iguais": os nós têm de estar todos contados.
    node_total = sum(r["count"] for r in spark_rows if r["archetype"]["entity"] == "node")
    assert node_total == len(graph.nodes)


@pytest.mark.spark
def test_count_de_relationship_conta_fontes_distintas(spark: SparkSession) -> None:
    """Três arestas idênticas de um nó contam **uma** vez; o ``count`` da
    referência é o número de nós de origem, mesmo com eles espalhados pelas
    fatias."""
    graph = _sample_graph()

    spark_rows = _extract_archetype_counts_spark(
        spark,
        partial(_GraphDriver, graph),  # type: ignore[arg-type]
        None,
        1.0,
        _SLICES,
    )

    watched_5 = [
        r
        for r in spark_rows
        if r["archetype"]["entity"] == "relationship"
        and r["archetype"]["type"] == "WATCHED"
        # Pela chave canônica: em Python 0 == 0.0, e o `rating` float (outra
        # variação) casaria junto.
        and _canonical_key(r["archetype"]["properties"]) == '{"rating":0}'
    ]
    [row] = watched_5
    # Nós com k == 1 (uma aresta) e k == 2 (três idênticas): uma fonte cada.
    sources = [n for n in graph.nodes if n.labels == ["User"] and n.id % 4 in (1, 2)]
    assert row["count"] == len(sources)
    edges = sum(len(graph.edges[n.id]) for n in sources)
    assert row["count"] < edges  # arestas brutas dariam mais


@pytest.mark.spark
def test_banco_vazio_devolve_lista_vazia(spark: SparkSession) -> None:
    rows = _extract_archetype_counts_spark(
        spark,
        partial(_GraphDriver, _Graph([])),  # type: ignore[arg-type]
        None,
        1.0,
        _SLICES,
    )

    assert rows == []


@pytest.mark.spark
def test_porta_por_uri_com_spark_igual_ao_python(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Com ``spark`` e taxa válida, ``extract_archetype_counts_from_uri`` usa a
    engine Spark com a fábrica certa. O teste de ``sampling_rate`` para no
    ``raise`` e nunca chega a essa chamada."""
    monkeypatch.setattr(GraphDatabase, "driver", _fake_neo4j_driver)

    spark_rows = extract_archetype_counts_from_uri(_URI, auth=_AUTH, spark=spark, slices=_SLICES)
    python_rows = extract_database_archetype_counts(_GraphDriver(_sample_graph()))  # type: ignore[arg-type]

    assert _as_set(spark_rows) == _as_set(python_rows)
    # Dois resultados vazios seriam "iguais".
    assert spark_rows

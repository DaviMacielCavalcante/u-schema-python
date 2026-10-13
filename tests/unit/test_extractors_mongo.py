"""Extrator MongoDB — porte de ``Helpers.java`` (simplify/generateDocumentPair/reducePairs).

Cada teste trava um comportamento citado por linha do original; ver
``src/uschema/extractors/mongo.py`` pras citações completas. Cobre em
particular os dois achados de ``fase2_decisao_leitura_mongo_neo4j.md``: a
ordem de despacho ``Int64`` → ``bool`` → ``int`` (armadilha que só existe na
tradução pra Python) e o ``$numberLong`` como objeto agregado, igual ao
``$oid``.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import pytest
from bson import ObjectId
from bson.int64 import Int64

from uschema.extractors.mongo import (
    SIMPLE_DEFAULT_LONG,
    SIMPLE_DEFAULT_OBJECTID,
    TYPE_FIELD,
    _reduce_with_position,
    build_triples,
    extract_database_triples,
    generate_document_pair,
    reduce_pairs,
    simplify,
)

pytestmark = pytest.mark.unit


# --- simplify: escalares (Helpers.java:29-57, Constants.java:18-23) --------


def test_simplify_string_vira_sentinela_vazia() -> None:
    assert simplify({"nome": "widget"}) == {"nome": ""}


def test_simplify_bool_vira_sentinela_false() -> None:
    assert simplify({"ativo": True}) == {"ativo": False}
    assert simplify({"ativo": False}) == {"ativo": False}


def test_simplify_int32_vira_sentinela_zero() -> None:
    assert simplify({"estoque": 7}) == {"estoque": 0}


def test_simplify_float_vira_sentinela_zero_ponto_zero() -> None:
    assert simplify({"preco": 12.5}) == {"preco": 0.0}


def test_simplify_none_permanece_none() -> None:
    # Java: nenhum ramo do if/else if alcança `null`, e o `throw` final é
    # guardado por `input != null` — `null` atravessa sem erro.
    assert simplify({"campo": None}) == {"campo": None}


# --- simplify: ObjectId e Int64 viram objetos agregados ---------------------
# (achado de fase2_decisao_leitura_mongo_neo4j.md — Document.toJson() em modo
# STRICT serializa os dois como extended-JSON, não como string/escalar)


def test_simplify_object_id_vira_objeto_oid() -> None:
    resultado = simplify({"_id": ObjectId("5f43a4d2f3a2b1a9c8d7e6f5")})

    assert resultado == {"_id": SIMPLE_DEFAULT_OBJECTID}
    assert resultado["_id"] == {"$oid": "000000000000000000000000"}


def test_simplify_int64_vira_objeto_numberlong() -> None:
    resultado = simplify({"total": Int64(9_999_999_999)})

    assert resultado == {"total": SIMPLE_DEFAULT_LONG}
    assert resultado["total"] == {"$numberLong": "0"}


def test_simplify_sentinelas_sao_copias_independentes() -> None:
    """Duas chamadas não podem devolver o mesmo dict mutável compartilhado."""
    resultado = simplify({"a": ObjectId(), "b": ObjectId()})

    assert resultado["a"] is not resultado["b"]
    resultado["a"]["$oid"] = "mutado"
    assert resultado["b"]["$oid"] == "000000000000000000000000"


# --- simplify: a ordem de despacho Int64 -> bool -> int ---------------------
# Achado central de fase2_decisao_leitura_mongo_neo4j.md: no Java, Boolean/
# Integer/Long são classes disjuntas — a ordem dos `instanceof` não importa.
# Em Python, `bson.Int64` e `bool` são subclasses de `int`; testar na ordem
# errada faz os dois virarem `0` em silêncio, sem nenhum erro.


def test_simplify_int64_nao_e_confundido_com_int_generico() -> None:
    resultado = simplify({"total": Int64(42)})

    assert resultado["total"] != 0
    assert resultado["total"] == {"$numberLong": "0"}


def test_simplify_bool_nao_e_confundido_com_int_generico() -> None:
    # `False == 0` em Python, então a checagem de valor não bastaria pra
    # provar que o despacho testou `bool` e não caiu no ramo `int` genérico
    # por acidente — só a checagem de tipo/identidade prova isso.
    resultado = simplify({"ativo": True})

    assert resultado["ativo"] is False
    assert type(resultado["ativo"]) is bool


def test_simplify_int_puro_continua_int_apos_a_reordenacao() -> None:
    # Garante que mover Int64/bool pra frente do despacho não quebrou o
    # int32 genérico, que ainda tem de cair na sentinela `0` (int).
    resultado = simplify({"estoque": 7})

    assert resultado["estoque"] == 0
    assert isinstance(resultado["estoque"], int)
    assert not isinstance(resultado["estoque"], bool)


# --- simplify: recursão em dict/list, sem colapsar array --------------------


def test_simplify_objeto_aninhado_e_recursivo() -> None:
    resultado = simplify({"endereco": {"rua": "Av. X", "numero": 100}})

    assert resultado == {"endereco": {"rua": "", "numero": 0}}


def test_simplify_lista_nao_colapsa_elementos() -> None:
    # `Helpers.java:46-52` — Spark mantém todos os elementos; o colapso pra
    # um só elemento é comportamento do extrator map-reduce (map.js), não
    # deste.
    resultado = simplify({"tags": ["a", "b", "c"]})

    assert resultado == {"tags": ["", "", ""]}


def test_simplify_lista_vazia_permanece_vazia() -> None:
    assert simplify({"tags": []}) == {"tags": []}


def test_simplify_lista_de_documentos_e_recursiva() -> None:
    resultado = simplify({"itens": [{"qtd": 1}, {"qtd": 2}]})

    assert resultado == {"itens": [{"qtd": 0}, {"qtd": 0}]}


# --- simplify: tipo não suportado -------------------------------------------


def test_simplify_tipo_nao_suportado_lanca_type_error() -> None:
    # Porte de `UnsupportedOperationException` (`Helpers.java:53-54`) —
    # comportamento observável do oráculo, replicado, não suavizado.
    class _TipoQualquer:
        pass

    valor = _TipoQualquer()
    with pytest.raises(TypeError, match="not supported"):
        simplify({"campo": valor})


# --- generate_document_pair (Helpers.java:64-70, com #6 corrigido) ----------


def test_generate_document_pair_extrai_timestamp_de_object_id() -> None:
    oid = ObjectId.from_datetime(datetime(2020, 1, 1, tzinfo=UTC))

    schema, (first_ts, last_ts, count) = generate_document_pair({"_id": oid, "nome": "x"})

    assert schema == {"_id": SIMPLE_DEFAULT_OBJECTID, "nome": ""}
    assert first_ts == last_ts
    assert first_ts > 0
    assert count == 1


def test_generate_document_pair_id_nao_object_id_usa_timestamp_zero() -> None:
    """**Bug #6** (`bugs_originais.md`) — `Helpers.java:66`.

    O Java faz `doc.getObjectId("_id").getTimestamp()`, que lança
    `ClassCastException` quando `_id` não é `ObjectId` — derruba a extração
    inteira em coleções de origem relacional (ex.: Northwind, `_id` inteiro).
    Corrigido por construção: timestamp só se `_id` for `ObjectId`, senão `0`
    — sentinela válida, não valor (mesmo tratamento que
    `SchemaTriple.first_timestamp`/`last_timestamp`).
    """
    schema, (first_ts, last_ts, count) = generate_document_pair({"_id": 42, "nome": "x"})

    assert first_ts == 0
    assert last_ts == 0
    assert count == 1
    assert schema == {"_id": 0, "nome": ""}


def test_generate_document_pair_sem_id_usa_timestamp_zero() -> None:
    _schema, (first_ts, last_ts, count) = generate_document_pair({"nome": "x"})

    assert (first_ts, last_ts, count) == (0, 0, 1)


# --- reduce_pairs (Helpers.java:79-85) --------------------------------------


def test_reduce_pairs_combina_min_max_soma() -> None:
    assert reduce_pairs((10, 20, 1), (5, 30, 2)) == (5, 30, 3)


def test_reduce_pairs_e_comutativo() -> None:
    a, b = (10, 20, 1), (5, 30, 2)

    assert reduce_pairs(a, b) == reduce_pairs(b, a)


# --- _reduce_with_position (engine Spark, Fase 4.3) ------------------------
# Puro, sem Spark: é só a função que o `reduceByKey` recebe.


def test_reduce_with_position_combina_dados_e_fica_com_a_menor_posicao() -> None:
    first = ((10, 20, 1), (2, 5))
    second = ((5, 30, 2), (1, 7))

    assert _reduce_with_position(first, second) == ((5, 30, 3), (1, 7))


def test_reduce_with_position_e_comutativo() -> None:
    # A menor posição vem de `a` e o menor timestamp vem de `b`: cada parte do
    # valor é combinada por conta própria. O valor esperado fixo impede que dois
    # resultados errados e iguais passem.
    a = ((10, 20, 1), (0, 3))
    b = ((5, 30, 2), (0, 8))

    assert _reduce_with_position(a, b) == _reduce_with_position(b, a) == ((5, 30, 3), (0, 3))


def test_reduce_with_position_compara_a_fatia_antes_do_documento() -> None:
    # O documento 0 da fatia 3 vem depois do documento 900 da fatia 1: a posição
    # compara como tupla, fatia primeiro.
    cedo = ((0, 0, 1), (1, 900))
    tarde = ((0, 0, 1), (3, 0))

    [_, posicao] = _reduce_with_position(tarde, cedo)

    assert posicao == (1, 900)


# --- build_triples (MongoDB2USchema.java:73-83) -----------------------------


def test_build_triples_agrupa_documentos_de_mesmo_esqueleto() -> None:
    documentos = [{"nome": "a", "idade": 1}, {"nome": "b", "idade": 2}]

    [linha] = build_triples(documentos, "pessoas")

    assert linha["schema"] == {"nome": "", "idade": 0, "_type": "pessoas"}
    assert linha["count"] == 2


def test_build_triples_separa_esqueletos_diferentes() -> None:
    documentos: list[dict[str, Any]] = [{"nome": "a"}, {"nome": "a", "idade": 1}]

    linhas = build_triples(documentos, "pessoas")

    assert len(linhas) == 2
    assert all(linha["count"] == 1 for linha in linhas)


def test_build_triples_anexa_type_depois_da_agregacao() -> None:
    # `MongoDB2USchema.java:82` — `pair._1.put(typeField, collectionName)`
    # roda depois do `reduceByKey`, no `.map()` final.
    [linha] = build_triples([{"a": 1}], "clientes")

    assert linha["schema"][TYPE_FIELD] == "clientes"


def test_build_triples_combina_timestamps_min_max_ao_agrupar() -> None:
    cedo = ObjectId.from_datetime(datetime(2020, 1, 1, tzinfo=UTC))
    tarde = ObjectId.from_datetime(datetime(2021, 1, 1, tzinfo=UTC))
    documentos = [{"_id": cedo, "a": 1}, {"_id": tarde, "a": 2}]

    [linha] = build_triples(documentos, "coisas")

    assert linha["firstTimestamp"] < linha["lastTimestamp"]
    assert linha["count"] == 2


def test_build_triples_ordem_de_chaves_do_objeto_nao_separa_grupo() -> None:
    """`org.bson.Document` estende `LinkedHashMap` — `equals` é por conteúdo,
    não por ordem de inserção. Dois documentos com os mesmos campos em ordem
    diferente têm de cair no mesmo grupo.
    """
    documentos = [{"a": 1, "b": "x"}, {"b": "y", "a": 2}]

    [linha] = build_triples(documentos, "coisas")

    assert linha["count"] == 2


def test_build_triples_ordem_de_lista_separa_grupo() -> None:
    """`ArrayList.equals` **é** por ordem — ao contrário de `Document`. Como
    o `simplify` apaga valores, a diferença só fica observável quando os
    *tipos* na lista diferem por posição (`["texto", 1]` vs `[1, "texto"]`
    simplificam para esqueletos de listas diferentes: `["", 0]` vs `[0, ""]`).
    """
    documentos = [{"itens": ["texto", 1]}, {"itens": [1, "texto"]}]

    linhas = build_triples(documentos, "coisas")

    assert len(linhas) == 2


def test_build_triples_lista_vazia_de_documentos_devolve_lista_vazia() -> None:
    assert build_triples([], "vazio") == []


def test_build_triples_aceita_qualquer_iteravel_nao_so_lista() -> None:
    # `documents` é tipado como `Iterable`, não `list` — precisa aceitar um
    # gerador/cursor, não só uma lista já materializada (é assim que o
    # cursor do `pymongo` chegaria aqui).
    def gerador() -> Iterator[dict[str, int]]:
        yield {"a": 1}
        yield {"a": 2}

    [linha] = build_triples(gerador(), "coisas")

    assert linha["count"] == 2


# --- extract_database_triples (MongoDB2USchema.java:48-58, :60-86) ----------
# Sem banco real: um `Database`/`Collection`/cursor falso, só com `__getitem__`,
# `.find()` e `.sort()` — é toda a superfície que `extract_database_triples` usa.
# Cobre a função sem precisar de `pymongo.MongoClient` real (isso fica pra
# `extract_triples`, não testado aqui por exigir um MongoDB de verdade —
# ver `todolist_fase2.md`).
#
# Os documentos falsos têm `_id`, como todo documento do MongoDB: o `sort` do
# cursor (Fase 4.3) ordena por ele.


class _CursorFalso:
    """Devolve os documentos na ordem em que foram dados (a ordem "física"),
    até alguém pedir ``sort``."""

    def __init__(self, documentos: list[dict[str, Any]]) -> None:
        self._documentos = documentos

    def sort(self, campo: str, direcao: int = 1) -> _CursorFalso:
        ordenados = sorted(self._documentos, key=lambda d: d[campo], reverse=direcao == -1)
        return _CursorFalso(ordenados)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self._documentos)


class _ColecaoFalsa:
    def __init__(self, documentos: list[dict[str, Any]]) -> None:
        self._documentos = documentos

    def find(self) -> _CursorFalso:
        return _CursorFalso(self._documentos)


class _BancoFalso:
    def __init__(self, colecoes: dict[str, list[dict[str, Any]]]) -> None:
        self._colecoes = colecoes

    def __getitem__(self, nome: str) -> _ColecaoFalsa:
        return _ColecaoFalsa(self._colecoes[nome])


def test_extract_database_triples_le_uma_colecao() -> None:
    banco = _BancoFalso({"pessoas": [{"_id": 1, "nome": "a"}, {"_id": 2, "nome": "b"}]})

    [linha] = extract_database_triples(banco, ["pessoas"])  # type: ignore[arg-type]

    assert linha["schema"] == {"_id": 0, "nome": "", TYPE_FIELD: "pessoas"}
    assert linha["count"] == 2


def test_extract_database_triples_concatena_varias_colecoes() -> None:
    # `MongoDB2USchema.process` (`:50-52`) concatena o resultado de cada
    # coleção num único array antes de entregar pra inferência — cada
    # coleção continua com seu próprio agrupamento por esqueleto.
    banco = _BancoFalso(
        {
            "pessoas": [{"_id": 1, "nome": "a"}],
            "produtos": [{"_id": 1, "preco": 1.0}, {"_id": 2, "preco": 2.0}],
        }
    )

    linhas = extract_database_triples(banco, ["pessoas", "produtos"])  # type: ignore[arg-type]

    tipos = {linha["schema"][TYPE_FIELD]: linha["count"] for linha in linhas}
    assert tipos == {"pessoas": 1, "produtos": 2}


def test_extract_database_triples_colecao_vazia_nao_gera_linha() -> None:
    banco = _BancoFalso({"vazio": []})

    assert extract_database_triples(banco, ["vazio"]) == []  # type: ignore[arg-type]


def test_extract_database_triples_le_em_ordem_de_id_nao_na_ordem_fisica() -> None:
    """Fase 4.3: a ordem das triplas é a de primeira aparição **em ordem de
    ``_id``**, não a ordem em que os documentos estão gravados.

    Por causa do #8, essa ordem decide qual ``count`` sobrevive na inferência, e
    é a única que a engine Spark consegue reproduzir. Aqui a ordem física é a
    inversa: lida sem ``sort``, o esqueleto com ``email`` viria primeiro.
    """
    banco = _BancoFalso(
        {
            "pessoas": [
                {"_id": 3, "email": "c"},
                {"_id": 2, "nome": "b"},
                {"_id": 1, "nome": "a"},
            ]
        }
    )

    linhas = extract_database_triples(banco, ["pessoas"])  # type: ignore[arg-type]

    assert ["nome" in linha["schema"] for linha in linhas] == [True, False]
    assert [linha["count"] for linha in linhas] == [2, 1]

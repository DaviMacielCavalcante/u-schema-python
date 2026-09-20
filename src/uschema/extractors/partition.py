"""Divisão de uma coleção do MongoDB em fatias por faixa de ``_id`` (Fase 4.0).

Nada aqui existe no Java: o oráculo delega o particionamento ao conector Spark
do MongoDB, que este porte não usa (ver a decisão da 2.0 em
``todolist_fase2.md``). Como a leitura é por driver nativo, as fatias têm de ser
construídas à mão — é o que este módulo faz.

Por que faixa de ``_id``, e não ``skip``/``limit``
-------------------------------------------------

``skip(n)`` **percorre e descarta** os ``n`` primeiros: não existe salto direto
para a posição ``n``. Com 8 fatias de 100 mil documentos, a última pula 700 mil,
e a soma das fatias percorre ~2,8 milhões de posições para ler 800 mil
documentos. O custo cresce com o número de fatias — exatamente o contrário do
que a Fase 4 quer medir.

O filtro por faixa (``{"_id": {"$gte": ..., "$lt": ...}}``) é servido pelo
índice do ``_id``, que toda coleção tem, e não percorre nada fora da fatia. Vale
para ``ObjectId`` (Rota A) e para inteiro (Rota B), porque a comparação é entre
valores do mesmo tipo.

Premissa
--------

**O banco está parado durante a extração** — nenhuma inserção ou remoção entre o
cálculo dos cortes e a leitura das fatias. É o cenário da ferramenta (inferir o
esquema de uma base para migrá-la) e o mesmo que a bateria da Fase 3 usa.
"""

from collections.abc import Mapping, Sequence
from itertools import chain, pairwise
from typing import Any

from pymongo.collection import Collection

__all__ = [
    "id_boundaries",
    "ranges_from_boundaries",
    "slice_filter",
]


def id_boundaries(collection: Collection[Mapping[str, Any]], slices: int) -> list[Any]:
    """Obter os valores de ``_id`` que separam as fatias.

    Lê **apenas** os ``_id`` da coleção, em ordem crescente, e devolve o valor
    que está em cada fronteira de fatia.

    Parameters
    ----------
    collection : pymongo.collection.Collection
        Coleção a fatiar.
    slices : int
        Número de fatias desejado.

    Returns
    -------
    list of Any
        ``slices - 1`` valores de ``_id``, em ordem crescente. Lista vazia se
        ``slices <= 1`` ou se a coleção não tiver documentos suficientes para
        fatiar.
    """
    documents_quantity = collection.count_documents({})

    if slices <= 1 or documents_quantity < slices:
        return []

    ordered_ids = collection.find({}, {"_id": 1}).sort("_id", 1)

    ids_in_slice: list[Any] = []

    step = documents_quantity // slices

    for index, doc in enumerate(ordered_ids):
        is_in_boundaries = index % step == 0

        if index != 0 and is_in_boundaries:
            ids_in_slice.append(doc["_id"])

        if slices - 1 == len(ids_in_slice):
            break

    return ids_in_slice


def ranges_from_boundaries(boundaries: Sequence[Any]) -> list[tuple[Any | None, Any | None]]:
    """Transformar os cortes em faixas ``(limite_inferior, limite_superior)``.

    Parameters
    ----------
    boundaries : Sequence of Any
        Os cortes devolvidos por :func:`id_boundaries`, em ordem crescente.

    Returns
    -------
    list of tuple
        ``len(boundaries) + 1`` faixas, na forma ``(inferior, superior)``. A
        primeira tem ``None`` no lugar do limite inferior e a última, ``None``
        no superior — ``None`` significa "sem limite deste lado".

    Examples
    --------
    >>> ranges_from_boundaries([10, 20])
    [(None, 10), (10, 20), (20, None)]
    """
    if not boundaries:
        return [(None, None)]

    return list(
        chain(
            [(None, boundaries[0])],
            pairwise(boundaries),
            [(boundaries[-1], None)],
        )
    )


def slice_filter(lower: Any | None, upper: Any | None) -> dict[str, Any]:
    """Montar o filtro do ``find()`` correspondente a uma faixa.

    Parameters
    ----------
    lower : Any or None
        Limite inferior, **inclusivo** (``$gte``). ``None`` = sem limite.
    upper : Any or None
        Limite superior, **exclusivo** (``$lt``). ``None`` = sem limite.

    Returns
    -------
    dict of str to Any
        O filtro para ``collection.find(...)``. Faixa sem nenhum limite devolve
        ``{}`` — a coleção inteira.

    Examples
    --------
    >>> slice_filter(None, 10)
    {'_id': {'$lt': 10}}
    >>> slice_filter(10, 20)
    {'_id': {'$gte': 10, '$lt': 20}}
    """
    dict_for_mongo: dict[str, Any] = {}

    if lower is None and upper is None:
        return dict_for_mongo

    if lower is None:
        dict_for_mongo = {"_id": {"$lt": upper}}
        return dict_for_mongo

    if upper is None:
        dict_for_mongo = {"_id": {"$gte": lower}}
        return dict_for_mongo

    dict_for_mongo = {"_id": {"$gte": lower, "$lt": upper}}

    return dict_for_mongo

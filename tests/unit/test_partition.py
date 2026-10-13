"""Fatiamento de coleção por faixa de ``_id`` (Fase 4.0).

Nada aqui corresponde a código do oráculo — o particionamento é próprio do
porte, porque a leitura é por driver nativo e não pelo conector Spark (decisão
da 2.0). Os testes travam três coisas: a **cobertura** (cada documento cai em
exatamente uma fatia), a **leitura barata** (o cursor não é percorrido além do
último corte) e o **`_id` inteiro** da Rota B, onde o valor ``0`` é um corte
legítimo e não pode ser confundido com "sem limite".

Sem banco real: uma `Collection` falsa com `count_documents` e
`find(...).sort(...)`, que é toda a superfície usada por ``id_boundaries``.
"""

from __future__ import annotations

from collections.abc import Iterator
from itertools import pairwise
from typing import Any

import pytest
from bson import ObjectId

from uschema.extractors.partition import (
    id_boundaries,
    ranges_from_boundaries,
    slice_filter,
)

pytestmark = pytest.mark.unit


class _CursorFalso:
    """Cursor mínimo: itera ``{"_id": valor}`` e conta quantos foram lidos."""

    def __init__(self, ids: list[Any], leituras: list[int]) -> None:
        self._ids = ids
        self._leituras = leituras

    def sort(self, chave: str, direcao: int) -> _CursorFalso:
        assert chave == "_id"
        assert direcao == 1
        return _CursorFalso(sorted(self._ids), self._leituras)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        for valor in self._ids:
            self._leituras[0] += 1
            yield {"_id": valor}


class _ColecaoFalsa:
    def __init__(self, ids: list[Any]) -> None:
        self._ids = ids
        #: Quantos documentos foram efetivamente consumidos do cursor.
        self.leituras = [0]

    def count_documents(self, filtro: dict[str, Any]) -> int:
        assert filtro == {}
        return len(self._ids)

    def find(self, filtro: dict[str, Any], projecao: dict[str, Any]) -> _CursorFalso:
        assert filtro == {}
        # A projeção é o que torna a consulta barata (servida pelo índice do
        # `_id`); se ela sumir, o teste falha aqui e não só em performance.
        assert projecao == {"_id": 1}
        return _CursorFalso(self._ids, self.leituras)


def _aplicar(faixas: list[tuple[Any, Any]], ids: list[Any]) -> list[list[Any]]:
    """Simular, em Python, o que cada ``find(slice_filter(...))`` traria."""
    resultado = []
    for inferior, superior in faixas:
        filtro = slice_filter(inferior, superior)
        limites = filtro.get("_id", {})
        resultado.append(
            [
                valor
                for valor in ids
                if ("$gte" not in limites or valor >= limites["$gte"])
                and ("$lt" not in limites or valor < limites["$lt"])
            ]
        )
    return resultado


# --- slice_filter -----------------------------------------------------------


def test_slice_filter_sem_limite_nenhum_pega_a_colecao_inteira() -> None:
    assert slice_filter(None, None) == {}


def test_slice_filter_so_com_limite_superior() -> None:
    assert slice_filter(None, 10) == {"_id": {"$lt": 10}}


def test_slice_filter_so_com_limite_inferior() -> None:
    assert slice_filter(10, None) == {"_id": {"$gte": 10}}


def test_slice_filter_com_os_dois_limites() -> None:
    assert slice_filter(10, 20) == {"_id": {"$gte": 10, "$lt": 20}}


def test_slice_filter_limite_zero_nao_e_ausencia_de_limite() -> None:
    # Rota B: `_id` inteiro começando em 0. Testar o limite por veracidade
    # (`if not lower`) trataria o corte 0 como "sem limite" e essa fatia
    # passaria a varrer a coleção inteira — sobreposição silenciosa.
    assert slice_filter(0, 10) == {"_id": {"$gte": 0, "$lt": 10}}


def test_slice_filter_aceita_object_id() -> None:
    # Rota A: o filtro não interpreta o valor, só o repassa.
    inferior = ObjectId("000000000000000000000001")
    superior = ObjectId("000000000000000000000002")

    assert slice_filter(inferior, superior) == {"_id": {"$gte": inferior, "$lt": superior}}


# --- ranges_from_boundaries -------------------------------------------------


def test_ranges_sem_cortes_devolve_uma_fatia_sem_limites() -> None:
    assert ranges_from_boundaries([]) == [(None, None)]


def test_ranges_um_corte_devolve_duas_fatias() -> None:
    assert ranges_from_boundaries([10]) == [(None, 10), (10, None)]


def test_ranges_dois_cortes_devolvem_tres_fatias() -> None:
    assert ranges_from_boundaries([10, 20]) == [(None, 10), (10, 20), (20, None)]


@pytest.mark.parametrize("cortes", [[1], [1, 2], [1, 2, 3], list(range(1, 20))])
def test_ranges_produz_um_a_mais_que_os_cortes(cortes: list[int]) -> None:
    assert len(ranges_from_boundaries(cortes)) == len(cortes) + 1


def test_ranges_sao_contiguas_e_abertas_nas_pontas() -> None:
    # O superior de cada faixa é o inferior da seguinte: é o que garante que
    # nenhum documento fica entre duas fatias.
    faixas = ranges_from_boundaries([10, 20, 30])

    assert faixas[0][0] is None
    assert faixas[-1][1] is None
    assert all(atual[1] == proxima[0] for atual, proxima in pairwise(faixas))


# --- id_boundaries ----------------------------------------------------------


def test_id_boundaries_corta_de_passo_em_passo() -> None:
    colecao = _ColecaoFalsa(list(range(80)))

    assert id_boundaries(colecao, 8) == [10, 20, 30, 40, 50, 60, 70]  # type: ignore[arg-type]


def test_id_boundaries_devolve_um_corte_a_menos_que_as_fatias() -> None:
    colecao = _ColecaoFalsa(list(range(100)))

    assert len(id_boundaries(colecao, 4)) == 3  # type: ignore[arg-type]


def test_id_boundaries_ordena_ainda_que_a_colecao_venha_embaralhada() -> None:
    colecao = _ColecaoFalsa([5, 1, 9, 3, 7, 2, 8, 4, 6, 0])

    assert id_boundaries(colecao, 5) == [2, 4, 6, 8]  # type: ignore[arg-type]


def test_id_boundaries_para_no_ultimo_corte() -> None:
    # O cursor não pode ser percorrido até o fim: depois do último corte o
    # resto não interessa. Com 80 documentos e 8 fatias, o último corte está
    # na posição 70 — ler além disso é desperdício proporcional ao volume.
    colecao = _ColecaoFalsa(list(range(80)))

    id_boundaries(colecao, 8)  # type: ignore[arg-type]

    assert colecao.leituras[0] == 71


@pytest.mark.parametrize("slices", [0, 1, -3])
def test_id_boundaries_sem_fatiamento_devolve_lista_vazia(slices: int) -> None:
    colecao = _ColecaoFalsa(list(range(10)))

    assert id_boundaries(colecao, slices) == []  # type: ignore[arg-type]


def test_id_boundaries_menos_documentos_que_fatias_devolve_lista_vazia() -> None:
    colecao = _ColecaoFalsa([1, 2])

    assert id_boundaries(colecao, 8) == []  # type: ignore[arg-type]


def test_id_boundaries_colecao_vazia_devolve_lista_vazia() -> None:
    assert id_boundaries(_ColecaoFalsa([]), 4) == []  # type: ignore[arg-type]


def test_id_boundaries_um_documento_por_fatia() -> None:
    # Caso de borda do guarda: com tantos documentos quanto fatias ainda dá
    # para fatiar (passo 1), e é a fronteira entre `<` e `<=`.
    colecao = _ColecaoFalsa([10, 20, 30, 40])

    assert id_boundaries(colecao, 4) == [20, 30, 40]  # type: ignore[arg-type]


def test_id_boundaries_com_object_id() -> None:
    ids = [ObjectId(f"{indice:024x}") for indice in range(1, 41)]
    colecao = _ColecaoFalsa(list(ids))

    assert id_boundaries(colecao, 4) == [ids[10], ids[20], ids[30]]  # type: ignore[arg-type]


# --- as três juntas: a propriedade que o 4.0 precisa garantir ---------------


@pytest.mark.parametrize(("total", "slices"), [(80, 8), (100, 4), (83, 8), (7, 7), (50, 3)])
def test_cada_documento_cai_em_exatamente_uma_fatia(total: int, slices: int) -> None:
    ids = list(range(total))
    colecao = _ColecaoFalsa(list(ids))

    faixas = ranges_from_boundaries(id_boundaries(colecao, slices))  # type: ignore[arg-type]
    fatias = _aplicar(faixas, ids)

    assert [valor for fatia in fatias for valor in fatia] == ids


def test_cada_documento_cai_em_exatamente_uma_fatia_com_id_zero() -> None:
    # Mesma propriedade na Rota B, onde o menor `_id` é 0 — o valor que um
    # teste de veracidade confundiria com "sem limite".
    ids = list(range(0, 40))
    colecao = _ColecaoFalsa(list(ids))

    faixas = ranges_from_boundaries(id_boundaries(colecao, 4))  # type: ignore[arg-type]
    fatias = _aplicar(faixas, ids)

    assert [valor for fatia in fatias for valor in fatia] == ids
    assert all(fatia for fatia in fatias)


def test_fatias_ficam_equilibradas_quando_o_total_divide() -> None:
    ids = list(range(80))
    colecao = _ColecaoFalsa(list(ids))

    faixas = ranges_from_boundaries(id_boundaries(colecao, 8))  # type: ignore[arg-type]

    assert [len(fatia) for fatia in _aplicar(faixas, ids)] == [10] * 8


def test_a_sobra_da_divisao_fica_na_ultima_fatia() -> None:
    # 83 documentos em 8 fatias: passo 10, e os 3 que sobram vão para a
    # última, que não tem limite superior.
    ids = list(range(83))
    colecao = _ColecaoFalsa(list(ids))

    faixas = ranges_from_boundaries(id_boundaries(colecao, 8))  # type: ignore[arg-type]

    assert [len(fatia) for fatia in _aplicar(faixas, ids)] == [10] * 7 + [13]

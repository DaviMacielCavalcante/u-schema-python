"""Infraestrutura do backend Spark (Fase 4.0).

Dois tipos de teste convivem aqui, e é de propósito: ``default_slices`` é
aritmética pura (``unit``, roda no pre-commit), enquanto ``local_session`` sobe
uma JVM (``spark``, só pre-push e CI). São os **primeiros testes ``spark`` do
repositório** — o marker existe desde a Fase 2 e nunca tinha sido usado.

Os testes ``spark`` exigem **Java 17 ou 21** com ``JAVA_HOME`` apontando para
ele; com o JDK 8 a sessão não sobe. Ver ``uschema.extractors.spark``.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from uschema.extractors.spark import DEFAULT_APP_NAME, default_slices, local_session

# --- default_slices: aritmética pura, sem JVM ------------------------------


@pytest.mark.unit
def test_default_slices_usa_os_nucleos_logicos() -> None:
    with patch.object(os, "cpu_count", return_value=8):
        assert default_slices() == 8


@pytest.mark.unit
def test_default_slices_cai_para_um_quando_o_sistema_nao_informa() -> None:
    # `os.cpu_count()` pode devolver None (documentado na stdlib); sem o
    # fallback, o `local[N]` receberia "None" e a sessão nem subiria.
    with patch.object(os, "cpu_count", return_value=None):
        assert default_slices() == 1


@pytest.mark.unit
def test_default_slices_e_positivo_nesta_maquina() -> None:
    assert default_slices() >= 1


# --- local_session: exige JVM ----------------------------------------------


@pytest.mark.spark
def test_local_session_abre_e_executa_uma_acao() -> None:
    with local_session(cores=2) as spark:
        assert spark.sparkContext.parallelize([1, 2, 3], 3).sum() == 6


@pytest.mark.spark
def test_local_session_respeita_o_numero_de_nucleos() -> None:
    with local_session(cores=2) as spark:
        assert spark.sparkContext.master == "local[2]"


@pytest.mark.spark
def test_local_session_usa_o_nome_de_aplicacao_padrao() -> None:
    with local_session() as spark:
        assert spark.sparkContext.appName == DEFAULT_APP_NAME


@pytest.mark.spark
def test_local_session_fecha_a_sessao_ao_sair_do_bloco() -> None:
    with local_session(cores=1) as spark:
        contexto = spark.sparkContext

    # `_jsc` vira None no `stop()`; é o sinal observável de que a sessão
    # fechou. Sem isso, cada bateria deixaria uma JVM viva.
    assert contexto._jsc is None


@pytest.mark.spark
def test_local_session_propaga_erro_do_bloco_e_ainda_fecha() -> None:
    with pytest.raises(ZeroDivisionError):  # noqa: SIM117 — os dois `with` são o teste
        with local_session(cores=1) as spark:
            contexto = spark.sparkContext
            raise ZeroDivisionError

    assert contexto._jsc is None


@pytest.mark.spark
def test_particoes_preservam_a_ordem_no_collect() -> None:
    """A ordem das partições é o que a 4.3 vai depender.

    ``collect()`` devolve partição por partição, na ordem do índice — é isso
    que permite reconstruir a ordem do cursor ordenado por ``_id`` e manter os
    dois backends idênticos tripla a tripla. Se o Spark deixasse de garantir
    isso, a ordem-dependência do #8 voltaria a morder.
    """
    with local_session(cores=4) as spark:
        faixas = list(range(20))

        assert spark.sparkContext.parallelize(faixas, 4).collect() == faixas

"""Engine do porte nas baterias (Fase 4.4): abrir a sessão e cronometrar o boot.

Uma corrida da engine Spark paga um custo fixo antes de extrair — subir a JVM e
o contexto Spark — que a engine Python não paga. É o ``boot_time`` das tabelas,
e fica **fora** do ``total_time`` (`dicionario_de_dados.md`).

**Uma corrida Spark por processo.** O ``session.stop()`` do PySpark encerra a
sessão, mas não a JVM: ela segue viva no processo, e a sessão seguinte a
reaproveita já aquecida. Com vários tamanhos num processo só, o primeiro pagaria
o boot inteiro e o aquecimento da JVM, e os outros não — justamente a distorção
que a 4.4 existe para evitar nos tamanhos pequenos. Por isso as baterias recusam
mais de uma combinação com ``--engine spark`` (:data:`SINGLE_RUN`), e o
`run_suite.sh` chama cada combinação num processo próprio.
"""

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from output import PYTHON, SPARK

if TYPE_CHECKING:
    # Só para anotação: a engine Python não pode carregar o pyspark.
    from pyspark.sql import SparkSession

#: Mensagem do ``ap.error`` quando uma corrida Spark pede mais de uma combinação.
SINGLE_RUN = (
    "--engine spark mede uma corrida por processo: o PySpark não derruba a JVM entre "
    "sessões, e os tamanhos seguintes pegariam a JVM já aquecida. Passe uma rota e um "
    "tamanho por vez — o run_suite.sh já faz isso."
)


@contextmanager
def engine_session(engine: str) -> Iterator[tuple["SparkSession | None", float | None]]:
    """Abrir a sessão da engine para uma corrida e medir o boot.

    Parameters
    ----------
    engine : str
        :data:`~output.PYTHON` ou :data:`~output.SPARK`.

    Yields
    ------
    tuple of (pyspark.sql.SparkSession or None, float or None)
        A sessão e os segundos para subi-la. Na engine Python, ``(None, None)``:
        os extratores rodam o caminho de sempre com ``spark=None``, e a coluna
        ``boot_time`` sai vazia.

    Raises
    ------
    ValueError
        Se a engine não for nenhuma das duas.
    """
    if engine == PYTHON:
        yield None, None
        return

    if engine != SPARK:
        raise ValueError(f"engine desconhecida: {engine!r}")

    # Import tardio e fora do cronômetro: carregar o pyspark no topo do módulo o
    # puxaria também na engine Python, e o custo do import não é boot da JVM.
    from uschema.extractors.spark import local_session

    start = time.perf_counter()

    with local_session() as spark:
        yield spark, time.perf_counter() - start

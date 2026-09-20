"""Infraestrutura do backend Spark da extração (Fase 4.0).

Só a sessão e o número de fatias moram aqui. O uso do Spark — o
``mapPartitions`` sobre as faixas de :mod:`uschema.extractors.partition` e o
``reduceByKey`` sobre o ``reduce_pairs`` — fica no backend de cada extrator
(4.1 e 4.2).

Papel do Spark, e o que ele **não** é
-------------------------------------

Spark aqui é **paralelizador do map-reduce**, nunca camada de leitura ou de
tipagem: quem lê é o driver nativo (``pymongo``/``neo4j``), dentro do executor.
Passar pelo conector oficial destruiria a tipagem BSON (``ObjectId``/``Int64``
viram string e long) e uniformizaria os documentos num esquema único com nulos
— apagando a variação estrutural que a ferramenta existe para medir. Ver a
decisão da 2.0 em ``todolist_fase2.md`` e o plano em ``fase4_spark.md``.

Requisito de JVM
----------------

O PySpark 4.x roda em **Java 17 ou 21**; com o JDK 8 a sessão nem sobe
(``UnsupportedClassVersionError``). Nesta máquina o ``java`` padrão é o 8, então
``JAVA_HOME`` precisa apontar para um JDK compatível::

    export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

:func:`local_session` transforma a falha de inicialização numa mensagem que diz
isso, em vez do erro de versão de classe cru da JVM.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager

from loguru import logger
from pyspark.sql import SparkSession

__all__ = [
    "DEFAULT_APP_NAME",
    "default_slices",
    "local_session",
]

DEFAULT_APP_NAME = "uschema-extract"


def default_slices() -> int:
    """Número de fatias a usar quando nenhum é informado.

    Uma fatia por núcleo lógico. As fatias desta fase são **equilibradas por
    construção** (faixas de ``_id`` de tamanho igual, ver
    :mod:`uschema.extractors.partition`), então não há motivo para o
    super-particionamento que se usa contra distribuição torta — ele só
    acrescentaria custo fixo por tarefa.

    Returns
    -------
    int
        Número de núcleos lógicos, ou ``1`` quando o sistema não informa.
    """
    number_of_cores = os.cpu_count()

    return number_of_cores or 1


@contextmanager
def local_session(
    app_name: str = DEFAULT_APP_NAME,
    cores: int | None = None,
) -> Generator[SparkSession, None, None]:
    """Abrir uma ``SparkSession`` local e fechá-la ao sair do bloco.

    Parameters
    ----------
    app_name : str, optional
        Nome da aplicação. O padrão é :data:`DEFAULT_APP_NAME`.
    cores : int or None, optional
        Núcleos a usar (``local[N]``). ``None`` usa todos (``local[*]``).

    Yields
    ------
    pyspark.sql.SparkSession
        A sessão aberta.

    Raises
    ------
    RuntimeError
        Se a sessão não subir — tipicamente JVM incompatível; ver o requisito
        de JVM no docstring do módulo.

    Examples
    --------
    >>> with local_session() as spark:  # doctest: +SKIP
    ...     spark.sparkContext.parallelize([1, 2, 3]).sum()
    6
    """
    master_string = "local[*]" if cores is None else f"local[{cores}]"

    try:
        session = SparkSession.builder.appName(app_name).master(master_string).getOrCreate()
    except Exception as e:
        raise RuntimeError(
            f"Unsupported {os.environ.get('JAVA_HOME') or 'Java'}! Use java 17+ !"
        ) from e

    # Fora do `try`: uma falha daqui não é problema de JVM, e não deve ser
    # reportada como se fosse. Lazy — o loguru só formata se o nível estiver
    # ligado. `version` e `applicationId` identificam a corrida no log da
    # bateria: é por eles que se casa um tempo medido com a sessão que o
    # produziu.
    logger.debug(
        "SparkSession aberta: master={}, app={}, versão={}, id={}",
        master_string,
        app_name,
        session.version,
        session.sparkContext.applicationId,
    )

    try:
        yield session
    finally:
        session.stop()

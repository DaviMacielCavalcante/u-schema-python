"""Registros em memória de uma corrida — o que cada bateria mede antes de gravar.

Cinco `dataclass`, uma por bateria, que estavam declaradas dentro do script que
as usa. Reunidas aqui porque descrevem **o mesmo objeto** — uma corrida do porte
sobre um alvo — com as diferenças que o paradigma e o propósito impõem, e ler as
cinco lado a lado é o que torna essas diferenças visíveis:

============== ============================================================
`NorthwindRun`  dataset real, dois caminhos de leitura; sem tamanho nem rota
`MongoSizeRun`  documento por rota e tamanho; sem ``result`` (não há oráculo)
`Neo4jSizeRun`  grafo por tamanho; compara com ``resources/``
`MongoOracleRun` documento, os dois lados na mesma instância
`Neo4jOracleRun` grafo, os dois lados **e** as duas referências
============== ============================================================

O que **não** mora aqui: a escrita das tabelas, que é do `output`. Estes objetos
não conhecem CSV — cada bateria os traduz em linha no seu ``record()``, e é lá
que a precisão de cada coluna é decidida.

Uma assimetria é deliberada e não deve ser "unificada" sem medir de novo:

- **`counts` tem dois formatos.** Nas baterias por tamanho é ``(real, porte)``;
  nas do oráculo é ``(real, porte, oráculo)``. O terceiro elemento é o que
  sustenta o "#8 replicado", e só existe onde o Java rodou.

Desde a Fase 4.4, as quatro corridas sobre dado gerado levam ``engine`` e
``t_boot`` (``None`` na engine Python). O `NorthwindRun` não: o Northwind roda
só na engine Python, e o caminho de arquivo nem tem banco a fatiar.
"""

from dataclasses import dataclass

from uschema.validation.equivalence import ComparisonResult

__all__ = [
    "MongoOracleRun",
    "MongoSizeRun",
    "Neo4jOracleRun",
    "Neo4jSizeRun",
    "NorthwindRun",
]


@dataclass(frozen=True)
class NorthwindRun:
    """Uma leitura completa do Northwind por um caminho."""

    origin: str
    t_extraction: float
    t_inference: float
    t_write: float
    counts: dict[str, tuple[int, int]]
    result: ComparisonResult

    @property
    def total(self) -> float:
        """Relógio de parede do banco até o XMI em disco."""
        return self.t_extraction + self.t_inference + self.t_write

    @property
    def closing(self) -> int:
        """Quantas coleções têm o `count` do modelo igual ao real."""
        return sum(1 for actual, model in self.counts.values() if actual == model)


@dataclass(frozen=True)
class MongoSizeRun:
    """Uma corrida completa sobre um banco."""

    database: str
    route: str
    size: str
    t_extraction: float
    t_inference: float
    t_write: float
    #: ``python`` ou ``spark``; ver `engines.py`.
    engine: str
    #: Segundos para subir a ``SparkSession``, fora do :attr:`total`; ``None``
    #: na engine Python.
    t_boot: float | None
    t_query: float
    counts: dict[str, tuple[int, int]]

    @property
    def total(self) -> float:
        """Relógio de parede do banco até o XMI em disco."""
        return self.t_extraction + self.t_inference + self.t_write


@dataclass(frozen=True)
class Neo4jSizeRun:
    """Uma corrida completa num tamanho."""

    size: str
    schema: str
    t_extraction: float
    t_inference: float
    t_write: float
    #: ``python`` ou ``spark``; ver `engines.py`.
    engine: str
    #: Segundos para subir a ``SparkSession``, fora do :attr:`total`; ``None``
    #: na engine Python.
    t_boot: float | None
    t_query: float
    counts: dict[str, tuple[int, int]]
    result: ComparisonResult

    @property
    def total(self) -> float:
        """Relógio de parede do banco até o XMI em disco."""
        return self.t_extraction + self.t_inference + self.t_write


@dataclass(frozen=True)
class MongoOracleRun:
    """Uma corrida completa sobre um banco, com os dois lados medidos."""

    database: str
    route: str
    size: str
    t_extraction: float
    t_inference: float
    t_write: float
    #: ``python`` ou ``spark``; ver `engines.py`.
    engine: str
    #: Segundos para subir a ``SparkSession``, fora do :attr:`total`; ``None``
    #: na engine Python.
    t_boot: float | None
    t_oracle: float
    t_query: float
    #: Por entidade: quantos existem no banco, quantos o porte contabiliza e
    #: quantos o oráculo contabiliza. Os dois últimos casam quando o #8 é
    #: replicado fielmente, que é o que a Fase 3.3 afirma.
    counts: dict[str, tuple[int, int, int]]
    result: ComparisonResult

    @property
    def total(self) -> float:
        """Relógio de parede do banco até o XMI em disco, do lado do porte."""
        return self.t_extraction + self.t_inference + self.t_write


@dataclass(frozen=True)
class Neo4jOracleRun:
    """Uma corrida completa num tamanho, com os dois lados medidos."""

    size: str
    schema: str
    t_extraction: float
    t_inference: float
    t_write: float
    #: ``python`` ou ``spark``; ver `engines.py`.
    engine: str
    #: Segundos para subir a ``SparkSession``, fora do :attr:`total`; ``None``
    #: na engine Python.
    t_boot: float | None
    t_oracle: float
    t_query: float
    #: Por label: quantos nós existem, quantos o porte contabiliza e quantos o
    #: oráculo contabiliza. No grafo os três fecham — o #8 não passa por aqui.
    counts: dict[str, tuple[int, int, int]]
    vs_oracle: ComparisonResult
    vs_resources: ComparisonResult

    @property
    def total(self) -> float:
        """Relógio de parede do banco até o XMI em disco, do lado do porte."""
        return self.t_extraction + self.t_inference + self.t_write

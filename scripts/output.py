"""Tabelas de resultado das Fases 3 e 4 — **um grão por arquivo**.

As baterias antes gravavam um CSV cada, misturando três granularidades no mesmo
arquivo: fatos da corrida (os tempos) repetidos em cada linha de entidade, e
o veredito repetido em cada linha de divergência. Dava para
anexar linha a linha durante a corrida, mas obrigava a deduplicar na análise.

Aqui cada tabela tem um grão só, e todas se unem por ``run_id``:

=================== =======================================================
`runs.csv`          uma execução do porte — tempos e metadados
`oracle.csv`        uma execução do oráculo Java — só o relógio de parede
`comparisons.csv`   um confronto com um XMI de referência
`divergences.csv`   uma divergência
=================== =======================================================

A limpeza do banco anterior **não** é medida: acontece, é impressa no log das
baterias, e não vira coluna nem tabela. `clean_databases.py` segue como
utilitário.

**O esquema completo, coluna a coluna, está em `dicionario_de_dados.md`** (raiz
do repositório), que é a referência para quem for analisar os CSVs. Este módulo
implementa o que aquele documento descreve.

Desde a Fase 4.4, ``runs`` carrega ``engine`` e ``boot_time``, e a engine entra
no ``run_id``. O esquema de 13 colunas da bateria canônica (``results/``) não é
mais produzido: a guarda de cabeçalho recusa anexar nele, e a bateria da Fase 4
grava em ``results/fase4/``.

Todo número gravado tem **dono explícito**
------------------------------------------
O esquema anterior não permitia distinguir medida nossa de medida do Java: os
tempos do porte eram colunas soltas e o do oráculo era uma coluna a mais, no
meio deles. Hoje cada produtor tem a sua tabela — ``runs`` é do porte,
``oracle`` é do Java.

Pelo mesmo motivo ``comparisons`` e ``divergences`` nomeiam os dois lados do
confronto (``subject``/``reference``), e a mensagem de divergência é gravada com
os rótulos posicionais do harness já substituídos — ver :func:`name_sides`.

Nomes de coluna em inglês, como o resto do código; a prosa dos `.md` segue em
português. O módulo se chama `output` e não `results` porque o diretório de
saída na raiz é `results/`: um módulo homônimo vira namespace package e o mypy
passa a resolver o import para a pasta de dados.

Colunas derivadas **não** entram: razão de captura, percentual e veredito por
entidade são conta da análise. A exceção é ``normalized``, que é a métrica de
comparabilidade com o artigo — declarada como exceção no dicionário, não como
revogação da regra.

Uma medida fica **fora das tabelas**, só no log das baterias: a contagem por
entidade — real no banco contra somado no XMI, calculada por
:func:`modeled_counts`. É do log que sai o invariante citável do Northwind (14
de 17 coleções fechando a contagem).
"""

import csv
from collections.abc import Iterable, Mapping
from pathlib import Path
from types import TracebackType
from typing import Any, TextIO

from pyecore.ecore import EObject

from uschema.naming.inflector import Inflector
from uschema.validation.equivalence import ComparisonResult, Divergence

#: Instância única — o Inflector é uma lista ordenada de regras, imutável em uso,
#: e reconstruí-la por entidade custaria à toa numa bateria de 800 mil documentos.
_INFLECTOR = Inflector()

#: Valor de ``subject``. Constante em vez de literal solto porque as cinco
#: baterias gravam a mesma string e o mypy não pega typo em literal — um
#: ``"prot"`` passaria e só apareceria na análise.
PORT = "port"

#: Valores de ``reference``. ``RESOURCES`` é o XMI publicado pelos autores
#: originais sobre o dataset **deles**; ``SEEDED_ORACLE`` é o oráculo Java
#: rodado sobre **a mesma instância** que o porte.
RESOURCES = "resources"
SEEDED_ORACLE = "seeded_oracle"

#: Valores de ``engine`` (Fase 4): qual engine do porte fez a extração. Constantes
#: pelo mesmo motivo de :data:`PORT` — um ``"sprak"`` passaria pelo mypy.
PYTHON = "python"
SPARK = "spark"
ENGINES = [PYTHON, SPARK]

#: Semente padrão das baterias. Mora aqui — e não em cada script — porque o
#: `run_suite.sh` também precisa dela: a guarda contra rodar a mesma semente duas
#: vezes procura o valor em `results/fase4/runs.csv`, e duplicá-lo no shell
#: abriria espaço para os dois divergirem, que é justamente o caso que a guarda
#: existe para pegar.
DEFAULT_SEED = 23

ROOT = Path(__file__).resolve().parents[1]

#: Saídas da Fase 4: tabelas e XMIs numa árvore própria. A bateria canônica de
#: 27/08 mora em `results/` e `out/{porte,oraculo}/`, versionada arquivo a
#: arquivo, e uma corrida nova com os nomes antigos sobrescreveria aqueles XMIs —
#: já aconteceu no gate da 4.3. Aqui cada XMI se chama pelo ``run_id`` da
#: corrida que o produziu, que é único por construção.
RESULTS_DIR = ROOT / "results" / "fase4"
PORT_XMI_DIR = ROOT / "out" / "fase4" / "porte"
ORACLE_XMI_DIR = ROOT / "out" / "fase4" / "oraculo"

#: Casas decimais das colunas de tempo. `query_time` leva quatro porque é o
#: divisor de `normalized`: a duas casas o arredondamento sozinho desloca a razão
#: em até 8%, e a coluna derivada deixa de bater com as que a definem. Constantes
#: em vez de literal em cada `record()` porque `normalized` divide os valores
#: **já arredondados** — as duas precisões têm de ser a mesma coisa, não duas.
TIME_DECIMALS = 2
QUERY_DECIMALS = 4

RUNS = [
    "run_id",
    "experiment",
    "size",
    "paradigm",
    "route",
    "target",
    "origin",
    "engine",
    "total_time",
    "extraction_time",
    "inference_time",
    "write_time",
    # Fora do `total_time`: o custo fixo da engine Spark, vazio na Python.
    "boot_time",
    "query_time",
    "normalized",
]

#: O oráculo tem tabela própria porque só reporta **um** número: o relógio de
#: parede do `docker run`. Na mesma tabela do porte ele deixaria
#: `extraction_time`/`inference_time`/`write_time` vazias em toda linha, e
#: obrigaria `total_time` a ter duas definições.
ORACLE_RUNS = ["run_id", "total_time", "normalized"]

COMPARISONS = [
    "run_id",
    "subject",
    "reference",
    "equivalent",
    "fatal_divergences",
    "non_fatal_divergences",
]

DIVERGENCES = ["run_id", "subject", "reference", "category", "fatal", "message"]

TABLES = {
    "runs": RUNS,
    "oracle": ORACLE_RUNS,
    "comparisons": COMPARISONS,
    "divergences": DIVERGENCES,
}

#: Chave natural de cada tabela — o que não pode repetir no arquivo. O `run_id` é
#: determinístico, então repetir uma bateria no mesmo diretório regrava as mesmas
#: chaves em **append**: sem esta guarda a duplicata entra calada e só aparece
#: depois, num `uniq -d`, com as contagens da análise já dobradas.
#:
#: `divergences` fica de fora de propósito: um confronto legitimamente produz N
#: linhas com o mesmo `run_id`, e ali não há chave natural.
KEYS = {
    "runs": ("run_id",),
    "oracle": ("run_id",),
    "comparisons": ("run_id", "reference"),
}


def run_id(
    experiment: str,
    paradigm: str,
    target: str,
    seed: int | None = None,
    origin: str | None = None,
    engine: str | None = None,
) -> str:
    """Montar o identificador determinístico de uma corrida.

    Determinístico de propósito: reconstruível a partir das colunas, sem
    depender da ordem das linhas. O experimento entra porque o mesmo tamanho com
    a mesma semente é medida duas vezes — na bateria por tamanho e na cadeia do
    oráculo —, e são corridas distintas.

    **A semente entra no identificador mesmo não sendo mais coluna.** É ela que
    separa duas corridas do mesmo alvo com sementes diferentes; sem ela, rodar
    com ``--seed`` distinto produziria chave duplicada em silêncio.

    **A engine entra por último, e sempre** nas tabelas da Fase 4 — inclusive
    ``python`` —, pelo mesmo motivo: as duas engines medem o mesmo alvo com a
    mesma semente. É opcional aqui só porque a bateria canônica da Fase 3 foi
    gravada sem ela.

    Parameters
    ----------
    experiment : str
        Qual bateria: ``size``, ``oracle_chain`` ou ``equivalence``.
    paradigm : str
        ``mongodb`` ou ``neo4j``.
    target : str
        Banco ou schema medido (``up_a_small``, ``movies_min``, ``northwind``).
    seed : int, optional
        Semente do gerador; ausente nos datasets que não são gerados.
    origin : str, optional
        ``file`` ou ``database``, quando o mesmo alvo é lido por mais de um
        caminho.
    engine : str, optional
        :data:`PYTHON` ou :data:`SPARK`, a engine do porte que fez a extração.

    Returns
    -------
    str
        Identificador com os campos presentes unidos por ``-``.

    Examples
    --------
    >>> run_id("size", "mongodb", "up_a_small", seed=23)
    'size-mongodb-up_a_small-23'
    >>> run_id("equivalence", "mongodb", "northwind", origin="file")
    'equivalence-mongodb-northwind-file'
    >>> run_id("size", "mongodb", "up_a_small", seed=23, engine="spark")
    'size-mongodb-up_a_small-23-spark'
    """
    parts = [experiment, paradigm, target]

    if seed is not None:
        parts.append(str(seed))

    if origin is not None:
        parts.append(origin)

    if engine is not None:
        parts.append(engine)

    return "-".join(parts)


def normalized(total_time: float, query_time: float) -> str:
    """Dividir o tempo do pipeline pelo da query de referência.

    A métrica do artigo do U-Schema (Information Systems 104, 2022): *"the
    normalized value (inference time divided by query time)"*. Serve para
    comparar com a Table 4 de lá sem depender da máquina — ver
    ``scripts/baseline.py`` para a query e a justificativa.

    O dividendo é o **pipeline inteiro** (``total_time``), que é o que o artigo
    chama de *inference*; a coluna ``inference_time`` do nosso esquema é outra
    coisa e fica em ~0,00s.

    **Divide os valores como eles vão para o CSV, não os crus, e devolve a razão
    em precisão cheia.** O contrato do `dicionario_de_dados.md` é que dividir
    ``total_time`` por ``query_time``, lidos do arquivo, reproduza esta coluna;
    arredondar só o resultado publicaria a razão de dois números que o CSV não
    tem, e é por isso que ``query_time`` leva quatro casas.

    Parameters
    ----------
    total_time : float
        Segundos do pipeline: extração + inferência + escrita.
    query_time : float
        Segundos da query de referência, no mesmo banco.

    Returns
    -------
    str
        A razão em precisão cheia, pronta para o CSV.

    Raises
    ------
    ValueError
        Se ``query_time`` for zero, negativo ou pequeno demais para sobreviver
        ao arredondamento da coluna — divisor inválido indica cronômetro
        quebrado, não resultado a gravar.

    Examples
    --------
    >>> normalized(23.452, 2.366)
    '9.911242603550296'

    A razão é a das colunas, então bate com o que a análise recalcula:

    >>> normalized(23.452, 2.366) == str(23.45 / 2.366)
    True
    """
    # Valida o divisor **arredondado**, que é o que de fato divide: um
    # `query_time` positivo mas menor que meio décimo de milissegundo vira 0.0
    # nas quatro casas da coluna, e a divisão estouraria com ZeroDivisionError
    # em vez da mensagem daqui.
    divisor = round(query_time, QUERY_DECIMALS)

    if divisor <= 0:
        raise ValueError(f"query_time inválido como divisor: {query_time!r}")

    return str(round(total_time, TIME_DECIMALS) / divisor)


def format_seconds(value: float) -> str:
    """Formatar uma coluna de tempo do CSV.

    Existe para que a precisão das colunas e a que :func:`normalized` usa saiam
    da **mesma** constante — divergirem faria a razão deixar de bater com as
    colunas que a definem.

    Parameters
    ----------
    value : float
        Segundos medidos.

    Returns
    -------
    str
        O valor com :data:`TIME_DECIMALS` casas.

    Examples
    --------
    >>> format_seconds(23.4521)
    '23.45'
    """
    return f"{value:.{TIME_DECIMALS}f}"


def format_boot(value: float | None) -> str:
    """Formatar a coluna ``boot_time``, vazia na engine Python.

    Parameters
    ----------
    value : float or None
        Segundos para subir a ``SparkSession``; ``None`` na engine Python.

    Returns
    -------
    str
        O valor com :data:`TIME_DECIMALS` casas, ou vazio.

    Examples
    --------
    >>> format_boot(3.214)
    '3.21'
    >>> format_boot(None)
    ''
    """
    return "" if value is None else format_seconds(value)


def format_query_time(value: float) -> str:
    """Formatar a coluna ``query_time``, que leva mais casas que as outras.

    Parameters
    ----------
    value : float
        Segundos da query de referência.

    Returns
    -------
    str
        O valor com :data:`QUERY_DECIMALS` casas.

    Examples
    --------
    >>> format_query_time(2.36612)
    '2.3661'
    """
    return f"{value:.{QUERY_DECIMALS}f}"


def fraction(part: int, whole: int) -> str:
    """Formatar ``part``/``whole`` como percentual, tolerando denominador zero.

    Só o log das baterias usa isto. Coleção vazia no MongoDB ou label ausente no
    grafo dão ``whole == 0``, e a divisão crua abortava a corrida **antes** do
    ``record()`` — descartando a medição inteira, inclusive o ``docker run`` do
    oráculo, por causa de uma linha de log.

    Parameters
    ----------
    part : int
        Quantos o modelo (ou o oráculo) contabiliza.
    whole : int
        Quantos existem no banco.

    Returns
    -------
    str
        O percentual com uma casa, ou ``'n/a'`` quando não há o que dividir.

    Examples
    --------
    >>> fraction(42592, 100000)
    '42.6%'
    >>> fraction(0, 0)
    'n/a'
    """
    if whole == 0:
        return "n/a"

    return f"{part / whole:.1%}"


def entity_name(source: str) -> str:
    """Nome do ``EntityType`` no XMI a partir do nome da origem.

    Casa a origem (``orders``) com o nome que o modelo usa (``Orders``), que é o
    vocabulário das mensagens de ``divergences`` — sem isso o log da bateria
    nomearia as entidades de um jeito e o CSV de outro. A regra é a mesma que o
    ``SchemaInference`` usa para nomear a entidade: capitalizar pelo Inflector,
    que **não** pluraliza nem mexe em underscore.

    Parameters
    ----------
    source : str
        Nome da coleção (MongoDB) ou do label (Neo4j).

    Returns
    -------
    str
        Nome do ``EntityType`` correspondente.

    Raises
    ------
    ValueError
        Se o Inflector devolver ``None``, o que só ocorre para entrada ``None``.

    Examples
    --------
    >>> entity_name("purchase_orders")
    'Purchase_orders'
    >>> entity_name("User")
    'User'
    """
    name = _INFLECTOR.capitalize(source)

    if name is None:
        raise ValueError(f"nome de origem sem capitalização possível: {source!r}")

    return name


def modeled_counts(model: EObject) -> dict[str, int]:
    """Somar o ``count`` das variações de cada entidade de um ``USchema``.

    Quanto **aquele modelo** diz ter visto, por entidade. Vale para os dois
    produtores e para os dois paradigmas — o grafo tem núcleo de construção
    próprio, mas o metamodelo é o mesmo.

    Não alimenta tabela nenhuma: as baterias confrontam este valor com a
    contagem no banco e **imprimem** o par, que é como a subcontagem do bug
    **#8** fica observável numa corrida.

    Lida só ``entities``; ``relationships`` (``WATCHED``, ``FAVORITE``) ficam de
    fora, porque o confronto é contra uma contagem de nós ou documentos.

    Parameters
    ----------
    model : EObject
        Raiz de um ``USchema``, do porte ou carregada de um XMI-oráculo.

    Returns
    -------
    dict of str to int
        Nome do ``EntityType`` para a soma dos ``count`` das suas variações.
    """
    return {entity.name: sum(v.count for v in entity.variations) for entity in model.entities}


def name_sides(message: str, subject: str, reference: str) -> str:
    """Trocar os rótulos posicionais do harness pelos nomes dos dois lados.

    O ``compare()`` herda do ``USchemaCompareMain`` os rótulos ``Schema1`` e
    ``Schema2``, que não dizem qual lado é o porte. Como a convenção é
    ``compare(referência, porte)`` em todos os pontos de chamada, ``Schema1`` é
    sempre a referência e ``Schema2`` sempre o sujeito.

    A substituição é feita **na gravação**, não no harness: os rótulos do
    ``equivalence.py`` são fiéis ao Java, e a legibilidade é requisito do CSV de
    evidência, não do comparador.

    Parameters
    ----------
    message : str
        Mensagem crua de :class:`~uschema.validation.equivalence.Divergence`.
    subject : str
        Quem está sendo avaliado — hoje sempre ``port``.
    reference : str
        Contra o que se comparou: ``resources`` ou ``seeded_oracle``.

    Returns
    -------
    str
        Mensagem com os dois lados nomeados.

    Examples
    --------
    >>> name_sides("Count differs: Schema1 A.1 has 4, Schema2 A.2 has 7",
    ...            "port", "resources")
    'Count differs: resources A.1 has 4, port A.2 has 7'
    """
    return message.replace("Schema1", reference).replace("Schema2", subject)


def _check_header(path: Path, header: list[str]) -> bool:
    """Recusa append num CSV de esquema antigo; True se o arquivo é novo.

    Arquivo de zero byte conta como novo: não tem esquema com que conflitar, e é
    o que sobra de uma corrida interrompida antes do primeiro `flush`.
    """
    if not path.exists() or path.stat().st_size == 0:
        return True

    with open(path, newline="") as file:
        current = next(csv.reader(file), [])

    if current != header:
        raise SystemExit(
            f"{path} tem cabeçalho incompatível.\n"
            f"  esperado: {','.join(header)}\n"
            f"  achado:   {','.join(current)}\n"
            "Renomeie o arquivo antigo ou use outro diretório."
        )

    return False


def _load_keys(path: Path, columns: tuple[str, ...]) -> set[tuple[str, ...]]:
    """Ler as chaves já gravadas num CSV, para recusar a segunda gravação.

    Lê o arquivo inteiro uma vez, na abertura. As tabelas têm dezenas de linhas
    por suíte — o custo é irrelevante e a alternativa seria descobrir a duplicata
    na análise, depois de a bateria ter rodado uma hora.
    """
    if not path.exists() or path.stat().st_size == 0:
        return set()

    with open(path, newline="") as file:
        return {tuple(row[column] for column in columns) for row in csv.DictReader(file)}


class Results:
    """Escreve as quatro tabelas em append, num diretório.

    Abre os quatro arquivos de uma vez e mantém um ``DictWriter`` por tabela —
    estado real, daí ser classe. O `DictWriter` deixa cada bateria passar só as
    colunas que ela produz; o resto sai vazio, sem `None` espalhado no CSV.

    Cada linha é gravada com `flush` imediato: uma bateria de uma hora
    interrompida no meio preserva o que já mediu.

    Na abertura, além do cabeçalho, carrega as **chaves** já gravadas (:data:`KEYS`)
    e recusa regravá-las: o `run_id` é determinístico, então repetir uma bateria
    no mesmo diretório duplicaria em append, sem erro nenhum.

    Examples
    --------
    >>> with Results(Path("results")) as tables:  # doctest: +SKIP
    ...     tables.add_run({"run_id": "size-neo4j-movies_min-23", ...})
    """

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self._files: dict[str, TextIO] = {}
        self._writers: dict[str, csv.DictWriter[str]] = {}
        self._keys: dict[str, set[tuple[str, ...]]] = {}

    def __enter__(self) -> "Results":
        """Abrir as quatro tabelas, escrevendo o cabeçalho nas que forem novas."""
        self.directory.mkdir(parents=True, exist_ok=True)

        for name, header in TABLES.items():
            path = self.directory / f"{name}.csv"

            is_new = _check_header(path, header)

            columns = KEYS.get(name)
            if columns is not None:
                self._keys[name] = set() if is_new else _load_keys(path, columns)

            file = open(path, "a", newline="")
            writer = csv.DictWriter(file, fieldnames=header, restval="")

            if is_new:
                writer.writeheader()
                file.flush()

            self._files[name] = file
            self._writers[name] = writer

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Fechar os quatro arquivos."""
        for file in self._files.values():
            file.close()

    def _reject_duplicate(self, table: str, row: Mapping[str, Any]) -> None:
        """Falhar se a chave desta linha já está no arquivo.

        Antes da gravação, não depois: a bateria para na primeira duplicata, com
        o CSV ainda íntegro, em vez de terminar produzindo um arquivo que a
        análise conta em dobro.
        """
        columns = KEYS.get(table)

        if columns is None:
            return

        key = tuple(str(row[column]) for column in columns)

        if key in self._keys[table]:
            raise SystemExit(
                f"{self.directory / f'{table}.csv'} já tem {'+'.join(columns)}"
                f" = {'+'.join(key)}.\n"
                "Esta corrida já foi medida neste diretório — gravar de novo duplicaria.\n"
                "  Arquive o diretório antes de repetir:\n"
                "    mv results results_$(date +%d-%m)"
            )

        self._keys[table].add(key)

    def _write(self, table: str, row: Mapping[str, Any]) -> None:
        """Gravar uma linha e descarregar no disco."""
        self._reject_duplicate(table, row)
        self._writers[table].writerow(row)
        self._files[table].flush()

    def add_run(self, row: Mapping[str, Any]) -> None:
        """Gravar a linha de uma corrida do **porte**.

        Só o porte entra aqui; o oráculo tem :meth:`add_oracle`. Por isso
        ``run_id`` é chave sozinho e as três parcelas de tempo nunca são vazias.
        """
        self._write("runs", row)

    def add_oracle(self, run: str, seconds: float, query_time: float) -> None:
        """Gravar o relógio de parede do oráculo Java para uma corrida.

        Tabela própria: o container não separa extração de inferência, então o
        único número que ele devolve é o total do ``docker run`` — que inclui
        ~18s de boot de Maven, JVM e Spark. Junta com ``runs`` por ``run_id``.

        O ``query_time`` não é regravado aqui — mora em ``runs.csv``, e é o
        **mesmo** divisor dos dois lados, porque a query roda uma vez por
        instância. Só o ``normalized`` entra, para que porte e oráculo sejam
        lidos lado a lado contra a Table 4 do artigo.
        """
        self._write(
            "oracle",
            {
                "run_id": run,
                "total_time": format_seconds(seconds),
                "normalized": normalized(seconds, query_time),
            },
        )

    def add_comparison(
        self, run: str, subject: str, reference: str, result: ComparisonResult
    ) -> None:
        """Gravar um confronto **e todas as suas divergências**.

        As duas tabelas são escritas juntas, num método só, porque os
        contadores de ``comparisons`` são a contagem das linhas de
        ``divergences``: separá-los deixaria as duas versões do mesmo fato
        livres para divergir.

        Parameters
        ----------
        run : str
            Identificador da corrida.
        subject : str
            Quem está sendo avaliado — hoje sempre ``PORT``.
        reference : str
            ``RESOURCES`` ou ``SEEDED_ORACLE``.
        result : ComparisonResult
            Saída do ``compare()``. O veredito e a fatalidade de cada
            divergência são lidos daqui, não recalculados.
        """
        fatal = [d for d in result.divergences if d.fatal]

        self._write(
            "comparisons",
            {
                "run_id": run,
                "subject": subject,
                "reference": reference,
                "equivalent": result.equivalent,
                "fatal_divergences": len(fatal),
                "non_fatal_divergences": len(result.divergences) - len(fatal),
            },
        )

        self._add_divergences(run, subject, reference, result.divergences)

    def _add_divergences(
        self, run: str, subject: str, reference: str, divergences: Iterable[Divergence]
    ) -> None:
        """Gravar o detalhe de um confronto, uma linha por divergência."""
        for divergence in divergences:
            self._write(
                "divergences",
                {
                    "run_id": run,
                    "subject": subject,
                    "reference": reference,
                    "category": divergence.category.value,
                    "fatal": divergence.fatal,
                    "message": name_sides(divergence.message, subject, reference),
                },
            )

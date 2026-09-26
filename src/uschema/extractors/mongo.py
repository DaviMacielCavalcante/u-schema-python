"""Extrator MongoDB — porte de ``Helpers.java`` (simplify/generateDocumentPair/reducePairs).

Porte fiel de ``mongodb2uschema/utils/Helpers.java`` (**não** o pacote `.spark`,
que é um caminho paralelo não usado pelo oráculo — ver o achado no topo de
``todolist_fase2.md``). O pipeline original (``MongoDB2USchema.java:73-83``):

.. code-block:: java

    JavaMongoRDD<Document> rddCollection = MongoSpark.load(jsc);
    rddCollection
        .mapToPair(doc -> Helpers.generateDocumentPair(doc))
        .reduceByKey((t1, t2) -> Helpers.reducePairs(t1, t2))
        .collect().stream()
        .map(pair -> { pair._1.put(typeField, collectionName); return ...; })

Este módulo porta o `map`/`reduce` inteiro (``simplify``, ``generate_document_pair``,
``reduce_pairs``, e o agrupamento por assinatura em ``build_triples``) como
funções puras, sem I/O — recebem os documentos já materializados (uma lista,
ou o cursor do ``pymongo``), não abrem conexão.

A conexão em si — porte de ``MongoDB2USchema.process``/``processEntity``
(``:48-58``/``:60-86``) e de ``MongoDB2USchemaMain.run`` (``:45-59``) — é
``extract_database_triples``/``extract_triples``, no fim do módulo: driver
nativo (``pymongo``), não o conector Spark; ver a seção seguinte. O
``extract_triples`` tem ainda um backend Spark opcional (Fase 4.1), descrito na
última seção.

Por que driver nativo, não conector Spark
------------------------------------------
Investigado em ``fase2_decisao_leitura_mongo_neo4j.md``: o oráculo lê via API
RDD do conector antigo (``MongoSpark.load``), que devolve ``org.bson.Document``
cru — nunca passa por DataFrame nem por inferência de schema. Nenhum conector
Spark vivo hoje (Mongo v11.x, Neo4j v6.0.0) ainda expõe essa API; os dois
viraram DataFrame-only. Ler direto via ``pymongo`` é a reconstrução mais fiel
do mecanismo do oráculo disponível — não um desvio dele.

Como os tipos viajam (a decisão central deste módulo)
-------------------------------------------------------
``Helpers.simplify`` apaga cada valor de folha, trocando por uma sentinela do
seu tipo (``Constants.java:18-23``). O oráculo produz o esqueleto final via
``Document.toJson()``, que serializa ``ObjectId`` como **objeto** extended-JSON
(``{"$oid": "..."}``) — e, como este projeto descobriu ao investigar o driver
Java (``JsonWriterSettings``, modo ``STRICT``, ``fase2_decisao_leitura_mongo_neo4j.md``),
serializa ``int64`` do mesmo jeito (``{"$numberLong": "..."}``). Os dois viram
**entidades agregadas** na inferência (Fase 1), não atributos primitivos.

Este módulo produz esses objetos **diretamente no ``dict`` nativo** — não via
``bson.json_util.dumps()`` + ``json.loads()``, que reintroduziria uma camada de
texto JSON que a Fase 1.5 já eliminou de propósito (o ``abstractjson`` do Java
não tem equivalente no porte; ver ``extractors/triple.py``).

Como o agrupamento por assinatura funciona (build_triples)
-------------------------------------------------------------
O `reduceByKey` do Java (``MongoDB2USchema.java:76-77``) agrupa pela **chave**
``pair._1`` — o ``Document`` simplificado. ``org.bson.Document`` estende
``LinkedHashMap``, cujo ``equals``/``hashCode`` (herdados de ``AbstractMap``)
são **estruturais e independentes de ordem de chave**: dois documentos com os
mesmos pares chave/valor, em ordem diferente, são a mesma chave de grupo. Uma
``ArrayList`` aninhada, ao contrário, compara **por ordem** — ``[1, 2]`` e
``[2, 1]`` não são o mesmo array.

``build_triples`` reproduz isso agrupando por uma chave canônica —
``json.dumps(schema, sort_keys=True)``, em ``_group_key`` — que ordena chaves de objeto (como o
``equals`` de mapa faz) mas preserva a ordem de listas (como o ``equals`` de
``ArrayList`` faz). O `_type` (``MongoDB2USchema.java:82``, atributo
`typeField`) é anexado **depois** do agrupamento, igual ao Java — incluí-lo
antes não mudaria o resultado neste caso (é a mesma string pra toda coleção),
mas a ordem importa como documentação do comportamento original.

A armadilha de tradução (Int64/bool são subclasses de int em Python)
-----------------------------------------------------------------------
No Java, ``Boolean``/``Integer``/``Long`` são classes disjuntas — a ordem dos
``instanceof`` em ``Helpers.java`` não importa. Em Python isso não vale:
``bson.Int64`` é subclasse de ``int`` (existe justamente pra forçar codificação
BSON ``int64`` em vez de ``int32``), e ``bool`` também é subclasse de ``int``
(fato da linguagem). Um despacho que testasse ``isinstance(v, int)`` antes de
``Int64``/``bool`` devolveria ``0`` silenciosamente onde o oráculo produz
``{"$numberLong": "0"}``, e ``0`` onde produz ``false`` — sem nenhum erro.
``_simplify_value`` testa **``Int64`` → ``bool`` → ``int`` genérico**, nessa
ordem, por isso.

Backend Spark (Fase 4.1)
------------------------
Com uma ``SparkSession`` (``spark=None`` é o caminho Python de sempre), o
``extract_triples`` usa o Spark para paralelizar o map-reduce, nunca para ler
ou tipar. Cada partição recebe uma fatia ``(coleção, inferior, superior)`` de
:mod:`uschema.extractors.partition`, abre o próprio ``MongoClient`` e roda o
``generate_document_pair`` ali dentro (``_read_partition``). O que volta ao
Spark é só ``str`` e ``int``: o BSON não sai do executor.

O ``reduceByKey`` recebe o ``reduce_pairs`` sem *wrapper*, porque a chave é
``(coleção, _group_key(schema))`` e o valor é só a tripla de dados. Duas
consequências, as duas deliberadas:

- **A coleção entra na chave.** As fatias de todas as coleções dividem o mesmo
  RDD; sem ela, duas coleções com o mesmo schema virariam uma tripla só. No
  caminho Python isso não aparece, porque o ``build_triples`` roda por coleção.
- **O schema volta por ``json.loads`` da chave**, com as chaves de objeto
  ordenadas em vez da ordem do documento. Nada muda a jusante: a inferência
  ordena os campos antes de usar (o ``TreeSet`` de
  ``SchemaInference.java:190-194``).

A ordem das triplas **não** é a do caminho Python: ela sai do hash do
``reduceByKey``. Os dois backends são iguais como conjunto; igualar a ordem é a
Fase 4.3 (``todolist_fase4.md``).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING, Any

from bson import ObjectId
from bson.int64 import Int64
from pymongo import MongoClient
from pymongo.database import Database

from uschema.extractors.partition import id_boundaries, ranges_from_boundaries, slice_filter

if TYPE_CHECKING:
    # Só para anotação: um import real faria o caminho Python carregar
    # pyspark/py4j a cada import do módulo, sem usar nada deles.
    from pyspark.sql import SparkSession

__all__ = [
    "SIMPLE_DEFAULT_LONG",
    "SIMPLE_DEFAULT_OBJECTID",
    "TYPE_FIELD",
    "build_triples",
    "extract_database_triples",
    "extract_triples",
    "generate_document_pair",
    "reduce_pairs",
    "simplify",
]

#: Nome do atributo de tipo injetado em cada esqueleto agregado
#: (``typeField``/``config.getTypeMarkerAttribute()``, ``MongoDB2USchema.java:82``).
#: Mesma convenção usada em toda a Fase 1 — ver ``extractors/triple.py``.
TYPE_FIELD = "_type"

#: Sentinela de string/data (``Constants.java:18``, ``SIMPLE_DEFAULT_STRING``).
_SIMPLE_DEFAULT_STRING = ""
#: Sentinela de booleano (``Constants.java:19``, ``SIMPLE_DEFAULT_BOOLEAN``).
_SIMPLE_DEFAULT_BOOLEAN = False
#: Sentinela de inteiro 32-bit (``Constants.java:20``, ``SIMPLE_DEFAULT_INTEGER``).
_SIMPLE_DEFAULT_INTEGER = 0
#: Sentinela de ponto flutuante (``Constants.java:21``, ``SIMPLE_DEFAULT_DOUBLE``).
_SIMPLE_DEFAULT_DOUBLE = 0.0

#: Sentinela de ``ObjectId`` (``Constants.java:23``, ``SIMPLE_DEFAULT_OBJECTID``)
#: já na forma que ``Document.toJson()`` produz — um objeto extended-JSON, não
#: uma string. Vira entidade agregada na inferência (Fase 1), com um atributo
#: ``$oid`` do tipo string. Ver ``extractors/triple.py`` para a análise completa
#: de por que ``ObjectIdSC`` é código morto no caminho Spark.
SIMPLE_DEFAULT_OBJECTID: dict[str, str] = {"$oid": "000000000000000000000000"}

#: Equivalente pra ``int64``/``bson.Int64`` — **não existe** em
#: ``Constants.java`` porque o Java nunca precisou dessa sentinela como objeto
#: (``Long`` tem sua própria sentinela escalar, ``0L``, linha 22). A
#: necessidade de tratar ``int64`` como objeto agregado é um achado deste
#: porte (``fase2_decisao_leitura_mongo_neo4j.md``): o mesmo ``Document.toJson()``
#: que serializa ``ObjectId`` como ``{"$oid": ...}`` serializa ``long`` como
#: ``{"$numberLong": ...}`` em modo ``STRICT`` — o modo que o driver do
#: conector 2.4.1 usa por padrão. Sem fixture de golden-master ainda (nenhum
#: XMI de referência tem campo ``long``); cobrir com teste dedicado.
SIMPLE_DEFAULT_LONG: dict[str, str] = {"$numberLong": "0"}

#: Uma fatia do backend Spark: ``(coleção, inferior, superior)``, com os limites
#: de :func:`~uschema.extractors.partition.ranges_from_boundaries`.
_Slice = tuple[str, Any, Any]

#: Chave do ``reduceByKey``: ``(coleção, esqueleto canônico)``. A coleção entra
#: porque as fatias de todas as coleções dividem o mesmo RDD.
_GroupKey = tuple[str, str]


def reduce_pairs(first: tuple[int, int, int], second: tuple[int, int, int]) -> tuple[int, int, int]:
    """Combinar duas triplas (firstTimestamp, lastTimestamp, count) em uma só.

    Porte fiel de ``Helpers.reducePairs`` (``Helpers.java:79-85``): é a função
    de combinação do ``reduceByKey`` — chamada toda vez que dois documentos
    caem no mesmo grupo (mesmo esqueleto), pra fundir seus dados de janela
    temporal e contagem.

    Parameters
    ----------
    first, second : tuple of (int, int, int)
        Duas triplas ``(firstTimestamp, lastTimestamp, count)`` do mesmo grupo.

    Returns
    -------
    tuple of (int, int, int)
        A tripla combinada: menor ``firstTimestamp``, maior ``lastTimestamp``,
        soma dos ``count``.
    """
    min_tuple = min(first[0], second[0])
    max_tuple = max(first[1], second[1])
    result = first[2] + second[2]

    return (min_tuple, max_tuple, result)


def simplify(document: Mapping[str, Any]) -> dict[str, Any]:
    """Apagar os valores de um documento, trocando cada folha por uma sentinela de tipo.

    Ponto de entrada público — porte de ``Helpers.simplify(Document)``
    (``Helpers.java:18-27``). Documentos sempre entram aqui como ``dict``
    (nunca uma folha solta), então o tipo de retorno é concreto
    (``dict[str, Any]``), diferente do despacho recursivo interno
    (:func:`_private_simplify`), que aceita qualquer coisa.

    Parameters
    ----------
    document : dict of str to Any
        Um documento cru do MongoDB (já como ``dict`` Python, vindo do
        ``pymongo``).

    Returns
    -------
    dict of str to Any
        O mesmo documento, com cada valor de folha trocado pela sentinela do
        seu tipo.
    """
    private = _simplify_value(document)
    assert isinstance(private, dict)
    return private


def _simplify_value(simple_doc: Any) -> Any:
    """Despachar um valor qualquer (folha, dict aninhado ou lista) pra sua sentinela.

    Porte de ``Helpers.simplify(Object)`` (``Helpers.java:29-57``), o ramo
    recursivo que trata qualquer valor — não só documentos. Tipado como
    ``Any -> Any`` de propósito: ao contrário de :func:`simplify`, essa função
    é chamada recursivamente pra folhas, listas e dicts aninhados, então não
    tem como ter uma assinatura mais específica sem perder generalidade.

    A ordem dos ``isinstance`` **não é arbitrária** — ver a nota da docstring
    do módulo ("A armadilha de tradução"): ``bool`` e ``Int64`` precisam vir
    antes de ``int`` genérico, porque os dois são subclasses de ``int`` em
    Python (diferente do Java, onde ``Boolean``/``Integer``/``Long`` são
    classes disjuntas e a ordem não importa).

    Parameters
    ----------
    simple_doc : Any
        Um valor de folha, um ``dict`` aninhado, ou uma ``list``.

    Returns
    -------
    Any
        A sentinela correspondente ao tipo (escalar), ou uma cópia recursiva
        (para ``dict``/``list``), ou ``None``.

    Raises
    ------
    TypeError
        Se o valor não for de nenhum tipo suportado — equivalente ao
        ``UnsupportedOperationException("Document field type not supported: " + input.getClass())``
        do Java (``Helpers.java:53-54``).
    """
    if isinstance(simple_doc, str | datetime):
        return _SIMPLE_DEFAULT_STRING
    elif isinstance(simple_doc, bool):
        # bool antes de int: bool é subclasse de int em Python.
        return _SIMPLE_DEFAULT_BOOLEAN
    elif isinstance(simple_doc, Int64):
        # Int64 antes de int, mesmo motivo — ver docstring do módulo.
        return SIMPLE_DEFAULT_LONG.copy()
    elif isinstance(simple_doc, int):
        return _SIMPLE_DEFAULT_INTEGER
    elif isinstance(simple_doc, float):
        return _SIMPLE_DEFAULT_DOUBLE
    elif isinstance(simple_doc, ObjectId):
        return SIMPLE_DEFAULT_OBJECTID.copy()
    elif isinstance(simple_doc, Mapping):
        # Documento aninhado: chave mantida como está, só o valor é
        # simplificado recursivamente — igual a
        # `simplified.put(key, simplify(doc.get(key)))` (Helpers.java:23-24).
        new_dict: dict[str, Any] = {}
        for keys, values in simple_doc.items():
            new_dict[keys] = _simplify_value(values)
        return new_dict
    elif isinstance(simple_doc, list):
        # Recursivo sem colapsar (Helpers.java:46-52) — o Spark mantém todos
        # os elementos do array; colapsar pra um só é comportamento do
        # extrator map-reduce (map.js), não deste.
        new_list = []
        for item in simple_doc:
            new_list.append(_simplify_value(item))
        return new_list
    elif simple_doc is None:
        return None
    else:
        raise TypeError(f"Document field type not supported: {type(simple_doc)}")


def generate_document_pair(
    document: Mapping[str, Any],
) -> tuple[dict[str, Any], tuple[int, int, int]]:
    """Gerar o par (esqueleto simplificado, dados de janela) de um documento.

    Porte de ``Helpers.generateDocumentPair`` (``Helpers.java:64-70``), com a
    correção do bug **#6** (``bugs_originais.md``): o Java assume, sem
    checagem, que ``_id`` é sempre um ``ObjectId``
    (``doc.getObjectId("_id").getTimestamp()``, ``Helpers.java:66``) — o que lança
    ``ClassCastException`` quando a coleção vem de uma fonte relacional (ex.:
    Northwind), onde ``_id`` pode ser um inteiro comum. Aqui, o timestamp só é
    extraído quando ``_id`` de fato é um ``ObjectId``; caso contrário, usa-se
    ``0`` como sentinela (não um valor real).

    Parameters
    ----------
    document : dict of str to Any
        Um documento cru da coleção.

    Returns
    -------
    tuple of (dict[str, Any], tuple[int, int, int])
        O par ``(esqueleto_simplificado, (firstTimestamp, lastTimestamp, count))``,
        com ``firstTimestamp == lastTimestamp`` e ``count == 1`` (um documento
        isolado ainda não foi combinado com nenhum outro).
    """
    doc_id = document.get("_id")
    # Bug #6: `_id` não é ObjectId (ex.: dado de origem relacional).
    # 0 é sentinela, não um timestamp real.
    time = int(doc_id.generation_time.timestamp()) if isinstance(doc_id, ObjectId) else 0
    data = (time, time, 1)
    return (simplify(document), data)


def _group_key(schema: dict[str, Any]) -> str:
    """Chave canônica de agrupamento de um esqueleto.

    Compartilhada pelos dois backends, para que agrupem pelo mesmo critério. Ver
    "Como o agrupamento por assinatura funciona" na docstring do módulo.

    Parameters
    ----------
    schema : dict of str to Any
        Esqueleto simplificado (saída de :func:`simplify`).

    Returns
    -------
    str
        O esqueleto serializado com as chaves de objeto ordenadas e a ordem das
        listas preservada.
    """
    dump = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    return dump


def _triple_row(
    schema: dict[str, Any], data: tuple[int, int, int], collection_name: str
) -> dict[str, Any]:
    """Anexar o ``_type`` ao esqueleto e montar a linha da tripla.

    É o passo final do ``processEntity`` (``MongoDB2USchema.java:78-83``), o único
    que o backend Spark executa no driver.

    Parameters
    ----------
    schema : dict of str to Any
        Esqueleto já agrupado. **É mutado**: recebe o ``TYPE_FIELD``.
    data : tuple of (int, int, int)
        ``(firstTimestamp, lastTimestamp, count)`` acumulado pelo :func:`reduce_pairs`.
    collection_name : str
        Vira o valor de ``TYPE_FIELD``.

    Returns
    -------
    dict of str to Any
        ``{"schema", "count", "firstTimestamp", "lastTimestamp"}``.
    """
    schema[TYPE_FIELD] = collection_name
    triple_value = {
        "schema": schema,
        "count": data[2],
        "firstTimestamp": data[0],
        "lastTimestamp": data[1],
    }
    return triple_value


def build_triples(
    documents: Iterable[Mapping[str, Any]], collection_name: str
) -> list[dict[str, Any]]:
    """Agrupar os documentos de uma coleção em triplas por esqueleto de tipos.

    Porte de ``MongoDB2USchema.processEntity`` (``MongoDB2USchema.java:60-86``),
    substituindo o ``mapToPair``/``reduceByKey`` do Spark por um agrupamento
    manual com chave canônica.

    A chave de agrupamento é ``json.dumps(esqueleto, sort_keys=True, separators=(",", ":"))``
    — **não** o par inteiro, e **não** o documento original. ``sort_keys=True``
    ordena as chaves de objeto (reproduzindo a igualdade estrutural e
    independente de ordem do ``Document``/``LinkedHashMap`` do Java), mas
    preserva a ordem de listas aninhadas (reproduzindo a igualdade
    dependente-de-ordem do ``ArrayList``). Ver a docstring do módulo,
    "Como o agrupamento por assinatura funciona".

    O campo de tipo (``TYPE_FIELD``) é injetado **depois** do agrupamento,
    igual ao Java (``MongoDB2USchema.java:82``).

    Parameters
    ----------
    documents : Iterable of Mapping[str, Any]
        Os documentos da coleção (tipicamente um cursor do ``pymongo``, por
        isso ``Iterable`` e não ``list`` — não exige materializar tudo antes).
    collection_name : str
        Nome da coleção; vira o valor do campo ``_type`` de cada esqueleto.

    Returns
    -------
    list of dict of str to Any
        Uma linha por esqueleto distinto encontrado na coleção, no formato
        ``{"schema", "count", "firstTimestamp", "lastTimestamp"}`` — pronta
        para ``extractors.triple.triples_from_rows``. Equivalente ao ``dict``
        que ``Helpers.documentPairToJSONNode`` monta (``Helpers.java:101-115``),
        só que sem o passo de texto JSON (``ObjectMapper``) do Java.
    """
    # Chave: esqueleto canônico (str). Valor: par (esqueleto, dados) acumulado.
    new_dict: dict[str, tuple[dict[str, Any], tuple[int, int, int]]] = {}
    for doc in documents:
        new = generate_document_pair(doc)
        # Só o esqueleto (new[0]) entra na chave — os dados (new[1]) mudam a
        # cada documento e não podem influenciar o agrupamento.
        dump = _group_key(new[0])
        if dump in new_dict:
            triple = reduce_pairs(new[1], new_dict[dump][1])
            new_dict[dump] = (new[0], triple)
        else:
            new_dict[dump] = new

    new_list = []
    for value in new_dict.values():
        new_list.append(_triple_row(value[0], value[1], collection_name))
    return new_list


def extract_database_triples(
    database: Database[Mapping[str, Any]], collections: Iterable[str]
) -> list[dict[str, Any]]:
    """Extrair as triplas de várias coleções de um mesmo banco.

    Porte da parte de ``MongoDB2USchema.process`` (``MongoDB2USchema.java:48-58``)
    que percorre a lista de coleções — sem a parte de inferência/construção do
    modelo (``si.infer``, ``builder.build``, escrita do ``.xmi``), que não é
    responsabilidade da camada de extração. O Java abre um
    ``JavaSparkContext`` novo a cada coleção do laço (``:63-71``) e fecha
    (``:84``) antes de seguir pra próxima; aqui isso não existe — o `find()`
    do PyMongo já é suficiente, sem Spark.

    Parameters
    ----------
    database : pymongo.database.Database
        Conexão já aberta com o banco.
    collections : Iterable of str
        Nomes das coleções a extrair, na mesma ordem em que
        ``MongoDB2USchemaMain.java:57`` monta a lista — o ``.split(",")`` da
        propriedade ``mongodb.collections``.

    Returns
    -------
    list of dict of str to Any
        As triplas de todas as coleções, combinadas numa lista só (uma
        coleção não influencia o agrupamento de outra — ``build_triples`` é
        chamada uma vez por coleção). Cada item tem as chaves ``"schema"``,
        ``"count"``, ``"firstTimestamp"`` e ``"lastTimestamp"``.
    """
    new_list = []
    for name in collections:
        new_list.extend(build_triples(database[name].find(), name))
    return new_list


def _read_partition(
    database_uri: str,
    database_name: str,
    partition: Iterable[_Slice],
) -> Iterator[tuple[_GroupKey, tuple[int, int, int]]]:
    """Ler as fatias de uma partição e emitir os pares já simplificados.

    Roda **no executor** (Fase 4.1). A conexão abre aqui dentro, e não no driver,
    porque conexão aberta não atravessa o *pickle*: o que viaja é a URI.

    A fronteira de tipagem fica aqui. O ``simplify`` roda antes de qualquer coisa
    voltar ao Spark, e o que sai é só ``str`` (a chave) e ``int`` (os dados). O
    BSON nunca sai do executor (item de fronteira da 4.0).

    Parameters
    ----------
    database_uri : str
        URI do MongoDB. Presa por ``functools.partial`` no driver.
    database_name : str
        Nome do banco. Presa do mesmo jeito.
    partition : Iterable of _Slice
        As fatias desta partição. Com ``parallelize(fatias, len(fatias))``, é uma
        só, mas a função não depende disso.

    Yields
    ------
    tuple of (_GroupKey, tuple of (int, int, int))
        ``((coleção, esqueleto canônico), (first, last, count))`` por documento.
        O ``reduceByKey`` combina esses pares depois.
    """
    with MongoClient[Mapping[str, Any]](database_uri) as mc:
        db = mc[database_name]

        for name, inferior, superior in partition:
            cursor = db[name].find(slice_filter(inferior, superior))

            for doc in cursor:
                schema, data = generate_document_pair(doc)

                key = (name, _group_key(schema))

                yield key, data


def _extract_triples_spark(
    spark: SparkSession,
    database_uri: str,
    database_name: str,
    collections: Iterable[str],
    slices: int | None,
) -> list[dict[str, Any]]:
    """Backend Spark de :func:`extract_triples` (Fase 4.1).

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
        Sessão já aberta; ver :func:`uschema.extractors.spark.local_session`.
    database_uri : str
        URI do MongoDB.
    database_name : str
        Nome do banco.
    collections : Iterable of str
        Coleções a extrair.
    slices : int or None
        Fatias **por coleção**. ``None`` = :func:`~uschema.extractors.spark.default_slices`.

    Returns
    -------
    list of dict of str to Any
        As mesmas linhas do backend Python, **como conjunto**: a ordem sai do
        hash do ``reduceByKey``, não da primeira aparição (tratado na 4.3).
    """
    # Import local de propósito: o topo do módulo não pode puxar o pyspark, que é
    # o que spark.py faz ao ser importado. Ver o bloco de imports.
    from uschema.extractors.spark import default_slices

    if slices is None:
        slices = default_slices()

    slices_list = []

    with MongoClient[Mapping[str, Any]](database_uri) as mc:
        for name in collections:
            ids_boundaries_list = id_boundaries(mc[database_name][name], slices)

            limits = ranges_from_boundaries(ids_boundaries_list)

            for inferior_limit, superior_limit in limits:
                slices_list.append((name, inferior_limit, superior_limit))

    if not slices_list:
        return []

    rdd = spark.sparkContext.parallelize(slices_list, len(slices_list))

    rdd_partitioned = rdd.mapPartitions(partial(_read_partition, database_uri, database_name))

    rdd_reduced = rdd_partitioned.reduceByKey(reduce_pairs)

    pairs_list = rdd_reduced.collect()

    rows = []

    for key, data in pairs_list:
        name = key[0]
        schema = json.loads(key[1])

        row = _triple_row(schema, data, name)

        rows.append(row)

    return rows


def extract_triples(
    database_uri: str,
    database_name: str,
    collections: Iterable[str],
    *,
    spark: SparkSession | None = None,
    slices: int | None = None,
) -> list[dict[str, Any]]:
    """Abrir a conexão com o MongoDB e extrair as triplas do banco indicado.

    Porte de ``MongoDB2USchemaMain.run`` (``MongoDB2USchemaMain.java:45-59``,
    a parte de conexão) — URI, nome do banco e coleções entram como
    parâmetros em vez de virem de um arquivo de propriedades (``config.properties``),
    já que este porte não usa o framework de injeção de dependência (Guice)
    do original.

    O ``with`` garante que a conexão feche mesmo se a extração falhar no meio.

    Parameters
    ----------
    database_uri : str
        URI de conexão do MongoDB.
    database_name : str
        Nome do banco de dados.
    collections : Iterable of str
        Nomes das coleções a extrair.
    spark : pyspark.sql.SparkSession or None, optional
        Backend. ``None`` (padrão) é o caminho Python de sempre. Uma sessão liga o
        backend Spark (Fase 4.1). A sessão vem aberta de fora para que a bateria
        meça o boot da JVM separado do trabalho (4.4).
    slices : int or None, optional
        Fatias por coleção no backend Spark; ignorado sem ``spark``.

    Returns
    -------
    list of dict of str to Any
        As triplas de todas as coleções indicadas, cada uma com as chaves
        ``"schema"``, ``"count"``, ``"firstTimestamp"`` e ``"lastTimestamp"``.
    """
    if spark is None:
        with MongoClient[Mapping[str, Any]](database_uri) as client:
            return extract_database_triples(client[database_name], collections)

    return _extract_triples_spark(spark, database_uri, database_name, collections, slices)

"""Extrator Neo4j — porte de ``SparkProcess``/``IdArchetypeMapping``/``TypeUtils``.

Porte fiel de ``neo4j2uschema/spark/*`` + ``neo4j2uschema/utils/TypeUtils.java``.
Este módulo cobre só a **camada de extração** (arquétipos por nó + contagem por
valor distinto) — a **camada de construção do modelo** (``USchemaBuilder``,
``StructuralVariationBuilder``, ``AttributeOptionalsChecker``,
``IgnoreSimilarReferenceBoundsProcessor``) é um módulo separado, porque **não é
a Fase 1**: ver o achado no topo de ``todolist_fase2.md`` §2.2.

Mecanismo do oráculo (``SparkProcess.java:50-121``)
-----------------------------------------------------
Duas *cypher*, não um ``find()`` direto:

1. ``MATCH (n) RETURN DISTINCT labels(n)`` (``:102-105``) — lista as combinações
   de labels existentes no banco.
2. Por combinação: ``MATCH (n:Labels) WHERE size(labels(n))=N WITH n OPTIONAL
   MATCH (n)-[r]->(m) RETURN n, r, labels(m)`` (``:107-114``) — uma linha por
   (nó, relacionamento de saída); ``OPTIONAL MATCH`` garante uma linha (com
   ``r``/``m`` nulos) mesmo pra nó sem saída.

O pipeline por linha, então:

.. code-block:: java

    .mapToPair(new IdArchetypeMapping())      // linha -> (id do nó, arquétipo da linha)
    .reduceByKey(new ReduceByIdArchetype())   // funde por nó: união das referências distintas
    .flatMap(new SplitMapping())              // nó -> [nó completo, cada referência distinta]
    .countByValue();                          // conta ocorrências de cada valor exato

Este módulo porta isso como ``node_archetype`` (:func:`node_archetype`),
``reduce_archetypes_by_node`` e ``build_archetype_counts`` — funções puras,
sem I/O — mais ``extract_database_archetype_counts``/``extract_archetype_counts``
(a conexão via ``neo4j`` nativo).

Por que **sem** o round-trip de texto JSON (e o que isso exige preservar)
----------------------------------------------------------------------------
``SplitMapping.generateJsonText`` serializa cada arquétipo pra **texto**;
``.countByValue()`` conta strings exatas; ``Json2USchemaModel`` faz
``new JSONObject(string)`` pra **reler**. Este porte pula o texto — mesma
decisão do Mongo (``extractors/mongo.py``, "Como os tipos viajam") — e produz
os arquétipos como ``dict`` nativo direto, agrupando por uma chave canônica
(:func:`_canonical_key`, igual a ``extractors.mongo._canonical_schema_key``).

**Isso só é seguro porque duas consequências observáveis do round-trip foram
verificadas na fonte e são replicadas explicitamente em** :func:`get_type_name`
(não em :func:`obtain_type` — a contagem/agrupamento acontece **antes** do
round-trip no Java, então usa os tipos "crus"; só a nomeação do tipo do
``Attribute``, que só acontece depois de reler o texto, é afetada):

1. **``Long`` (inteiro Neo4j) sempre vira ``"integer"``, nunca ``"long"``.**
   A sentinela de ``INTEGER`` é sempre o valor fixo ``0L`` (``TypeUtils.java:31``,
   ``SHORT_LONG``); ``Long.toString(0L)`` é o texto ``"0"``, sem sufixo; ao
   reler, ``org.json`` (``json:20180130``, verificado no fonte de
   ``JSONObject.stringToValue`` — tenta ``Integer`` antes de ``Long``, só sobe
   pra ``Long``/``BigInteger`` se o valor não couber em 32 bits) devolve
   sempre ``Integer``, nunca ``Long``, pro texto ``"0"``. Logo
   ``TypeUtils.geetSimpleType`` (que testa ``instanceof Long`` **antes** de
   ``instanceof Integer``) nunca vê um ``Long`` de verdade — o ramo ``"long"``
   é **código morto** no caminho real. `getTypeName` é chamado só sobre valores
   já relidos (``StructuralVariationBuilder.java:85``,
   ``TypeUtils.getTypeName(properties.get(name))`` — ``properties`` é sempre um
   ``JSONObject`` parseado, nunca o ``TreeMap`` cru).
2. **Lista heterogênea ou vazia sempre vira ``"string[]"``, nunca ``"any[]"``.**
   ``TypeUtils.obtainType(Iterable)`` devolve o literal ``"any"`` (uma
   ``String`` Java) quando os tipos não convergem pra 1 só (inclui lista
   vazia, 0 tipos distintos). Essa string, embutida no array-literal que
   ``addProperties`` monta (``["any"]``) e revertida por
   ``generateJsonText``/``JSONArray`` na releitura, chega em
   ``TypeUtils.getTypeName`` como uma ``String`` Java comum — e
   ``geetSimpleType`` não olha o *conteúdo* da string, só o *tipo* — qualquer
   ``String`` vira ``"string"``. O marcador ``"any"`` nunca sobrevive ao
   round-trip como um tipo especial.

Como :func:`obtain_type` já devolve tipos Python "nativos" (``bool``/``str``/
``float``/``int``, nunca uma distinção "era ``Long``"), e :func:`get_type_name`
despacha **pelo tipo Python do valor, não pelo conteúdo**, as duas
consequências acima saem **de graça** do despacho por tipo — não é preciso
nenhuma lógica especial pra "fingir" o round-trip. Isso só funciona porque as
duas conclusões acima foram verificadas contra o fonte real do ``org.json``
pinado (`JSONObject.stringToValue`, tag ``20180130``) — não são suposição.

Uma armadilha de tradução igual à do Mongo (``bool``/``int``)
------------------------------------------------------------------
Em Python, ``bool`` é subclasse de ``int`` (mesma armadilha de
``extractors/mongo.py``). :func:`obtain_type` despacha ``list`` → ``bool`` →
``str`` → ``float`` → ``int``, nessa ordem — trocar ``bool``/``int`` faz
booleano virar sentinela de inteiro em silêncio. A deduplicação de tipos numa
lista heterogênea (:func:`_list_sentinel`) tem o mesmo cuidado:
``False == 0`` em Python, então comparar por igualdade de valor confundiria
``[True, 5]`` com um array homogêneo; a chave de deduplicação inclui o
``type()`` do valor, não só o valor.

Uma assimetria do oráculo a preservar (labels próprios vs. labels do alvo)
------------------------------------------------------------------------------
``IdArchetypeMapping.nodeToJSONObject`` ordena os **próprios** labels do nó
antes de montar o campo ``labels`` (``.sorted()``, ``:60-62``) — e é esse valor
ordenado que vira o nome do ``EntityType`` (``StructuralVariationBuilder``,
``String.join("_AND_", labels)``). Mas ``addRelationships`` **não** ordena
``targetLabels`` (``:100-104``, sem ``.sorted()``) — o ``refsTo`` de uma
referência usa a ordem crua que ``labels(m)`` devolveu. Pra um nó
multi-label que também é alvo de alguma relação, isso pode gerar **dois**
``EntityType`` nominalmente diferentes pro mesmo nó (um pela ordem "própria",
outro pela ordem "de referência") — **confirmado com dado real** (Neo4j Aura,
27/07/2026, via ``scripts/check_extraction_neo4j.py``): um nó ``:Zebra:Apple``
produziu ``Apple_AND_Zebra`` (variação real) e ``Zebra_AND_Apple`` (placeholder
vazio) como dois ``EntityType`` distintos. Catalogado como ``N1`` em
``bugs_originais.md``. Este módulo **preserva a assimetria**
(:func:`node_archetype` ordena; :func:`_relationship_archetype` não) — decisão
de fidelidade, não um bug do porte.

Tipos Neo4j sem sentinela dedicada
-------------------------------------
``TypeUtils.obtainType`` só reconhece ``LIST``/``BOOLEAN``/``STRING``/
``FLOAT``/``INTEGER`` (``TYPE_SYSTEM``); qualquer outro tipo Cypher
(``Duration``, ``Point``, ``Date``/``Time``/``DateTime``, ``ByteArray``) cai no
``return NULL`` final (``:47``) — a string literal ``"null"``, que sobrevive o
round-trip como ``String`` comum e vira ``"string"`` em :func:`get_type_name`.
Comportamento observável do oráculo: nenhuma exceção, silenciosamente vira
"string". Diferente do Mongo, que lança pra tipo não suportado
(``extractors.mongo.simplify``) — não uniformizar os dois; cada um replica o
que o respectivo oráculo faz.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, Protocol

from neo4j import READ_ACCESS, Driver, RoutingControl

__all__ = [
    "build_archetype_counts",
    "extract_archetype_counts",
    "extract_database_archetype_counts",
    "get_type_name",
    "node_archetype",
    "obtain_type",
    "reduce_archetypes_by_node",
]

#: Sentinela pra tipo Cypher fora de LIST/BOOLEAN/STRING/FLOAT/INTEGER
#: (``TypeUtils.java:47``, ``NULL``). É uma *string*, não ``None`` — sobrevive
#: ao round-trip de JSON do oráculo como texto comum (ver docstring do módulo).
_SENTINEL_OTHER = "null"

#: Marcador de "tipos não convergem pra 1 só" numa lista
#: (``TypeUtils.java:58``, ``ANY``) — usado só dentro de :func:`_list_sentinel`;
#: nunca sobrevive como conceito distinto em :func:`get_type_name` (vira
#: ``"string"`` pelo despacho por tipo Python, não por conteúdo — ver docstring).
_ANY_MARKER = "any"


class _EntityLike(Protocol):
    """Contrato estrutural comum a nós e relacionamentos do driver ``neo4j``.

    Cobre só o que :func:`_entity_properties` precisa (nomes de propriedade e
    acesso por chave) — ``Node``/``Relationship`` reais do driver satisfazem
    isto, mas o Protocol evita depender das classes concretas do driver nos
    tipos deste módulo.
    """

    def keys(self) -> Iterator[str]: ...
    def __getitem__(self, key: str) -> Any: ...


class _NodeLike(_EntityLike, Protocol):
    """Contrato estrutural de um nó: ``_EntityLike`` mais identidade e labels.

    ``element_id`` identifica o nó entre as várias linhas do cypher (usado
    por :func:`reduce_archetypes_by_node` para agrupar); ``labels`` alimenta
    o campo ``labels`` do arquétipo em :func:`node_archetype`.
    """

    @property
    def element_id(self) -> str: ...
    @property
    def labels(self) -> Iterable[str]: ...


class _RelationshipLike(_EntityLike, Protocol):
    """Contrato estrutural de um relacionamento: ``_EntityLike`` mais o tipo da aresta.

    ``type`` é o nome do relacionamento Cypher (ex. ``"ACTED_IN"``), usado no
    campo ``type`` do arquétipo montado por :func:`_relationship_archetype`.
    """

    type: str


def obtain_type(value: Any) -> Any:
    """Apagar um valor de propriedade Neo4j, trocando-o pela sentinela do seu tipo.

    Porte de ``TypeUtils.obtainType`` (``TypeUtils.java:34-48``). Só reconhece os tipos
    do ``TYPE_SYSTEM`` (lista/booleano/string/float/inteiro) — qualquer outro cai
    em ``_SENTINEL_OTHER``, equivalente ao ``return NULL`` final do Java (ver o
    cabeçalho do módulo, "Tipos Neo4j sem sentinela dedicada").

    Parameters
    ----------
    value : Any
        Valor cru de uma propriedade do nó/relacionamento.

    Returns
    -------
    Any
        A sentinela do tipo — não o valor em si (ver o cabeçalho do módulo,
        "bool/int"). ``bool`` é testado **antes** de ``int`` porque ``bool`` é
        subclasse de ``int`` em Python. Pra qualquer tipo Cypher fora do
        ``TYPE_SYSTEM``, devolve ``_SENTINEL_OTHER`` sem lançar — mesmo
        comportamento observável do ``return NULL`` final (``TypeUtils.java:47``).
    """
    if isinstance(value, list):
        new_list = [_list_sentinel(value)]
        return new_list
    elif isinstance(value, bool):
        return False
    elif isinstance(value, str):
        return "s"
    elif isinstance(value, float):
        return 0.0
    elif isinstance(value, int):
        return 0
    else:
        return _SENTINEL_OTHER


def _list_sentinel(values: list[Any]) -> Any:
    """Derivar a sentinela de uma lista de propriedade, ou ``_ANY_MARKER`` se heterogênea.

    Parte de ``TypeUtils.obtainType(Iterable<Value>)`` (``TypeUtils.java:51-59``)
    pro ramo ``Iterable`` — ver o cabeçalho
    do módulo, "Uma armadilha de tradução" (o ``type()`` entra na chave de
    deduplicação porque ``False == 0`` em Python confundiria ``[True, 5]``
    com uma lista homogênea de inteiros).

    Parameters
    ----------
    values : list of Any
        Os elementos crus da propriedade de lista.

    Returns
    -------
    Any
        A sentinela do tipo único, se todos os elementos convergirem pro
        mesmo ``(type, valor)``; caso contrário, ``_ANY_MARKER`` (inclui
        lista vazia — zero tipos distintos).
    """
    new_dict: dict[tuple[type[Any], Any], Any] = {}
    for element in values:
        result = obtain_type(element)
        key = (type(result), _hashable(result))
        new_dict.setdefault(key, result)

    if len(new_dict) == 1:
        return next(iter(new_dict.values()))
    else:
        return _ANY_MARKER


def _hashable(sentinel: Any) -> Any:
    """Trocar uma sentinela por algo hasheável, pra uso como chave de deduplicação.

    ``list`` não é hasheável em Python (ao contrário do que a chave de
    :func:`_list_sentinel` precisa); listas viram ``tuple`` recursivamente,
    todo o resto passa direto.

    Parameters
    ----------
    sentinel : Any
        Uma sentinela devolvida por :func:`obtain_type` (escalar ou lista).

    Returns
    -------
    Any
        A mesma sentinela, com qualquer ``list`` aninhada trocada por ``tuple``.
    """
    if isinstance(sentinel, list):
        return tuple(_hashable(element) for element in sentinel)
    else:
        return sentinel


def _simple_type_name(sentinel: Any) -> str:
    """Nomear o tipo de uma sentinela escalar (nunca lista).

    Porte de ``TypeUtils.geetSimpleType`` (``TypeUtils.java:75-90``; nome com
    o typo do original) — o
    despacho escalar usado tanto direto por :func:`get_type_name` quanto,
    recursivamente, pro primeiro elemento de uma lista não-vazia. ``bool``
    antes de ``int`` pela mesma razão de sempre (subclasse em Python).

    Parameters
    ----------
    sentinel : Any
        Uma sentinela escalar (nunca uma ``list`` — quem despacha listas é
        :func:`get_type_name`).

    Returns
    -------
    str
        ``"boolean"``, ``"string"``, ``"double"``, ``"integer"``, ou
        ``_ANY_MARKER`` se o tipo não for nenhum desses.
    """
    if isinstance(sentinel, bool):
        return "boolean"
    elif isinstance(sentinel, str):
        return "string"
    elif isinstance(sentinel, float):
        return "double"
    elif isinstance(sentinel, int):
        return "integer"
    else:
        return _ANY_MARKER


def get_type_name(sentinel: Any) -> str:
    """Nomear o tipo de uma sentinela, como o oráculo faz **depois** do round-trip JSON.

    Porte de ``TypeUtils.getTypeName``/``geetSimpleType`` (``TypeUtils.java:62-90``).
    Só é chamado sobre valores já relidos de texto — ver o cabeçalho do módulo,
    "Por que sem o round-trip de texto JSON", itens 1 e 2, para as duas
    consequências que este despacho por tipo replica de graça (``Long`` sempre
    "integer", lista heterogênea/vazia sempre "string[]").

    Parameters
    ----------
    sentinel : Any
        Sentinela devolvida por :func:`obtain_type` (ou o elemento agregado de
        uma lista, via :func:`_list_sentinel`).

    Returns
    -------
    str
        Nome do tipo (``"string"``, ``"integer"``, ``"double"``, ``"boolean"``)
        ou, pra lista não-vazia, o nome do tipo do primeiro elemento com
        sufixo ``"[]"``. Pra lista vazia, esta função repassa a lista pra
        :func:`_simple_type_name`, que cai no seu próprio fallback e devolve
        ``_ANY_MARKER`` — equivalente ao ``else`` final de ``geetSimpleType``
        (``TypeUtils.java:89``); ``getTypeName`` em si não tem fallback
        próprio, só delega (``:71``). Inalcançável a partir de
        :func:`obtain_type` (que nunca devolve lista vazia), mantido por
        fidelidade ao ramo Java.
    """
    if isinstance(sentinel, list):
        if len(sentinel) > 0:
            type_name = _simple_type_name(sentinel[0])
            type_name = type_name + "[]"
            return type_name
        else:
            return _simple_type_name(sentinel)
    else:
        type_name = _simple_type_name(sentinel)
        return type_name


def _entity_properties(entity: _RelationshipLike | _NodeLike) -> dict[str, Any]:
    """Apagar todas as propriedades de um nó/relacionamento, trocando cada uma pela sua sentinela.

    Porte de ``IdArchetypeMapping.addProperties`` (``IdArchetypeMapping.java:70-82``).
    Usa ``.keys()`` em vez de
    iterar ``entity`` direto porque ``_EntityLike`` só declara ``.keys()``,
    não ``__iter__`` (ver o comentário na chamada, `# noqa: SIM118`).

    Parameters
    ----------
    entity : _RelationshipLike or _NodeLike
        O nó ou relacionamento cujas propriedades serão apagadas.

    Returns
    -------
    dict of str to Any
        Uma chave por nome de propriedade, cada valor trocado pela sentinela
        do seu tipo (:func:`obtain_type`).
    """
    return {key: obtain_type(entity[key]) for key in entity.keys()}  # noqa: SIM118


def _relationship_archetype(
    relationship: _RelationshipLike, target_labels: list[str]
) -> dict[str, Any]:
    """Montar o arquétipo de um relacionamento de saída.

    Porte de ``IdArchetypeMapping.addRelationships``
    (``IdArchetypeMapping.java:94-111``). Ao contrário de
    :func:`node_archetype`, **não** ordena ``target_labels`` — ver o
    cabeçalho do módulo, "Uma assimetria do oráculo a preservar" (é uma
    decisão de fidelidade ao bug ``N1`` do oráculo, não um descuido).

    Parameters
    ----------
    relationship : _RelationshipLike
        O relacionamento de saída.
    target_labels : list of str
        Labels do nó-alvo, na ordem crua devolvida por ``labels(m)`` (sem
        ordenar).

    Returns
    -------
    dict of str to Any
        Com as chaves ``type``, ``refsTo``, ``entity`` (sempre
        ``"relationship"``) e ``properties``.
    """
    new_dict = {
        "type": relationship.type,
        "refsTo": list(target_labels),
        "entity": "relationship",
        "properties": _entity_properties(relationship),
    }
    return new_dict


def _references(
    relationship: _RelationshipLike | None, target_labels: list[str] | None
) -> list[dict[str, Any]]:
    """Envolver o relacionamento de saída da linha numa lista de referências.

    Uma linha do cypher tem no máximo uma referência de saída (ver o
    cabeçalho do módulo); esta função normaliza isso pro formato de lista
    que :func:`node_archetype`/:func:`reduce_archetypes_by_node` esperam no
    campo ``references``.

    Parameters
    ----------
    relationship : _RelationshipLike or None
        O relacionamento de saída, ou ``None`` se o nó não tiver saída.
    target_labels : list of str or None
        Labels do nó-alvo, ou ``None`` junto com ``relationship=None``.

    Returns
    -------
    list of dict of str to Any
        Lista vazia se não houver relacionamento; caso contrário, uma lista
        com um único item — o arquétipo de :func:`_relationship_archetype`.
    """
    if relationship is None or target_labels is None:
        return []
    else:
        return [_relationship_archetype(relationship, target_labels)]


def node_archetype(
    node: _NodeLike, relationship: _RelationshipLike | None, target_labels: list[str] | None
) -> dict[str, Any]:
    """Montar o arquétipo de uma linha (nó + no máximo uma referência de saída).

    Porte de ``IdArchetypeMapping.nodeToJSONObject`` (``IdArchetypeMapping.java:57-68``)/
    ``addProperties``/``addRelationships``. Os labels **próprios** do nó são
    ordenados (``:60-62``) — diferente do ``refsTo`` de uma referência, que
    não é (ver "Uma assimetria do oráculo a preservar" no cabeçalho do
    módulo). Uma linha do cypher (ver o cabeçalho do módulo) traz
    um nó e, opcionalmente, um relacionamento de saída com seu nó-alvo; esta
    função monta o ``dict`` que representa essa combinação.

    Parameters
    ----------
    node : _NodeLike
        O nó da linha (sempre presente).
    relationship : _RelationshipLike or None
        O relacionamento de saída, se houver (``OPTIONAL MATCH`` pode devolver
        ``None``).
    target_labels : list of str or None
        Labels do nó-alvo do relacionamento, ou ``None`` junto com
        ``relationship=None`` (mesmo teste do Java, ``:43`` — os dois nulos
        juntos, nunca só um).

    Returns
    -------
    dict of str to Any
        Com as chaves ``labels`` (ordenados — ver o cabeçalho do módulo, "Uma
        assimetria do oráculo a preservar"), ``entity``, ``properties`` e
        ``references``.
    """
    new_dict = {
        "labels": sorted(node.labels),
        "entity": "node",
        "properties": _entity_properties(node),
        "references": _references(relationship, target_labels),
    }

    return new_dict


def _canonical_key(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def reduce_archetypes_by_node(
    rows: Iterable[tuple[str, dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Fundir as linhas de um mesmo nó num único arquétipo, unindo referências distintas.

    Porte de ``ReduceByIdArchetype.call`` (``ReduceByIdArchetype.java:16-23``).
    Cada linha de :func:`node_archetype` traz
    no máximo uma referência de saída; nós com várias arestas de saída viram
    várias linhas com o mesmo ``node_id``. Esta função funde essas linhas,
    mantendo os campos do nó (do primeiro arquétipo visto) e acumulando
    referências únicas (por chave canônica) na lista ``references``.

    Parameters
    ----------
    rows : iterable of (str, dict of str to Any)
        Pares ``(node_id, archetype)`` — tipicamente ``element_id`` do nó e o
        resultado de :func:`node_archetype`.

    Returns
    -------
    dict of str to dict of str to Any
        Um arquétipo fundido por ``node_id``.
    """
    merged: dict[str, dict[str, Any]] = {}
    seen_reference_keys: dict[str, dict[str, dict[str, Any]]] = {}
    for node_id, archetype in rows:
        if node_id not in merged:
            merged[node_id] = {**archetype, "references": []}
            seen_reference_keys[node_id] = {}
        for reference in archetype["references"]:
            key = _canonical_key(reference)
            if key not in seen_reference_keys[node_id]:
                seen_reference_keys[node_id][key] = reference
                merged[node_id]["references"].append(reference)
    return merged


def _tally(
    archetype: Mapping[str, Any], counts: dict[str, int], archetypes: dict[str, Mapping[str, Any]]
) -> None:
    """Contabilizar uma ocorrência de um arquétipo (nó ou referência) por chave canônica.

    Parte de :func:`build_archetype_counts`, equivalente a um passo do
    ``.countByValue()`` do Spark: incrementa a contagem da chave e guarda a
    primeira instância vista do arquétipo (todas as instâncias com a mesma
    chave canônica são estruturalmente idênticas, então a primeira serve
    pra representar o grupo). Muta ``counts``/``archetypes`` in-place.

    Parameters
    ----------
    archetype : mapping of str to Any
        O arquétipo (nó completo ou referência) a contabilizar.
    counts : dict of str to int
        Contagem acumulada por chave canônica; atualizado in-place.
    archetypes : dict of str to mapping of str to Any
        Primeira instância vista por chave canônica; atualizado in-place.
    """
    canon_key = _canonical_key(archetype)
    counts[canon_key] = counts.get(canon_key, 0) + 1
    archetypes.setdefault(canon_key, archetype)


def build_archetype_counts(merged: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Explodir cada nó fundido em si mesmo + suas referências, e contar ocorrências exatas.

    Porte de ``SplitMapping.call`` (``SplitMapping.java:30-41``) seguido de
    ``.countByValue()`` (``SparkProcess.java:99``). Cada arquétipo de
    nó fundido (de :func:`reduce_archetypes_by_node`) gera uma contagem para
    si próprio (com ``references`` completo) e uma contagem separada para
    cada referência distinta que ele carrega — replicando o
    ``flatMap``/``countByValue`` do Spark sem o round-trip de texto (ver o
    cabeçalho do módulo).

    Parameters
    ----------
    merged : mapping of str to mapping of str to Any
        Arquétipos fundidos por nó, como devolvidos por
        :func:`reduce_archetypes_by_node`.

    Returns
    -------
    list of dict of str to Any
        Um item por valor distinto (nó completo ou referência), cada um com
        as chaves ``archetype`` e ``count``.
    """
    counts: dict[str, int] = {}
    archetypes: dict[str, Mapping[str, Any]] = {}

    for archetype in merged.values():
        _tally(archetype, counts, archetypes)
        for reference in archetype["references"]:
            _tally(reference, counts, archetypes)
    return [{"archetype": archetypes[key], "count": count} for key, count in counts.items()]


def extract_archetype_counts(
    rows: Iterable[tuple[_NodeLike, _RelationshipLike | None, list[str] | None]],
) -> list[dict[str, Any]]:
    """Encadear o pipeline puro completo: linhas cruas -> arquétipos por nó -> contagens.

    Orquestra, em sequência, :func:`node_archetype`,
    :func:`reduce_archetypes_by_node` e :func:`build_archetype_counts` — o
    equivalente do ``.mapToPair().reduceByKey().flatMap().countByValue()`` de
    ``SparkProcess.java`` (ver o cabeçalho do módulo). Não faz I/O; recebe as
    linhas já lidas do banco (por :func:`extract_database_archetype_counts`,
    ou diretamente em teste).

    Parameters
    ----------
    rows : iterable of (_NodeLike, _RelationshipLike or None, list of str or None)
        Uma tupla ``(nó, relacionamento de saída, labels do alvo)`` por linha
        do cypher — ``relationship``/``target_labels`` são ``None`` juntos
        quando o nó não tem saída.

    Returns
    -------
    list of dict of str to Any
        As contagens finais, no formato de :func:`build_archetype_counts`.
    """
    per_row = (
        (node.element_id, node_archetype(node, relationship, target_labels))
        for node, relationship, target_labels in rows
    )
    merged = reduce_archetypes_by_node(per_row)
    return build_archetype_counts(merged)


def _distinct_label_combinations(driver: Driver, database: str | None) -> list[list[str]]:
    """Rodar a 1ª cypher do oráculo: listar as combinações de labels existentes no banco.

    Porte de ``generateLabelsMinMaxCountQuery``/``executeSimpleQuery``
    (chamadas em ``SparkProcess.java:60``; definições em ``:102-105``, a
    *query* ``MATCH (n) RETURN DISTINCT labels(n)``, e ``:116-121``, a
    execução via ``.collect()``, sem *map*/*reduce*) — ver o cabeçalho do
    módulo, "Mecanismo do oráculo".

    Parameters
    ----------
    driver : Driver
        Driver Neo4j nativo já conectado.
    database : str or None
        Nome do banco, ou ``None`` para o banco default.

    Returns
    -------
    list of list of str
        Uma lista de labels por combinação distinta encontrada.
    """
    result = driver.execute_query(
        "MATCH (n) RETURN DISTINCT labels(n)", database_=database, routing_=RoutingControl.READ
    )
    return [list(record[0]) for record in result.records]


def _read_label_combination(
    driver: Driver, database: str | None, labels: list[str], sampling_rate: float
) -> Iterator[tuple[_NodeLike, _RelationshipLike | None, list[str] | None]]:
    """Rodar a 2ª cypher do oráculo: ler as linhas de uma combinação de labels.

    Cada linha traz um nó e, opcionalmente, uma aresta de saída. Porte de
    ``generateLabels``/``generateQuery``/``executeQuery``
    (``SparkProcess.java:83-90``/``:107-114``/``:92-100``), a segunda das
    quais monta o texto ``MATCH (n:Labels) WHERE size(labels(n))=N WITH n
    OPTIONAL MATCH (n)-[r]->(m) RETURN n, r, labels(m)`` — ver o cabeçalho do
    módulo, "Mecanismo do oráculo". O ``OPTIONAL MATCH``
    garante uma linha (com ``r``/``m`` nulos) mesmo pra nó sem saída;
    ``sampling_rate`` entra como ``rand() < taxa`` no ``WHERE``, só quando
    diferente de ``1.0``.

    Lê em **streaming**, e não com ``driver.execute_query``
    ---------------------------------------------------------
    ``execute_query`` é *eager*: materializa a lista inteira antes de
    devolver. Num tamanho grande do User Profiles isso são milhões de
    registros, e o custo não é só memória — é **throughput**. Conforme a
    lista cresce, o processo passa mais tempo alocando e menos drenando o
    socket; a janela de recepção TCP fecha, e o servidor fica bloqueado
    esperando o cliente ler. Medido em 08/08/2026, mesma *query* e mesmo
    servidor, sobre 200 mil registros:

    ==================  =======  ==============  ========
    consumo             tempo    taxa            memória
    ==================  =======  ==============  ========
    streaming           6,3s     31.549 rec/s    constante
    ``execute_query``   39,6s    5.045 rec/s     424 MB
    ==================  =======  ==============  ========

    Com o servidor reportando ``rwnd_limited: 100,0%`` — ocioso, esperando o
    nosso processo. É a causa real do que ``bugs_originais.md`` catalogou
    como ``E1`` ("deleção massiva contamina a extração seguinte"): o gatilho
    não é a deleção nem o servidor, é o **tamanho do resultado** — por isso
    ``small``/``medium`` sempre passavam enquanto ``large``/``larger``
    colapsavam.

    Parameters
    ----------
    driver : Driver
        Driver Neo4j nativo já conectado.
    database : str or None
        Nome do banco, ou ``None`` para o banco default.
    labels : list of str
        A combinação de labels a filtrar (``size(labels(n)) == len(labels)``).
    sampling_rate : float
        Fração de relacionamentos de saída amostrados; ``1.0`` desliga a
        amostragem (sem cláusula extra no ``WHERE``).

    Yields
    ------
    tuple of (_NodeLike, _RelationshipLike or None, list of str or None)
        Uma tupla ``(nó, relacionamento de saída, labels do alvo)`` por
        linha do cypher; os dois últimos são ``None`` juntos quando o nó não
        tem saída.
    """
    label_pattern = "".join(f":`{label}`" for label in labels)
    query = (
        f"MATCH (n{label_pattern}) WHERE size(labels(n)) = $n_labels "
        + "WITH n OPTIONAL MATCH (n)-[r]->(m) "
        + (f"WHERE rand() < {sampling_rate} " if sampling_rate != 1.0 else "")
        + "RETURN n, r, labels(m)"
    )

    with driver.session(database=database, default_access_mode=READ_ACCESS) as session:
        for record in session.run(query, n_labels=len(labels)):
            yield (record[0], record[1], list(record[2]) if record[2] is not None else None)


def extract_database_archetype_counts(
    driver: Driver, database: str | None = None, sampling_rate: float = 1.0
) -> list[dict[str, Any]]:
    """Ponto de entrada com I/O: rodar as duas cypher do oráculo e extrair as contagens.

    Porte de ``SparkProcess.process`` (``SparkProcess.java:50-75``). Descobre as combinações de
    labels existentes (:func:`_distinct_label_combinations`), lê as linhas de
    cada combinação (:func:`_read_label_combination`) e alimenta
    :func:`extract_archetype_counts`.

    Parameters
    ----------
    driver : Driver
        Driver Neo4j nativo já conectado.
    database : str or None
        Nome do banco, ou ``None`` para o banco default.
    sampling_rate : float
        Fração de relacionamentos de saída amostrados (``rand() < taxa`` no
        ``WHERE``); ``1.0`` (default) desliga a amostragem — ver o cabeçalho
        de :func:`_read_label_combination`. Porte de ``SparkProcess``'s
        ``samplingRate`` (``:41-48``, ``IllegalArgumentException`` se fora de
        ``(0, 1]`` — aqui ``ValueError``). O oráculo que gerou os XMIs de
        referência sempre rodou com ``1.0`` (``Neo4j2USchemaMain.java:18``,
        ``SAMPLING_RATIO``).

    Returns
    -------
    list of dict of str to Any
        As contagens finais, no formato de :func:`build_archetype_counts`.

    Raises
    ------
    ValueError
        Se ``sampling_rate`` for ``<= 0`` ou ``> 1`` — porte de
        ``SparkProcess.java:43``.
    """
    if sampling_rate <= 0 or sampling_rate > 1:
        raise ValueError(f"Sampling rate <= 0 or > 1, Value: {sampling_rate}")

    def _rows() -> Iterator[tuple[_NodeLike, _RelationshipLike | None, list[str] | None]]:
        for label in _distinct_label_combinations(driver, database):
            yield from _read_label_combination(driver, database, label, sampling_rate)

    return extract_archetype_counts(_rows())

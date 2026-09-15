"""Construção do ``USchema`` do Neo4j — porte do núcleo de construção próprio.

Porte de ``neo4j2uschema/model/*`` (``USchemaBuilder``,
``StructuralVariationBuilder``, ``AttributeOptionalsChecker``,
``IgnoreSimilarReferenceBoundsProcessor``, ``repository/UModelRepository``).
**Não é a Fase 1** — não usa ``SchemaInference``/``USchemaModelBuilder``
(``inference/``); é um segundo núcleo, próprio do paradigma grafo. Ver o
achado no topo de ``todolist_fase2.md`` §2.2 e a docstring de
``extractors/neo4j.py``.

Entrada: a saída de ``extractors.neo4j.build_archetype_counts``/
``extract_archetype_counts``/``extract_database_archetype_counts`` — uma
``list[{"archetype": {...}, "count": N}]``, já em ``dict`` nativo (nunca texto
JSON — mesma decisão de design do resto do porte). Porte de
``Json2USchemaModel.processArchetypes`` (``Json2USchemaModel.java:39-51``):

.. code-block:: java

    uBuilder.createUSchema(databaseName);
    archetypesCounts.entrySet().forEach(entry -> {
        JSONObject entityArchetypeJson = new JSONObject(entry.getKey());
        variationBuilder.createVariation(entityArchetypeJson, entry.getValue());
    });
    attributeOptionalsChecker.processOptionals();
    similarReferenceProcessor.joinVariationsIgnoringBounds();

Aqui, sem o ``new JSONObject(entry.getKey())`` — o arquétipo já chega
estruturado; :func:`build_uschema_from_archetypes` é a fachada equivalente.

Fronteira mypy/PyEcore
-----------------------
``EObject`` é ``Any`` (PyEcore não distribui ``py.typed``) — mesmo aviso do
``inference/builder.py``/``inference/m2m.py``. Despacho de tipo por
``eClass.name``, nunca ``isinstance``, porque o objeto é do metamodelo Ecore,
não uma classe Python concreta.

Duas divergências propositais entre as duas classes do oráculo que checam tipo
------------------------------------------------------------------------------
Achado ao ler ``AttributeOptionalsChecker.getStringType``
(``:112-124``) contra ``IgnoreSimilarReferenceBoundsProcessor.getTypeRepresentation``
(``:193-210``): **as duas funções de "tipo como string" não são a mesma, e
uma delas está incompleta.**

- ``getStringType`` só reconhece ``PrimitiveType``/``PTuple`` — qualquer outro
  ``DataType`` (inclusive ``PList``, que é o que ``createType`` (
  ``USchemaBuilder.java:67-84``) **de fato produz** pra todo atributo array no
  caminho Neo4j, já que ``TypeUtils.getTypeName`` nunca gera uma string de
  tipo heterogêneo) cai no ``return EMPTY`` final. **Consequência**: a chave
  de deduplicação de ``attributeAsString`` (``:100-104``) pra um atributo
  array é sempre ``"nome:"`` — o tipo do elemento nunca entra na chave. Dois
  atributos de mesmo nome e arrays de tipos *diferentes* (`"tags": string[]`
  vs. `"tags": integer[]`) contam como o **mesmo** atributo pro
  ``MapCounter`` de opcionalidade. Replicado por construção em
  :func:`_string_type_for_optionality` — **não** estendida pra reconhecer
  ``PList``, mesmo sendo o caso real; seria uma correção, não um porte fiel.
- ``getTypeRepresentation`` **reconhece** ``PList``/``PSet``/``PTuple``/``PMap``
  recursivamente (usada só em ``IgnoreSimilarReferenceBoundsProcessor``, pra
  agrupar variações por assinatura completa de features). Portada à parte em
  :func:`_type_representation`, sem compartilhar código com
  :func:`_string_type_for_optionality` — são funções **genuinamente
  diferentes** no oráculo, não uma duplicação seguindo o DRY.

``addReferenceToCount``/``getReferenceCount`` não portados
-------------------------------------------------------------
``UModelRepository.java:82-105`` — confirmado por *grep* no módulo inteiro
(``neo4j2uschema/model``): ``addReferenceToCount`` é chamado (
``USchemaBuilder.createReference:104``, ``StructuralVariationBuilder.
getOrCreateReference:125``), mas ``getReferenceCount`` **nunca** é chamado em
nenhum caminho — o valor é escrito e nunca lido de volta. Código morto, mesma
política do ``OptionalTagger`` em ``inference/build_uschema.py``: não portar.
"""

from __future__ import annotations

import json
from typing import Any

from pyecore.ecore import EObject, EPackage

from uschema.extractors.neo4j import get_type_name

__all__ = ["build_uschema_from_archetypes"]

#: ``Constants.LABELS_JOINER`` — separador entre labels de um nó multi-label
#: no nome do ``EntityType`` (``String.join(LABELS_JOINER, labels)``,
#: ``StructuralVariationBuilder.java:49``).
_LABELS_JOINER = "_AND_"

#: ``Constants.ARRAY`` — sufixo de tipo array (``TypeUtils.getTypeName``,
#: ``extractors/neo4j.py``) e prefixo homônimo em
#: ``AttributeOptionalsChecker.getStringArrayType`` (``:126-136``, usados em
#: sentidos opostos nos dois lados — ver :func:`_create_type`).
_ARRAY_SUFFIX = "[]"


class _ModelRepository:
    """Índices por nome — porte de ``UModelRepository`` (armazenamento puro).

    Chaveado por **nome** (``str``), não pela instância ``EObject`` como o
    Java faz com ``RelationshipType`` — equivalente aqui, porque
    ``get_or_create_relationship_type`` já garante um nome único por
    instância (mesma relação 1:1 que o Java mantém via
    ``getOrCreateRefType``).
    """

    def __init__(self) -> None:
        self.schema: EObject | None = None
        self._entity_classes: dict[str, EObject] = {}
        self._reference_classes: dict[str, EObject] = {}
        #: Porte de ``referencesStructuralVariations``
        #: (``UModelRepository.java:16``) — chave externa é o nome do
        #: ``RelationshipType``, interna é ``properties.toString()``
        #: (aqui, :func:`_properties_key`).
        self._ref_variations: dict[str, dict[str, EObject]] = {}

    def get_entity_class(self, name: str) -> EObject | None:
        return self._entity_classes.get(name)

    def save_entity_class(self, entity: EObject) -> None:
        assert self.schema is not None
        self._entity_classes[entity.name] = entity
        self.schema.entities.append(entity)

    def get_reference_class(self, name: str) -> EObject | None:
        return self._reference_classes.get(name)

    def save_reference_class(self, relationship_type: EObject) -> None:
        assert self.schema is not None
        self.schema.relationships.append(relationship_type)
        self._reference_classes[relationship_type.name] = relationship_type
        self._ref_variations[relationship_type.name] = {}

    def get_ref_variation(self, relationship_type_name: str, properties_key: str) -> EObject | None:
        return self._ref_variations[relationship_type_name].get(properties_key)

    def save_ref_variation(
        self, relationship_type_name: str, properties_key: str, variation: EObject
    ) -> None:
        self._ref_variations[relationship_type_name][properties_key] = variation

    def get_ref_variation_id(self, relationship_type_name: str) -> int:
        """Porte de ``getRefVariationId`` (``UModelRepository.java:76-80``)."""
        return len(self._ref_variations[relationship_type_name]) + 1


class _USchemaBuilder:
    """Fábrica de ``EObject`` do metamodelo — porte de ``USchemaBuilder``.

    Parameters
    ----------
    pkg : EPackage
        Metamodelo carregado (:func:`~uschema.metamodel.registry.load_metamodel`).
    repository : _ModelRepository
        Onde ``get_or_create_*`` consulta/grava — mesma relação que
        ``USchemaBuilder``/``UModelRepository`` têm no Java.
    """

    def __init__(self, pkg: EPackage, repository: _ModelRepository) -> None:
        self._pkg: EPackage = pkg
        self._repository: _ModelRepository = repository

    def _create(self, class_name: str) -> EObject:
        eclass = self._pkg.getEClassifier(class_name)
        instance = eclass()
        return instance

    def create_uschema(self, name: str) -> None:
        """Porte de ``createUSchema`` (``USchemaBuilder.java:29-35``)."""
        created_schema = self._create("USchema")
        created_schema.name = name
        self._repository.schema = created_schema

    def get_or_create_entity_type(self, name: str) -> EObject:
        """Porte de ``getOrCreateEntityType`` (``:37-47``)."""
        entity_type = self._repository.get_entity_class(name)
        if entity_type is None:
            entity_type = self.create_entity_type(name)
            self._repository.save_entity_class(entity_type)
            return entity_type
        else:
            return entity_type

    def create_entity_type(self, name: str) -> EObject:
        """Porte de ``createEntityType`` (``:49-56``).

        ``root = True`` sempre — diferente do Mongo (``inference/builder.py``,
        raiz só se **alguma** variação vier raiz). No Neo4j **toda** entidade
        nasce raiz; não há noção de agregado embutido no caminho de
        construção deste módulo (nós são sempre entidades de primeira
        classe).
        """
        entity_type = self._create("EntityType")
        entity_type.name = name
        entity_type.root = True
        return entity_type

    def create_attribute(self, name: str, type_name: str) -> EObject:
        """Porte de ``createAttribute`` (``:58-65``)."""
        attribute = self._create("Attribute")
        attribute.name = name
        attribute.type = self._create_type(type_name)
        return attribute

    def _create_type(self, type_name: str) -> EObject:
        """Porte de ``createType`` (``:67-84``).

        ``type_name`` vem de ``TypeUtils.get_type_name`` — sufixo ``"[]"``
        indica array. ``PrimitiveType`` senão.
        """
        primitive_type = self._create("PrimitiveType")

        if type_name.endswith(_ARRAY_SUFFIX):
            primitive_type_name = type_name[: -len(_ARRAY_SUFFIX)]
            primitive_type.name = primitive_type_name
            plist = self._create("PList")
            plist.elementType = primitive_type
            return plist
        else:
            primitive_type.name = type_name
            return primitive_type

    def create_variation(self, variation_id: int, count: int) -> EObject:
        """Porte de ``createVariation`` (``:87-94``).

        Não seta ``firstTimestamp``/``lastTimestamp`` — o oráculo Neo4j
        também não (só ``count``/``variationId``); o metamodelo já defaulta
        ``ELong`` pra ``0`` sem precisar setar explicitamente (verificado:
        ``StructuralVariation().firstTimestamp == 0`` sem atribuição).
        """
        variation = self._create("StructuralVariation")
        variation.count = count
        variation.variationId = variation_id
        return variation

    def create_reference(self, type_name: str, refs_to: str, target_variation: EObject) -> EObject:
        """Porte de ``createReference`` (``:96-107``), sem ``addReferenceToCount``.

        ``lowerBound=0``/``upperBound=1`` aqui — o chamador
        (:meth:`_StructuralVariationBuilder._get_or_create_reference`) sobe
        ``lowerBound`` pra ``1`` logo em seguida, igual ao Java (``:117``).
        """
        reference = self._create("Reference")
        reference.name = type_name
        reference.lowerBound = 0
        reference.upperBound = 1
        reference.refsTo = self.get_or_create_entity_type(refs_to)
        reference.isFeaturedBy.append(target_variation)
        return reference

    def get_or_create_relationship_type(self, name: str) -> EObject:
        """Porte de ``getOrCreateRefType`` (``:109-120``)."""
        relationship_type = self._repository.get_reference_class(name)
        if relationship_type is None:
            relationship_type = self._create("RelationshipType")
            relationship_type.name = name
            self._repository.save_reference_class(relationship_type)

        return relationship_type


class _StructuralVariationBuilder:
    """O núcleo real — porte de ``StructuralVariationBuilder``.

    Parameters
    ----------
    builder : _USchemaBuilder
        Fábrica + ``get_or_create_*``.
    repository : _ModelRepository
        Índices de variação de relacionamento por propriedades.
    """

    def __init__(self, builder: _USchemaBuilder, repository: _ModelRepository) -> None:
        self._builder = builder
        self._repository = repository

    def create_variation(self, archetype: dict[str, Any], count: int) -> None:
        """Porte de ``createVariation`` (``StructuralVariationBuilder.java:41-55``).

        Despacha por ``archetype["entity"]`` — ``"node"`` ou
        ``"relationship"`` (ver ``extractors.neo4j.node_archetype``/
        ``_relationship_archetype``). Qualquer outro valor (nunca produzido
        pelo extrator) é ignorado, igual ao ``if``/``else if`` sem ``else``
        do Java.
        """
        entity = archetype.get("entity")

        if entity == "node":
            labels = _LABELS_JOINER.join(archetype["labels"])
            self._get_or_create_entity_type_and_variation(archetype, labels, count)
        elif entity == "relationship":
            self._get_or_create_ref_type_and_variation(archetype, archetype["type"], count)

    def _get_or_create_entity_type_and_variation(
        self, archetype: dict[str, Any], labels: str, count: int
    ) -> None:
        """Porte de ``getOrCreateEntityTypeAndVariation`` (``:57-68``)."""
        entity_type = self._builder.get_or_create_entity_type(labels)
        self._create_parent_entities(entity_type, labels)

        new_variation_id = len(entity_type.variations) + 1
        variation = self._builder.create_variation(new_variation_id, count)
        entity_type.variations.append(variation)

        self._process_properties(archetype["properties"], variation)
        self._process_references(archetype.get("references", []), variation)

    def _create_parent_entities(self, entity_type: EObject, labels: str) -> None:
        """Porte de ``createParentsEntities`` (``:70-80``) — herança múltipla por label."""
        separated_string: list[str] = labels.split(_LABELS_JOINER)
        if len(separated_string) > 1:
            for label in separated_string:
                entity_type.parents.append(self._builder.get_or_create_entity_type(label))

    def _process_properties(self, properties: dict[str, Any], variation: EObject) -> None:
        """Porte de ``processProperties`` (``:82-90``).

        Reusa ``extractors.neo4j.get_type_name`` — mesma função que já
        replica o achado do round-trip de JSON (``Long``→``"integer"``,
        heterogêneo/vazio→``"string[]"``); ver ``extractors/neo4j.py``.
        """
        for name, sentinel in properties.items():
            attribute = self._builder.create_attribute(name, get_type_name(sentinel))
            variation.features.append(attribute)
            variation.structuralFeatures.append(attribute)

    def _process_references(self, relationships: list[dict[str, Any]], variation: EObject) -> None:
        """Porte de ``processReferences`` (``:92-108``).

        ``references`` é local a esta chamada (um dict por variação de nó
        processada) — mesmo escopo que o ``HashMap`` local do Java
        (``:94``), não um índice global.
        """
        references: dict[str, EObject] = {}
        for relationship in relationships:
            type_name = relationship["type"]
            refs_to = _LABELS_JOINER.join(relationship["refsTo"])

            features_variation = self._get_or_create_ref_type_and_variation(
                relationship, type_name, 0
            )
            self._get_or_create_reference(
                variation, references, type_name, features_variation, refs_to
            )

    def _get_or_create_reference(
        self,
        variation: EObject,
        references: dict[str, EObject],
        type_name: str,
        features_variation: EObject,
        refs_to: str,
    ) -> None:
        """Porte de ``getOrCreateReference`` (``:110-127``), sem ``addReferenceToCount``."""
        key = type_name + refs_to
        reference = references.get(key)
        if reference is None:
            reference = self._builder.create_reference(type_name, refs_to, features_variation)
            reference.lowerBound = 1
            references[key] = reference
            variation.features.append(reference)
            variation.logicalFeatures.append(reference)
        else:
            reference.isFeaturedBy.append(features_variation)
            reference.upperBound = -1

    def _get_or_create_ref_type_and_variation(
        self, relationship: dict[str, Any], reference_name: str, count: int
    ) -> EObject:
        """Porte de ``getOrCreateRefTypeAndVariation`` (``:129-140``)."""
        relationship_type = self._builder.get_or_create_relationship_type(reference_name)
        variation = self._get_or_create_variation(relationship, relationship_type)

        if variation.count > 0:
            variation.count = variation.count + count
        else:
            if count != 0:
                variation.count = count

        return variation

    def _get_or_create_variation(
        self, relationship: dict[str, Any], relationship_type: EObject
    ) -> EObject:
        """Porte de ``getOrCreateVariation`` (``:142-157``).

        A chave de deduplicação (:func:`_properties_key`) substitui
        ``properties.toString()`` do ``org.json`` — order-independent nos
        dois casos (Java: ``HashMap`` interno do ``JSONObject``, cuja ordem
        de iteração é função do conjunto de chaves, não da ordem de
        inserção; aqui: ``json.dumps(sort_keys=True)``), então as classes de
        equivalência batem mesmo sem replicar o texto literal.
        """
        properties = relationship["properties"]
        key = _properties_key(properties)
        variation = self._repository.get_ref_variation(relationship_type.name, key)

        if variation is None:
            variation_id = self._repository.get_ref_variation_id(relationship_type.name)
            variation = self._builder.create_variation(variation_id, 0)
            relationship_type.variations.append(variation)

            self._process_properties(properties, variation)
            self._repository.save_ref_variation(relationship_type.name, key, variation)

        return variation


def _properties_key(properties: dict[str, Any]) -> str:
    """Chave canônica de um bloco de propriedades.

    Ver docstring de ``_get_or_create_variation``.
    """
    return json.dumps(properties, sort_keys=True, separators=(",", ":"))


def _process_optionals(schema: EObject) -> None:
    """Marcar atributos opcionais por ``EntityType`` — porte de ``AttributeOptionalsChecker``.

    Porte de ``processOptionals``/``processEntityPrimitiveTypeOptinals``
    (``AttributeOptionalsChecker.java:29-45``). Só entre as
    ``StructuralVariation`` do **mesmo** ``EntityType`` — sem equivalente
    cross-schema; ``RelationshipType`` nunca passa por aqui (só
    ``schema.entities``, igual ao Java ``:33``).
    """
    for entity in schema.entities:
        _process_entity_optionals(entity)


def _process_entity_optionals(entity: EObject) -> None:
    counts: dict[str, int] = {}

    for variation in entity.variations:
        for feature in variation.features:
            _count_feature(entity, feature, counts)

    variations_count = len(entity.variations)
    for variation in entity.variations:
        for feature in variation.features:
            if feature.eClass.name == "Attribute":
                key = _attribute_as_string(feature)
                if counts[key] < variations_count:
                    feature.optional = True


def _count_feature(entity: EObject, feature: EObject, counts: dict[str, int]) -> None:
    """Porte de ``attributeCount`` (``:58-68``) — conta ``Attribute`` e ``Reference``.

    Só ``Attribute`` acaba marcado opcional (``attributeUpdate``, ``:84-98``
    — o ramo ``Reference`` nunca seta nada); ``Reference`` entra na contagem
    mas o resultado não é lido de volta pra ela. Replicado como está: contar
    os dois, só usar o de ``Attribute``.
    """
    if feature.eClass.name == "Attribute":
        key = _attribute_as_string(feature)
    elif feature.eClass.name == "Reference":
        key = _reference_as_string(entity, feature)
    else:
        return

    counts[key] = counts.get(key, 0) + 1


def _attribute_as_string(attribute: EObject) -> str:
    """Porte de ``attributeAsString`` (``:100-104``)."""
    name: str = attribute.name
    string = name + ":" + _string_type_for_optionality(attribute.type)
    return string.lower()


def _reference_as_string(entity: EObject, reference: EObject) -> str:
    """Porte de ``referenceAsString`` (``:106-110``)."""
    entity_name: str = entity.name
    reference_name: str = reference.name
    refs_to_name: str = reference.refsTo.name
    string = (
        entity_name
        + "-["
        + reference_name
        + ":"
        + str(reference.isFeaturedBy[0].variationId)
        + "]->"
        + refs_to_name
    )
    return string.lower()


def _string_type_for_optionality(data_type: EObject) -> str:
    """Porte de ``getStringType``/``getStringArrayType`` (``:112-136``).

    **Incompleto por fidelidade** — só reconhece ``PrimitiveType``/``PTuple``;
    ``PList`` (o tipo que todo atributo array do Neo4j de fato tem, ver
    docstring do módulo) cai no ``EMPTY`` final. Não estender pra ``PList``:
    seria consertar o oráculo, não portá-lo.
    """
    name = data_type.eClass.name
    if name == "PrimitiveType":
        type_name: str = data_type.name
        return type_name
    elif name == "PTuple":
        return _string_array_type(data_type)
    else:
        return ""


def _string_array_type(ptuple: EObject) -> str:
    """Porte de ``getStringArrayType`` (``:126-136``)."""
    if len(ptuple.elements) > 0:
        return _ARRAY_SUFFIX + _string_type_for_optionality(ptuple.elements[0])
    else:
        return _ARRAY_SUFFIX


def _join_variations_ignoring_bounds(schema: EObject) -> None:
    """Fundir variações de features idênticas ignorando bounds de referência.

    Porte de ``joinVariationsIgnoringBounds``
    (``IgnoreSimilarReferenceBoundsProcessor.java:45-54``). Apesar do nome, o
    ``upperBound`` das referências correspondentes não vira sempre ``-1`` — é
    igualado ao **mínimo** entre as duas (``compareReferences``, ``:139-140``);
    só o ``lowerBound``/estrutura de features é ignorado na comparação. Só
    ``schema.entities`` — ``RelationshipType`` não passa por aqui.
    """
    for entity in schema.entities:
        _copy_similar_references_ignoring_bounds(entity)
        _remove_duplicate_variations(entity)
        _renumber_variations(entity)


def _renumber_variations(entity: EObject) -> None:
    """Porte de ``updateVariationIds`` (``:56-64``)."""
    for variation_id, variation in enumerate(entity.variations, start=1):
        variation.variationId = variation_id


def _remove_duplicate_variations(entity: EObject) -> None:
    """Porte de ``removeDuplicates`` (``:66-81``) — soma ``count`` na sobrevivente."""
    groups = _similar_variations_map(entity)
    for variations in groups.values():
        if len(variations) > 1:
            survivor, *duplicates = variations
            for duplicate in duplicates:
                survivor.count = survivor.count + duplicate.count
                entity.variations.remove(duplicate)


def _copy_similar_references_ignoring_bounds(entity: EObject) -> None:
    """Porte de ``copySimilarReferencesIgnoringBounds`` (``:83-95``)."""
    groups = _similar_variations_map(entity)
    for variations in groups.values():
        for v1 in variations:
            for v2 in variations:
                if v1 is not v2:
                    _compare_variations(v1, v2)


def _similar_variations_map(entity: EObject) -> dict[str, list[EObject]]:
    """Porte de ``generateSimilarVariationsMap`` (``:97-111``).

    ``SortedMap<String, ...>`` no Java (``TreeMap``) — a ordenação da chave
    não importa aqui: cada grupo é processado independentemente, e a ordem
    de iteração dos grupos não afeta o resultado (comutativo). Um ``dict``
    comum basta.
    """
    groups: dict[str, list[EObject]] = {}
    for variation in entity.variations:
        properties: list[str] = []
        for feature in variation.features:
            _put_property_key(properties, feature)
        key = ",".join(sorted(properties))
        groups.setdefault(key, []).append(variation)
    return groups


def _put_property_key(properties: list[str], feature: EObject) -> None:
    """Porte de ``putPropertyKeyOnList`` (``:167-181``)."""
    if feature.eClass.name == "Attribute":
        properties.append(feature.name + ":" + _attribute_representation(feature))
    elif feature.eClass.name == "Reference":
        properties.append(feature.name + ":" + _reference_representation(feature))


def _attribute_representation(attribute: EObject) -> str:
    """Porte de ``getAttributeRepresentation`` (``:183-186``)."""
    name: str = attribute.name
    return name + _type_representation(attribute.type)


def _reference_representation(reference: EObject) -> str:
    """Porte de ``getReferenceRepresentation`` (``:188-191``)."""
    name: str = reference.name
    refs_to_name: str = reference.refsTo.name
    return name + "->" + refs_to_name


def _type_representation(data_type: EObject) -> str:
    """Porte de ``getTypeRepresentation`` (``:193-210``).

    Diferente de :func:`_string_type_for_optionality` — reconhece
    ``PList``/``PSet``/``PTuple``/``PMap`` recursivamente. Ver "Duas
    divergências propositais" no docstring do módulo.
    """
    if data_type.eClass.name == "PrimitiveType":
        name: str = data_type.name
        return name
    elif data_type.eClass.name == "PList":
        return _type_representation(data_type.elementType) + "[]"
    elif data_type.eClass.name == "PSet":
        return _type_representation(data_type.elementType) + "{}"
    elif data_type.eClass.name == "PTuple":
        string = ",".join(_type_representation(element) for element in data_type.elements)
        return "[" + string + "]"
    elif data_type.eClass.name == "PMap":
        return (
            "{"
            + _type_representation(data_type.keyType)
            + ":"
            + _type_representation(data_type.valueType)
            + "}"
        )
    else:
        return ""


def _compare_variations(variation1: EObject, variation2: EObject) -> None:
    """Porte de ``compareVariations`` (``:113-129``)."""
    references1 = [f for f in variation1.features if f.eClass.name == "Reference"]
    references2 = [f for f in variation2.features if f.eClass.name == "Reference"]

    for r1 in references1:
        for r2 in references2:
            if r1 is not r2:
                _compare_references(r1, r2)


def _compare_references(r1: EObject, r2: EObject) -> None:
    """Porte de ``compareReferences`` (``:131-142``).

    O nome do método original sugere "ignorar bounds" (sempre virar
    ilimitado); na prática iguala ao **mínimo** dos dois ``upperBound`` — só
    fica ``-1`` se um dos dois já era. Ler o código, não o nome (mesma nota
    em ``todolist_fase2.md`` §2.2).
    """
    if _reference_representation(r1) == _reference_representation(r2):
        _copy_features_in_both_references(r1, r2)
        merged_upper_bound = min(r1.upperBound, r2.upperBound)
        r1.upperBound = merged_upper_bound
        r2.upperBound = merged_upper_bound


def _copy_features_in_both_references(r1: EObject, r2: EObject) -> None:
    """Porte de ``copyFeaturesInBothReferences`` (``:144-154``).

    União por ordem de inserção (``r1`` primeiro, depois só o que é novo em
    ``r2``), sem passar por ``set``/hash de identidade. O Java usa
    ``HashSet<StructuralVariation>``, cuja ordem de iteração já não é
    especificada por ele mesmo — ordem determinística aqui é uma escolha
    dentro do mesmo contrato, não uma divergência de fidelidade.

    Parameters
    ----------
    r1, r2 : EObject
        Duas ``Reference`` com a mesma representação
        (:func:`_reference_representation`) — mutadas in-place: o
        ``isFeaturedBy`` de ambas vira a mesma lista unida.

    Returns
    -------
    None
        O efeito é só a mutação de ``r1``/``r2``.
    """
    merged: list[EObject] = []
    combined = (*r1.isFeaturedBy, *r2.isFeaturedBy)
    for feature in combined:
        if feature not in merged:
            merged.append(feature)

    r1.isFeaturedBy.clear()
    r1.isFeaturedBy.extend(merged)

    r2.isFeaturedBy.clear()
    r2.isFeaturedBy.extend(merged)


def build_uschema_from_archetypes(
    pkg: EPackage, name: str, archetype_counts: list[dict[str, Any]]
) -> EObject:
    """Construir o ``USchema`` do Neo4j a partir dos arquétipos com contagem.

    Porte de ``Json2USchemaModel.processArchetypes`` (``Json2USchemaModel.java:39-51``)
    — a fachada completa: cria o ``USchema``, processa cada arquétipo,
    aplica os dois passos de pós-processamento. Contraparte, pro paradigma
    grafo, de ``inference.build_uschema.BuildUSchema.build_from_rows`` — mas
    **não** o mesmo código (ver docstring do módulo).

    Parameters
    ----------
    pkg : EPackage
        Metamodelo carregado (:func:`~uschema.metamodel.registry.load_metamodel`).
    name : str
        Nome do ``USchema`` resultante.
    archetype_counts : list of dict of str to Any
        Saída de ``extractors.neo4j.build_archetype_counts``/
        ``extract_archetype_counts``/``extract_database_archetype_counts`` —
        ``[{"archetype": {...}, "count": N}, ...]``.

    Returns
    -------
    EObject
        O ``USchema`` construído.

    Notes
    -----
    A ordem de ``archetype_counts`` afeta só a numeração de ``variationId``
    por ``EntityType`` (quando há mais de uma variação) — não a estrutura. O
    harness de comparação (0.3) casa variações por estrutura, não posição
    (mesma nota em ``inference/build_uschema.py``); não é preciso replicar a
    ordem de iteração (não-determinística) do ``HashMap`` que o Java usa.
    """
    repository = _ModelRepository()
    builder = _USchemaBuilder(pkg, repository)
    variation_builder = _StructuralVariationBuilder(builder, repository)

    builder.create_uschema(name)

    for row in archetype_counts:
        variation_builder.create_variation(row["archetype"], row["count"])

    assert repository.schema is not None

    _process_optionals(repository.schema)
    _join_variations_ignoring_bounds(repository.schema)

    return repository.schema

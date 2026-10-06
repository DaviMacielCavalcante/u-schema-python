#!/usr/bin/env bash
# Encadeia TODAS as baterias, numa semente e numa engine, SEQUENCIALMENTE.
#
# Substitui o `run_scale_suite.sh`, que orquestrava só as duas baterias de
# tamanho e existia para varrer três sementes. Com a semente única (padrão nos
# scripts, sobrescrevível), o que restava a orquestrar era a cadeia inteira —
# que é o que este script faz.
#
# Ordem: Northwind, cadeia do oráculo (documento e grafo) e as duas baterias de
# tamanho. É a ordem em que as evidências dependem umas das outras: o Northwind
# não precisa de dado gerado, e a cadeia do oráculo deixa os bancos no estado
# que o tamanho vai sobrescrever de qualquer forma.
#
# Por que sequencial: os dois clientes Python não disputam entre si, mas o
# mongod e o Neo4j disputam CPU e disco. Os tempos medidos aqui vão para a
# avaliação — medi-los sob carga variável do outro paradigma invalidaria a
# comparação de curvas, que é o objeto da fase.
#
# Fase 4.4: a engine do porte é o segundo argumento (`python`, o padrão, ou
# `spark`), e cada combinação de rota e tamanho roda num PROCESSO PRÓPRIO. O
# PySpark não derruba a JVM entre sessões, e num processo só os tamanhos
# seguintes pegariam a JVM aquecida pelo primeiro (`scripts/engines.py`). A
# engine Python roda do mesmo jeito, para as duas serem medidas iguais. Tudo vai
# para `results/fase4/` e `out/fase4/` — a bateria canônica de 27/08 fica
# intocada. O Northwind só roda na primeira suíte do diretório: não tem semente
# nem engine Spark, e uma segunda corrida duplicaria a chave dele.
#
# O terceiro argumento restringe a suíte a um banco (`mongodb` ou `neo4j`); sem
# ele, roda os dois. Separar deixa cada banco rodar no seu kernel: a leitura do
# grafo trava no 6.17 (§E1) e roda num kernel mais novo, em que o mongod não
# sobe. O Northwind é do MongoDB, e só roda na suíte que o inclui.
#
# DESTRUTIVO: começa esvaziando os bancos das baterias (`up_*` no MongoDB e o
# grafo do Neo4j; só o do banco pedido, quando há um) e depois cada corrida
# dropa e regera o seu. Com a semente no `run_id`, o dado é reconstruível — é
# para isso que a semente existe.
# O `northwind` NÃO é tocado (ver scripts/clean_databases.py).
#
# Uso:
#   ./scripts/run_suite.sh                  # semente padrão dos scripts (23), engine python
#   ./scripts/run_suite.sh 69               # outra semente
#   ./scripts/run_suite.sh 69 spark         # outra semente, engine spark
#   ./scripts/run_suite.sh 69 spark neo4j   # só as baterias do grafo
#   nohup ./scripts/run_suite.sh 23 spark > /dev/null 2>&1 &   # e ir dormir
#
# Pré-requisitos: o banco pedido ativo (os dois, sem o terceiro argumento),
# imagem do oráculo buildada.
#   systemctl is-active mongod neo4j
#   docker image inspect extrator-uschema > /dev/null
# Lembrete: o mongod não sobe em kernel >= 6.19 (guarda do rseq) — ver
# `todolist_fase3.md`, riscos da fase.

set -euo pipefail

cd "$(dirname "$0")/.."

if [ $# -gt 3 ]; then
    echo "erro: no máximo uma semente, uma engine e um banco" >&2
    echo "  uso: $0 [semente [engine [banco]]]" >&2
    exit 2
fi

ENGINE="${2:-python}"
if [ "$ENGINE" != python ] && [ "$ENGINE" != spark ]; then
    echo "erro: engine desconhecida: $ENGINE (python ou spark)" >&2
    exit 2
fi

# Vazio = os dois bancos. Os nomes são os da coluna `paradigm` e do `run_id`.
DATABASE="${3:-}"
case "$DATABASE" in
    "") CLEAN_ARGS=() ;;
    mongodb) CLEAN_ARGS=(--only mongo) ;;
    neo4j) CLEAN_ARGS=(--only neo4j) ;;
    *)
        echo "erro: banco desconhecido: $DATABASE (mongodb ou neo4j)" >&2
        exit 2
        ;;
esac

# Se a suíte inclui as baterias do banco `$1`.
wants() {
    [ -z "$DATABASE" ] || [ "$DATABASE" = "$1" ]
}

RESULTS=results/fase4

# Espelham os DEFAULT_SIZES/DEFAULT_ROUTES dos scripts. Ficam aqui porque o laço
# por combinação é do shell; um valor fora da lista o argparse recusa.
SIZES=(small medium large larger)
ROUTES=(A B)

# Sem argumento, o `--seed` nem é passado: vale o DEFAULT_SEED, que mora em
# `scripts/output.py` e é de onde os quatro scripts o importam. Duplicá-lo aqui
# abriria espaço para as duas divergirem.
SEED_ARGS=()
SEED_LABEL="padrão dos scripts"
if [ $# -ge 1 ]; then
    SEED_ARGS=(--seed "$1")
    SEED_LABEL="$1"
fi

# As baterias gravam em APPEND. Rodar duas vezes a mesma semente não sobrescreve
# nada — duplica, e a duplicata só aparece depois, num `uniq -d`. Já custou uma
# sessão inteira; a guarda é barata.
#
# A semente da guarda sai do mesmo lugar que a das baterias. Fixá-la aqui era o
# próprio cenário que a guarda existe para pegar: mudar o DEFAULT_SEED faria o
# grep procurar a semente errada, e a duplicata voltaria a passar calada.
#
# Com um banco só, a guarda olha só as corridas dele (o paradigma é o segundo
# campo do `run_id`): a suíte do outro banco, na mesma semente e engine, não é
# repetição.
if [ -s "$RESULTS/runs.csv" ]; then
    SEED_CHECK="${1:-$(uv run python -c \
        'import sys; sys.path.insert(0, "scripts"); from output import DEFAULT_SEED; print(DEFAULT_SEED)')}"
    DB_CHECK="${DATABASE:-mongodb|neo4j}"
    if grep -qE "^[^,-]*-(${DB_CHECK})-[^,]*-${SEED_CHECK}-${ENGINE}," "$RESULTS/runs.csv"; then
        echo "erro: $RESULTS/runs.csv já tem corridas da semente ${SEED_CHECK}" \
            "na engine ${ENGINE} (${DB_CHECK})." >&2
        echo "  Arquive o diretório antes de repetir:" >&2
        echo "    mv $RESULTS ${RESULTS}_\$(date +%d-%m)" >&2
        exit 3
    fi
fi

mkdir -p "$RESULTS" logs
LOG="logs/baterias_$(date +%Y%m%d_%H%M%S).log"

echo "semente  : ${SEED_LABEL}" | tee -a "$LOG"
echo "engine   : ${ENGINE}" | tee -a "$LOG"
echo "banco    : ${DATABASE:-mongodb e neo4j}" | tee -a "$LOG"
echo "log      : $LOG" | tee -a "$LOG"
echo "início   : $(date --iso-8601=seconds)" | tee -a "$LOG"

echo "" | tee -a "$LOG"
echo "########## limpeza inicial  $(date +%H:%M:%S) ##########" | tee -a "$LOG"
uv run python scripts/clean_databases.py "${CLEAN_ARGS[@]}" 2>&1 | tee -a "$LOG"

# O Northwind não leva `--seed` nem `--engine`: o dataset é real e versionado,
# não se regenera, e roda só na engine Python. Uma vez por diretório, e só na
# suíte que inclui o MongoDB.
run_northwind() {
    if [ -s "$RESULTS/runs.csv" ] && grep -q "^equivalence-mongodb-northwind-" "$RESULTS/runs.csv"; then
        echo "" | tee -a "$LOG"
        echo "########## northwind: já medido em $RESULTS, pulado ##########" | tee -a "$LOG"
        return 0
    fi
    echo "" | tee -a "$LOG"
    echo "########## northwind  $(date +%H:%M:%S) ##########" | tee -a "$LOG"
    # `tee` sem pipefail mascararia a falha; o `set -o pipefail` acima evita.
    uv run python scripts/run_northwind.py 2>&1 | tee -a "$LOG"
}

if wants mongodb; then run_northwind; fi

# Uma combinação por processo (ver o cabeçalho).
run_mongo() {
    local bateria="$1"
    for rota in "${ROUTES[@]}"; do
        for tamanho in "${SIZES[@]}"; do
            echo "" | tee -a "$LOG"
            echo "########## ${bateria} ${rota} ${tamanho}  $(date +%H:%M:%S) ##########" | tee -a "$LOG"
            uv run python "scripts/${bateria}.py" "${SEED_ARGS[@]}" --engine "$ENGINE" \
                --routes "$rota" --sizes "$tamanho" 2>&1 | tee -a "$LOG"
        done
    done
}

run_neo4j() {
    local bateria="$1"
    for tamanho in "${SIZES[@]}"; do
        echo "" | tee -a "$LOG"
        echo "########## ${bateria} ${tamanho}  $(date +%H:%M:%S) ##########" | tee -a "$LOG"
        uv run python "scripts/${bateria}.py" "${SEED_ARGS[@]}" --engine "$ENGINE" \
            --sizes "$tamanho" 2>&1 | tee -a "$LOG"
    done
}

if wants mongodb; then run_mongo run_oracle_mongo; fi
if wants neo4j; then run_neo4j run_oracle_neo4j; fi
if wants mongodb; then run_mongo run_size_mongo; fi
if wants neo4j; then run_neo4j run_size_neo4j; fi

echo "" | tee -a "$LOG"
echo "fim      : $(date --iso-8601=seconds)" | tee -a "$LOG"
echo "tabelas  : $RESULTS/{runs,oracle,comparisons,divergences}.csv" | tee -a "$LOG"

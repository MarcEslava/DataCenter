#!/bin/bash
set -euo pipefail

usage()
{
    cat <<EOF
Uso:
  $0 [opciones]

Defaults:
  URL      http://monitserver.local/api/v1/hooks/run
  HOOK     sample_hook
  SECRET   XAS4355fffg

Opciones:
  --url <hook_url>                 URL del endpoint (default incluido)
  --hook <hook_name>               Nombre del hook (default incluido)
  --secret <hmac_secret>           Secreto HMAC (default incluido)
  --payload <json>                 Payload inline (por defecto: {})
  --payload-file <ruta>            Payload desde fichero
  --signature-header <header>      Header de firma (por defecto: X-DCT-Signature)
  --content-type <tipo>            Content-Type (por defecto: application/json)
  -h, --help                       Muestra esta ayuda

Ejemplo:
  $0 --payload '{"source":"manual","branch":"main"}'
  $0 --url "http://monitserver.local/api/v1/hooks/run" --hook "sample_hook" --secret "XAS4355fffg"

EOF
}

require_cmd()
{
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1
    then
        echo "ERROR: comando no disponible: $cmd" >&2
        exit 1
    fi
}

DEFAULT_URL="http://monitserver.local/api/v1/hooks/run"
DEFAULT_HOOK="sample_hook"
DEFAULT_SECRET="XAS4355fffg"

URL="$DEFAULT_URL"
HOOK="$DEFAULT_HOOK"
SECRET="$DEFAULT_SECRET"
PAYLOAD="{}"
PAYLOAD_FILE=""
SIGNATURE_HEADER="X-DCT-Signature"
CONTENT_TYPE="application/json"

while [[ $# -gt 0 ]]
do
    case "$1" in
        --url)
            URL="${2:-}"
            shift 2
            ;;
        --hook)
            HOOK="${2:-}"
            shift 2
            ;;
        --secret)
            SECRET="${2:-}"
            shift 2
            ;;
        --payload)
            PAYLOAD="${2:-}"
            shift 2
            ;;
        --payload-file)
            PAYLOAD_FILE="${2:-}"
            shift 2
            ;;
        --signature-header)
            SIGNATURE_HEADER="${2:-}"
            shift 2
            ;;
        --content-type)
            CONTENT_TYPE="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Parametro desconocido: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$URL" || -z "$HOOK" || -z "$SECRET" ]]
then
    echo "ERROR: --url, --hook y --secret no pueden estar vacios" >&2
    usage
    exit 1
fi

if [[ -n "$PAYLOAD_FILE" ]]
then
    if [[ ! -f "$PAYLOAD_FILE" ]]
    then
        echo "ERROR: no existe el fichero de payload: $PAYLOAD_FILE" >&2
        exit 1
    fi

    PAYLOAD="$(cat "$PAYLOAD_FILE")"
fi

require_cmd curl
require_cmd openssl
require_cmd awk

SIGNATURE_HEX="$(printf '%s' "$PAYLOAD" | openssl dgst -sha256 -hmac "$SECRET" -hex | awk '{print $NF}')"
SIGNATURE_VALUE="sha256=$SIGNATURE_HEX"

REQUEST_URL="$URL?hook=$HOOK"
RESPONSE_FILE="$(mktemp)"

HTTP_CODE="$(curl -sS -o "$RESPONSE_FILE" -w "%{http_code}" \
    -X POST "$REQUEST_URL" \
    -H "Content-Type: $CONTENT_TYPE" \
    -H "$SIGNATURE_HEADER: $SIGNATURE_VALUE" \
    --data "$PAYLOAD")"

echo "URL: $REQUEST_URL"
echo "Header firma: $SIGNATURE_HEADER"
echo "HTTP code: $HTTP_CODE"
echo "Respuesta:"
cat "$RESPONSE_FILE"
echo

rm -f "$RESPONSE_FILE"

if [[ "$HTTP_CODE" -lt 200 || "$HTTP_CODE" -ge 300 ]]
then
    exit 1
fi

#!/usr/bin/env bash
# One command on a freshly rented Vast.ai GPU box: installs vLLM if it is missing, starts one
# OpenAI-compatible vLLM server per GPU, waits until each is ready, and prints the endpoint
# URLs and token to paste into the AI summaries admin page. Runs on the rented box only.
#
#   bash deploy/vllm.sh                 # generates a token, picks free mapped ports
#   VLLM_API_KEY=... bash deploy/vllm.sh
#   bash deploy/vllm.sh --check         # show what it would do; installs and starts nothing
#
# Tunables (env):
#   MODEL            default by GPU memory (the smallest card decides):
#                      >= 78 GB: Qwen/Qwen3-30B-A3B-Instruct-2507 (best in the Oklahoma pilot,
#                                ~61 GB BF16, ~2,000 wells in 2 min on one RTX PRO 6000)
#                      >= 20 GB: Qwen/Qwen3-8B (fits; weaker summaries)
#                      smaller:  Qwen/Qwen3-8B-FP8
#                    Set MODEL to override. Also tried for:
#                    mistralai/Ministral-3-8B-Instruct-2512 (FP8, Apache 2.0); Mistral models get
#                    vLLM's Mistral loading flags and no image inputs automatically
#   EXTRA_ARGS       extra `vllm serve` flags, e.g. "--max-num-batched-tokens 16384"
#   PORTS            one port per GPU, e.g. "10100 10200". Default: free ports this instance
#                    maps publicly (VAST_TCP_PORT_*), else BASE_PORT, BASE_PORT+1, ...
#   BASE_PORT        default 8000 (only without Vast port mappings)
#   VLLM_BIN         default: vllm on PATH, else the install below
#   VLLM_VERSION     default 0.24.0 (verified on 2x RTX 5090, 2026-11-19); "latest" for newest
#   TORCH_BACKEND    default cu128 (Blackwell / RTX 50 needs CUDA 12.8+ wheels)
#   VENV             install target, default /venv/main if it exists, else /workspace/vllm-venv
#   MAX_MODEL_LEN    default 8192   (well inputs are ~0.5-3k tokens; shorter = more batching room)
#   MAX_NUM_SEQS     default 256    (sequences per GPU that vLLM batches together)
#   GPU_UTIL         default 0.92
#   LOG_DIR          default /workspace/vllm-logs
#   HF_TOKEN         Hugging Face read token. Asked for when missing (Enter skips): anonymous
#                    downloads are throttled (a 60 GB model took ~20 min without one). Kept in
#                    $HF_HOME/token like `hf auth login`, so a restart on the same box doesn't ask.
set -euo pipefail

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

BASE_PORT="${BASE_PORT:-8000}"
MAX_MODEL_LEN="${MAX_MODEL_LEN:-8192}"
MAX_NUM_SEQS="${MAX_NUM_SEQS:-256}"
GPU_UTIL="${GPU_UTIL:-0.92}"
LOG_DIR="${LOG_DIR:-/workspace/vllm-logs}"
VLLM_VERSION="${VLLM_VERSION:-0.24.0}"
TORCH_BACKEND="${TORCH_BACKEND:-cu128}"
if [ -z "${VENV:-}" ]; then
  if [ -x /venv/main/bin/python ]; then VENV=/venv/main; else VENV=/workspace/vllm-venv; fi
fi

GPUS=$(nvidia-smi --query-gpu=index --format=csv,noheader | wc -l)
[ "$GPUS" -ge 1 ] || { echo "no GPU visible" >&2; exit 1; }

# Default model from the smallest card's memory; an explicit MODEL always wins.
GPU_MIB=$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits | tr -dc '0-9\n' | sort -n | head -1)
if [ -z "${MODEL:-}" ]; then
  if [ "${GPU_MIB:-0}" -ge 78000 ]; then
    MODEL=Qwen/Qwen3-30B-A3B-Instruct-2507
  elif [ "${GPU_MIB:-0}" -ge 20000 ]; then
    MODEL=Qwen/Qwen3-8B
    echo "WARNING: ${GPU_MIB} MiB per GPU is too small for Qwen/Qwen3-30B-A3B-Instruct-2507 (needs ~80 GB);" >&2
    echo "         using Qwen/Qwen3-8B. Summaries will be weaker; rent an 80-96 GB card for the 30B model." >&2
  else
    MODEL=Qwen/Qwen3-8B-FP8
    echo "WARNING: ${GPU_MIB} MiB per GPU; using Qwen/Qwen3-8B-FP8. Rent an 80-96 GB card for the 30B model." >&2
  fi
fi

# --- vLLM -----------------------------------------------------------------------------------
if [ -z "${VLLM_BIN:-}" ]; then
  if command -v vllm >/dev/null; then VLLM_BIN=$(command -v vllm)
  else VLLM_BIN="$VENV/bin/vllm"; fi
fi
install_vllm() {
  local spec="vllm==$VLLM_VERSION"
  [ "$VLLM_VERSION" = "latest" ] && spec="vllm"
  echo "installing $spec into $VENV (torch backend $TORCH_BACKEND); this takes a few minutes"
  command -v uv >/dev/null || python3 -m pip install -q uv
  [ -x "$VENV/bin/python" ] || uv venv -q --python 3.12 "$VENV"
  uv pip install -q --python "$VENV/bin/python" "$spec" --torch-backend="$TORCH_BACKEND"
}
if [ -x "$VLLM_BIN" ]; then
  echo "vLLM: $("$VLLM_BIN" --version 2>/dev/null | tail -1) at $VLLM_BIN"
elif [ "$CHECK_ONLY" = 1 ]; then
  echo "vLLM: not installed; would install vllm==$VLLM_VERSION into $VENV"
else
  install_vllm
  VLLM_BIN="$VENV/bin/vllm"
fi

# --- ports ----------------------------------------------------------------------------------
# Something accepts connections on the port. Bash's /dev/tcp needs no ss/netstat, which some
# images lack (a missing ss made every port look free).
port_in_use() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
if [ -n "${PORTS:-}" ]; then
  read -r -a PORT_LIST <<<"$PORTS"
else
  PORT_LIST=()
  # Free internal ports that Vast maps to a public port, lowest first. SSH and the
  # instance portal/Jupyter/TensorBoard ports are skipped because they are in use.
  while read -r p; do
    [ -n "$p" ] && [ "$p" -ge 1 ] && [ "$p" -le 65535 ] || continue
    port_in_use "$p" && continue
    PORT_LIST+=("$p")
  done < <(env | sed -n 's/^VAST_TCP_PORT_\([0-9]*\)=.*/\1/p' | sort -n)
  if [ "${#PORT_LIST[@]}" -eq 0 ]; then
    for ((g = 0; g < GPUS; g++)); do PORT_LIST+=($((BASE_PORT + g))); done
  fi
fi
if [ "${#PORT_LIST[@]}" -lt "$GPUS" ]; then
  echo "need $GPUS free ports, found: ${PORT_LIST[*]:-none}. Open more ports on the instance or set PORTS." >&2
  exit 1
fi

public_url() {
  local port=$1 var="VAST_TCP_PORT_$1"
  if [ -n "${PUBLIC_IPADDR:-}" ] && [ -n "${!var:-}" ]; then
    echo "http://$PUBLIC_IPADDR:${!var}"
  else
    echo "http://<public ip>:<external port mapped to $port>"
  fi
}

MODEL_ARGS=()
case "$MODEL" in
  mistralai/*)
    # From the model card; images are never sent, so no memory goes to the vision encoder.
    MODEL_ARGS+=(--tokenizer-mode mistral --config-format mistral --load-format mistral
      --limit-mm-per-prompt '{"image":0}') ;;
esac
read -r -a EXTRA <<<"${EXTRA_ARGS:-}"

# --- Hugging Face token and leftovers -----------------------------------------------------
HF_TOKEN_FILE="${HF_HOME:-$HOME/.cache/huggingface}/token"
if [ -z "${HF_TOKEN:-}" ] && [ -s "$HF_TOKEN_FILE" ]; then HF_TOKEN=$(<"$HF_TOKEN_FILE"); fi
gpu_mib_used() { nvidia-smi --query-gpu=memory.used --format=csv,noheader,nounits | sed -n "$(($1 + 1))p" | tr -dc 0-9; }
STOP_HINT="pkill -9 -i -f vllm; sleep 10; nvidia-smi   # memory should be near 0 MiB"

if [ "$CHECK_ONLY" = 1 ]; then
  echo "Hugging Face token: $([ -n "${HF_TOKEN:-}" ] && echo set || echo "none (will ask)")"
  for ((g = 0; g < GPUS; g++)); do echo "GPU $g memory in use: $(gpu_mib_used "$g") MiB"; done
  echo "GPUs: $GPUS; model: $MODEL; extra flags: ${MODEL_ARGS[*]} ${EXTRA[*]}"
  for ((g = 0; g < GPUS; g++)); do
    echo "GPU $g -> port ${PORT_LIST[$g]} -> $(public_url "${PORT_LIST[$g]}")"
  done
  exit 0
fi

# --- servers --------------------------------------------------------------------------------
# A vLLM from an earlier attempt keeps its GPU memory (its engine process is "VLLM::EngineCore",
# which a case-sensitive `pkill -f vllm` misses); a new server would then fail at start-up.
# PIDs of vLLM's API server (argv "[python] …/vllm serve …") and its engine processes (which
# rename themselves "VLLM::…"). Matched on the real arguments, never a text pattern, so a shell
# that merely mentions vllm is never matched or killed.
vllm_pids() {
  local d a
  for d in /proc/[0-9]*; do
    a=()
    mapfile -d '' -t a <"$d/cmdline" 2>/dev/null || continue
    [ "${#a[@]}" -gt 0 ] || continue
    if [[ "${a[0]}" == VLLM::* ]] ||
      [[ "${a[0]##*/}" == vllm && "${a[1]:-}" == serve ]] ||
      [[ "${a[1]:-}" == */vllm && "${a[2]:-}" == serve ]]; then
      echo "${d#/proc/}"
    fi
  done
}
stop_vllm() { vllm_pids | xargs -r kill -9 2>/dev/null || true; }
# Memory alone is not enough: an engine that is still starting has not allocated yet, and a second
# server started beside it runs out of memory (2026-11-21, after Ctrl+C on a first attempt).
others=$(vllm_pids)
if [ -n "$others" ]; then
  echo "A vLLM process is still running (pids: $(echo $others)). Stop it first:" >&2
  echo "  $STOP_HINT" >&2
  exit 1
fi
for ((g = 0; g < GPUS; g++)); do
  used=$(gpu_mib_used "$g")
  if [ "${used:-0}" -gt 1024 ]; then
    echo "GPU $g already has ${used} MiB in use (an earlier vLLM still running?). Stop it first:" >&2
    echo "  $STOP_HINT" >&2
    exit 1
  fi
done

if [ -z "${HF_TOKEN:-}" ] && [ -t 0 ]; then
  echo "Hugging Face read token (huggingface.co -> Settings -> Access Tokens -> Read)."
  echo "Without one, downloads are throttled. Press Enter to skip."
  read -rsp "HF token (hidden): " HF_TOKEN
  echo
  if [ -n "$HF_TOKEN" ]; then
    mkdir -p "$(dirname "$HF_TOKEN_FILE")"
    (umask 077; printf '%s' "$HF_TOKEN" > "$HF_TOKEN_FILE")
  fi
fi
if [ -n "${HF_TOKEN:-}" ]; then
  export HF_TOKEN
  echo "Hugging Face token: set"
else
  echo "Hugging Face token: none; large models may download slowly"
fi
export HF_XET_HIGH_PERFORMANCE="${HF_XET_HIGH_PERFORMANCE:-1}"

GENERATED_KEY=0
if [ -z "${VLLM_API_KEY:-}" ]; then
  VLLM_API_KEY=$(openssl rand -hex 24)
  GENERATED_KEY=1
fi
mkdir -p "$LOG_DIR"

declare -A PIDS
start_gpu() {
  local gpu=$1 port=${PORT_LIST[$1]}
  if port_in_use "$port"; then
    echo "port $port is already in use (an earlier vLLM still running?). Stop it first:" >&2
    echo "  $STOP_HINT" >&2
    exit 1
  fi
  echo "starting $MODEL on GPU $gpu, port $port (log: $LOG_DIR/gpu$gpu.log)"
  CUDA_VISIBLE_DEVICES=$gpu nohup "$VLLM_BIN" serve "$MODEL" \
    --host 0.0.0.0 --port "$port" \
    --api-key "$VLLM_API_KEY" \
    --served-model-name "$MODEL" \
    --max-model-len "$MAX_MODEL_LEN" \
    --max-num-seqs "$MAX_NUM_SEQS" \
    --gpu-memory-utilization "$GPU_UTIL" \
    "${MODEL_ARGS[@]}" "${EXTRA[@]}" \
    >"$LOG_DIR/gpu$gpu.log" 2>&1 &
  PIDS[$gpu]=$!
}

wait_ready() {
  local gpu=$1 port=${PORT_LIST[$1]}
  # Ready means: the process we started is alive AND the port accepts *this* token and serves
  # *this* model. A plain /health could be answered by an older server left on the port.
  until curl -sf -H "Authorization: Bearer $VLLM_API_KEY" "http://127.0.0.1:$port/v1/models" |
    grep -qF "\"$MODEL\""; do
    if ! kill -0 "${PIDS[$gpu]}" 2>/dev/null; then
      echo "vLLM on GPU $gpu exited; last log lines:" >&2; tail -n 30 "$LOG_DIR/gpu$gpu.log" >&2
      stop_vllm; exit 1
    fi
    sleep 5
  done
  echo "GPU $gpu ready on port $port"
}

# Background servers ignore Ctrl+C in a script, so stopping during start-up would leave an engine
# loading on the GPU. Stop everything this run started (nothing else was running: checked above).
stop_started() {
  trap - INT TERM
  echo >&2; echo "stopping the vLLM servers this run started" >&2
  stop_vllm
  exit 130
}
trap stop_started INT TERM

# First GPU alone so the model downloads once; the others then load from the cache.
start_gpu 0
wait_ready 0
for ((g = 1; g < GPUS; g++)); do start_gpu "$g"; done
for ((g = 1; g < GPUS; g++)); do wait_ready "$g"; done
trap - INT TERM

echo
echo "Paste into the AI summaries admin page → Box endpoint URL(s):"
for ((g = 0; g < GPUS; g++)); do public_url "${PORT_LIST[$g]}"; done
echo "Model: $MODEL"
if [ "$GENERATED_KEY" = 1 ]; then
  (umask 077; echo "$VLLM_API_KEY" > /workspace/vllm-token)
  echo "Box token (generated; also in /workspace/vllm-token, root only):"
  echo "$VLLM_API_KEY"
else
  echo "Box token: the VLLM_API_KEY you set."
fi

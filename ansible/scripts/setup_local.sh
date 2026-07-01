#!/usr/bin/env bash
# One-time setup on your workstation: venv + ansible + .env
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

echo "=== CPM Ansible local setup ==="

if [[ ! -d .venv ]]; then
  echo "Creating .venv..."
  python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo "Ansible: $(ansible-playbook --version | head -1)"

if [[ ! -f .env ]]; then
  REF_ENV="${ROOT}/../../../docker/reference/.env"
  if [[ -f "${REF_ENV}" ]]; then
    echo "Linking .env -> docker/reference/.env"
    ln -sf "${REF_ENV}" .env
  elif [[ -f "${ROOT}/.env.example" ]]; then
    echo "Creating .env from .env.example — edit passwords if needed"
    cp .env.example .env
    echo ""
    echo "To pull credentials from imr-dod-vm instead:"
    echo "  scp imr-dod-vm:~/DoD/docker/reference/.env ${ROOT}/.env"
  fi
fi

if [[ ! -f .env ]]; then
  echo "ERROR: No .env found. Create ${ROOT}/.env or copy from the VM."
  exit 1
fi

echo ""
echo "Done. Next:"
echo "  cd ${ROOT}"
echo "  source .venv/bin/activate"
echo "  ./scripts/connectivity_test.sh"
echo "  ansible-playbook site.yml"
echo "  ansible-playbook bootstrap.yml"

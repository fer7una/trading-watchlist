# Setup en WSL + VSCode (modo no-dolor)

## 1) Software mínimo

- Windows 10/11 con WSL2
- Ubuntu (WSL) 22.04+ recomendado
- Docker Desktop (WSL2 backend)
- VSCode + extensión **Remote - WSL**
- Git
- Python 3.11+
- TWS o IB Gateway (normalmente en Windows)

## 2) Preparar Ubuntu (WSL)

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git make sqlite3
```

## 3) Clonar repo y venv

```bash
git clone <TU_REPO> trading-watchlist
cd trading-watchlist
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## 4) Variables de entorno

Edita `.env`:
- `FMP_API_KEY`
- `IB_HOST`, `IB_PORT` (normalmente `127.0.0.1:7497` en paper)

## 5) Ejecutar

```bash
make run
```

Salida en `./out`.

## 6) Scheduling

### systemd (si lo tienes activado)

1) Copia los unit files

```bash
mkdir -p ~/.config/systemd/user
cp config/systemd/watchlist.* ~/.config/systemd/user/
```

2) Activa:

```bash
systemctl --user daemon-reload
systemctl --user enable --now watchlist.timer
systemctl --user list-timers | grep watchlist
```

> Nota: en WSL, los timers solo corren si WSL está levantado.

### Windows Task Scheduler

Crea una tarea diaria que ejecute:

```powershell
wsl -d Ubuntu -- bash -lc "cd ~/trading-watchlist && source .venv/bin/activate && make run"
```

Si quieres logs: redirige a `logs/watchlist.log`.

# ross-watchlist-diy

Generador DIY de watchlist estilo Ross (momentum / low float) usando:

- **IBKR (TWS o IB Gateway)** para scanner + market data + barras 1m.
- **Financial Modeling Prep (FMP)** para `floatShares`.
- **SQLite** para cachear (float + barras) y evitar rate limits.

Esto está pensado para correr **cada mañana** desde **WSL** y dejarte:

- `out/watchlist.json` (para tu pipeline)
- `out/tradingview_import.txt` (importable en TradingView)

> Para estudio, paper, y evitar quemarte la cuenta por curiosidad.

## Requisitos

- WSL2 (Ubuntu recomendado)
- Python 3.11+
- TWS o IB Gateway abierto y con API activada
- API key de FMP

## Setup rápido (WSL)

```bash
cd ross-watchlist-diy
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
cp .env.example .env
```

Edita `.env` y pon tu `FMP_API_KEY`.

Ejecuta:

```bash
make run
```

Salida:

- `out/watchlist.json`
- `out/tradingview_import.txt`

## Scheduling

### Opción A: systemd timer (si lo tienes activado en WSL)

Ver `config/systemd/*.service` y `*.timer`.

### Opción B: Windows Task Scheduler

Ejecuta:

```powershell
wsl -d Ubuntu -- bash -lc "cd /ruta/al/repo && source .venv/bin/activate && make run"
```

## Notas importantes

- **IBKR Scanner** no filtra por float. Por eso el float lo metemos por FMP.
- Para RVOL serio usamos enfoque **time-of-day** (vol acumulado hasta el minuto actual / media histórica a esa misma hora).
- Si tu IBKR no devuelve algunas métricas (por ejemplo `volume` en snapshot), baja a barras 1m para derivarlas.

## Desarrollo (opcional)

```bash
pip install -e .[dev]
make fmt
make lint
make test
```

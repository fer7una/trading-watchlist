# trading-watchlist

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
cd trading-watchlist
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

## Perfiles dinámicos (PROFILE)

Por defecto `PROFILE=auto` usa el calendario de mercado (`MARKET_CALENDAR=NYSE`) y selecciona:

- **PREMARKET** -> perfil `PRE_`
- **OPEN** -> perfil `OPEN_`
- **POST** -> perfil `POST_` si existe, si no `PRE_`
- **CLOSED** -> perfil `PRE_` + fallback si el scanner devuelve vacío

Manual:

- `PROFILE=premarket` usa `PRE_` (avisa si estás en sesión).
- `PROFILE=open` usa `OPEN_`; si no estás en sesión se fuerza a `PRE_` salvo `FORCE_PROFILE=1`.
- `PROFILE=closed` fuerza modo cerrado (útil para probar fallback).

Variables por perfil (todas opcionales, con fallback a las actuales):

`*_PRICE_MIN`, `*_PRICE_MAX`, `*_FLOAT_MAX`, `*_CHANGE_MIN_PCT`, `*_VOLUME_MIN`,
`*_RVOL_MIN`, `*_RVOL_ANCHOR_NY`, `*_USE_RTH`, `*_SPREAD_MAX`,
`*_MAX_CANDIDATES`, `*_MAX_RVOL_SYMBOLS`

Ejemplo: `PRE_PRICE_MIN=2` o `OPEN_USE_RTH=1`.

## Fallback en mercado cerrado

Si la fase es **CLOSED** y el scanner devuelve 0 candidatos o la lista final queda vacía:

- `CLOSED_FALLBACK=last_ok` (default): usa el último `out/watchlist.json` si no está viejo.
- `CLOSED_FALLBACK=empty`: escribe lista vacía con razón.
- `CLOSED_FALLBACK=research`: usa el último watchlist aunque esté viejo; si no existe, vacío.

`CLOSED_STALE_MAX_HOURS` controla la antigüedad máxima (default 36).  
`REQUIRE_ACTIVE_MARKETDATA=1` fuerza fallback si no hay datos válidos (`last` inválido).
Si no hay `out/watchlist.json`, intenta leer la última ejecución desde SQLite (tablas `watchlist_runs`/`watchlist_items`).

El output incluye metadata adicional: `profile_used`, `phase`, `schedule_times_ny`.

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
- `MAX_CANDIDATES` por defecto es **50** (límite típico por scan code). Ajusta si tu cuenta devuelve más.

## Desarrollo (opcional)

```bash
pip install -e .[dev]
make fmt
make lint
make test
```

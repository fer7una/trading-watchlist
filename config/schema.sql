PRAGMA journal_mode = WAL;

-- Universo / metadatos
CREATE TABLE IF NOT EXISTS symbols (
  symbol TEXT PRIMARY KEY,
  con_id INTEGER,
  primary_exchange TEXT,
  currency TEXT DEFAULT 'USD',
  last_seen_utc TEXT
);

-- Float (snapshot diario)
CREATE TABLE IF NOT EXISTS float_snapshots (
  symbol TEXT NOT NULL,
  asof_date TEXT NOT NULL,          -- YYYY-MM-DD (NY)
  float_shares INTEGER NOT NULL,
  source TEXT NOT NULL,
  created_utc TEXT NOT NULL,
  PRIMARY KEY(symbol, asof_date),
  FOREIGN KEY(symbol) REFERENCES symbols(symbol)
);

-- Barras 1m cacheadas
CREATE TABLE IF NOT EXISTS minute_bars (
  symbol TEXT NOT NULL,
  ts_utc TEXT NOT NULL,             -- ISO datetime UTC
  open REAL,
  high REAL,
  low REAL,
  close REAL,
  volume INTEGER,
  PRIMARY KEY(symbol, ts_utc),
  FOREIGN KEY(symbol) REFERENCES symbols(symbol)
);

-- Curvas baseline RVOL (time-of-day)
CREATE TABLE IF NOT EXISTS baseline_curves (
  symbol TEXT NOT NULL,
  session TEXT NOT NULL,
  bar_size TEXT NOT NULL,
  lookback_days INTEGER NOT NULL,
  method TEXT NOT NULL,
  trim_pct REAL NOT NULL,
  updated_utc TEXT NOT NULL,
  history_days_used INTEGER NOT NULL,
  baseline_json TEXT NOT NULL,
  notes TEXT,
  PRIMARY KEY(symbol, session, bar_size, lookback_days, method, trim_pct),
  FOREIGN KEY(symbol) REFERENCES symbols(symbol)
);

-- Ejecuciones diarias del generador
CREATE TABLE IF NOT EXISTS watchlist_runs (
  run_id TEXT PRIMARY KEY,
  generated_utc TEXT NOT NULL,
  anchor_time_ny TEXT NOT NULL,
  lookback_days INTEGER NOT NULL,
  filters_json TEXT NOT NULL
);

-- Resultado final
CREATE TABLE IF NOT EXISTS watchlist_items (
  run_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  grade TEXT NOT NULL,
  score REAL NOT NULL,
  last REAL,
  change_pct REAL,
  volume_today INTEGER,
  rvol REAL,
  float_shares INTEGER,
  spread REAL,
  notes TEXT,
  PRIMARY KEY(run_id, symbol),
  FOREIGN KEY(run_id) REFERENCES watchlist_runs(run_id),
  FOREIGN KEY(symbol) REFERENCES symbols(symbol)
);

CREATE INDEX IF NOT EXISTS idx_minute_bars_symbol_ts ON minute_bars(symbol, ts_utc);
CREATE INDEX IF NOT EXISTS idx_float_symbol_date ON float_snapshots(symbol, asof_date);
CREATE INDEX IF NOT EXISTS idx_baseline_curves_symbol ON baseline_curves(symbol);

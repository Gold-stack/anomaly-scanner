from pathlib import Path

def fetch_sp500_tickers():
    """
    Liest S&P 500 Ticker aus lokaler CSV.
    Datei: sp500.csv im Projekt-Root (eine Ebene über backend/)
    """

    backend_dir = Path(__file__).resolve().parent
    project_root = backend_dir.parent
    csv_path = project_root / "sp500.csv"

    if not csv_path.exists():
        raise RuntimeError(f"sp500.csv not found at {csv_path}")

    tickers = []

    with open(csv_path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip().upper()
            if not t:
                continue
            t = t.replace(".", "-")
            tickers.append(t)

    return sorted(set(tickers))

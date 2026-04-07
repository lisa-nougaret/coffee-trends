from pathlib import Path
import pandas as pd
from pytrends.request import TrendReq

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

KEYWORDS = ["iced coffee", "iced latte", "cappuccino", "espresso", "cold brew"]

def fetch_interest_over_time(
    keywords: list[str],
    timeframe: str = "today 5-y",
    geo: str = "",
    hl: str = "en-US",
    tz: int = 360,
) -> pd.DataFrame:
    pytrends = TrendReq(hl=hl, tz=tz)
    pytrends.build_payload(kw_list=keywords, timeframe=timeframe, geo=geo)
    return pytrends.interest_over_time()

def save_raw(df: pd.DataFrame) -> Path:
    output_path = RAW_DIR / "google_trends.csv"
    df.to_csv(output_path)
    return output_path

def run_ingestion() -> None:
    print(f"Fetching keywords: {KEYWORDS}")
    df = fetch_interest_over_time(KEYWORDS)
    output_path = save_raw(df)
    print(f"Saved {output_path}")

if __name__ == "__main__":
    run_ingestion()
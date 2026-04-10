from __future__ import annotations

from pathlib import Path
from functools import reduce
from datetime import date, timedelta
import time

import pandas as pd
from pytrends.request import TrendReq
from pytrends import exceptions

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

KEYWORDS = [
    # Core hot coffee drinks
    "espresso",
    "americano"
    "cappuccino",
    "latte",
    "flat white",
    "mocha",

    # Core cold coffee drinks
    "iced coffee",
    "cold brew",
    "iced latte",
    "iced americano",

    # Flavoured lattes
    "vanilla latte",
    "caramel latte",
    "hazelnut latte",

    # Seasonal or limited-time offerings
    "pumpkin spice latte",
    "peppermint mocha",
    "gingerbread latte",

    # Frappuccinos / desserts
    "frappuccino",
    "caramel frappuccino",
    "mocha frappuccino",

    # Macchiato drinks
    "caramel macchiato",
    "iced caramel macchiato",

    # Niche or emerging trends
    "dalgona coffee",
    "mushroom coffee",

    # Other brewing methods
    "pour over coffee",
    "french press coffee",
    "drip coffee"
]

def chunk_keywords(keywords: list[str], chunk_size: int = 5) -> list[list[str]]:
    if chunk_size < 1:
        raise ValueError("chunk_size must be 1 or more")
    return [keywords[i:i + chunk_size] for i in range(0, len(keywords), chunk_size)]

def normalize_geo(geo: str) -> str:
    return geo.strip().upper()

def normalize_timeframe(timeframe: str) -> str:
    timeframe = timeframe.strip()

    if timeframe == "today 10-y":
        end_date = date.today()
        start_date = end_date - timedelta(days=365 * 10)
        return f"{start_date:%Y-%m-%d} {end_date:%Y-%m-%d}"

    return timeframe

def validate_inputs(
    keywords: list[str],
    timeframe: str,
    geo: str,
    hl: str,
    tz: int,
) -> None:
    if not keywords:
        raise ValueError("keywords cannot be empty")

    cleaned_keywords = [kw.strip() for kw in keywords]
    if any(not kw for kw in cleaned_keywords):
        raise ValueError("keywords cannot have empty values")

    if geo and geo != geo.upper():
        raise ValueError(f"geo must be uppercase: {geo!r}")

    if not hl.strip():
        raise ValueError("hl cannot be empty")

    if not isinstance(tz, int):
        raise ValueError("tz must be an int")

def make_pytrends(hl: str, tz: int) -> TrendReq:
    return TrendReq(hl=hl, tz=tz)

def request_interest_over_time(
    keywords: list[str],
    timeframe: str,
    geo: str,
    hl: str,
    tz: int,
) -> pd.DataFrame:
    pytrends = make_pytrends(hl=hl, tz=tz)

    print(
        "  -> build_payload("
        f"keywords={keywords}, timeframe={timeframe!r}, geo={geo!r}, hl={hl!r}, tz={tz}"
        ")"
    )

    pytrends.build_payload(
        kw_list=keywords,
        timeframe=timeframe,
        geo=geo,
    )

    df = pytrends.interest_over_time().reset_index()

    if df.empty:
        raise RuntimeError(f"No data returned for {keywords}")

    if "isPartial" in df.columns:
        df = df.drop(columns=["isPartial"])

    return df

def fetch_interest_over_time(
    keywords: list[str],
    timeframe: str = "today 10-y",
    geo: str = "",
    hl: str = "en-US",
    tz: int = 360,
    max_retries_429: int = 4,
    base_sleep_seconds: int = 8,
) -> pd.DataFrame:
    keywords = [kw.strip() for kw in keywords]
    geo = normalize_geo(geo)
    timeframe = normalize_timeframe(timeframe)

    validate_inputs(
        keywords=keywords,
        timeframe=timeframe,
        geo=geo,
        hl=hl,
        tz=tz,
    )

    for attempt in range(1, max_retries_429 + 1):
        try:
            return request_interest_over_time(
                keywords=keywords,
                timeframe=timeframe,
                geo=geo,
                hl=hl,
                tz=tz,
            )

        except exceptions.TooManyRequestsError as exc:
            sleep_for = base_sleep_seconds * attempt
            print(
                f"  -> got 429 for {keywords}, waiting {sleep_for}s "
                f"({attempt}/{max_retries_429})"
            )
            if attempt == max_retries_429:
                raise RuntimeError(f"Too many requests for {keywords}") from exc
            time.sleep(sleep_for)

        except exceptions.ResponseError as exc:
            message = str(exc)

            if len(keywords) > 1 and "code 400" in message:
                print(f"  -> got 400 for {keywords}, trying smaller groups")
                mid = len(keywords) // 2

                left = fetch_interest_over_time(
                    keywords=keywords[:mid],
                    timeframe=timeframe,
                    geo=geo,
                    hl=hl,
                    tz=tz,
                    max_retries_429=max_retries_429,
                    base_sleep_seconds=base_sleep_seconds,
                )

                time.sleep(2)

                right = fetch_interest_over_time(
                    keywords=keywords[mid:],
                    timeframe=timeframe,
                    geo=geo,
                    hl=hl,
                    tz=tz,
                    max_retries_429=max_retries_429,
                    base_sleep_seconds=base_sleep_seconds,
                )

                return pd.merge(left, right, on="date", how="outer")

            raise RuntimeError(
                f"Request failed for keywords={keywords}, timeframe={timeframe!r}, geo={geo!r}"
            ) from exc

        except Exception as exc:
            raise RuntimeError(
                f"Failed to fetch data for keywords={keywords}, timeframe={timeframe!r}, geo={geo!r}"
            ) from exc

    raise RuntimeError("Something went wrong in fetch_interest_over_time")

def save_batch(df: pd.DataFrame, batch_number: int) -> Path:
    output_path = RAW_DIR / f"google_trends_batch_{batch_number:02d}.csv"
    df.to_csv(output_path, index=False)
    return output_path

def combine_batches(batch_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    if not batch_dfs:
        raise ValueError("No batch dataframes to combine")

    df_combined = reduce(
        lambda left, right: pd.merge(left, right, on="date", how="outer"),
        batch_dfs,
    )

    df_combined = df_combined.sort_values("date").reset_index(drop=True)
    return df_combined

def save_raw(df: pd.DataFrame) -> Path:
    output_path = RAW_DIR / "google_trends_expanded.csv"
    df.to_csv(output_path, index=False)
    return output_path

def run_ingestion(
    keywords: list[str] = KEYWORDS,
    chunk_size: int = 5,
    timeframe: str = "today 10-y",
    geo: str = "",
    hl: str = "en-US",
    tz: int = 360,
    sleep_seconds: int = 5,
) -> None:
    keyword_batches = chunk_keywords(keywords, chunk_size)

    print(
        f"Fetching Google Trends data for {len(keywords)} keywords in "
        f"{len(keyword_batches)} batches..."
    )

    batch_dfs: list[pd.DataFrame] = []

    for i, batch in enumerate(keyword_batches, start=1):
        print(f"\nFetching batch {i}/{len(keyword_batches)}: {batch}")

        try:
            df_batch = fetch_interest_over_time(
                keywords=batch,
                timeframe=timeframe,
                geo=geo,
                hl=hl,
                tz=tz,
            )
        except Exception as exc:
            print(f"  -> batch {i} failed: {exc}")
            continue

        output_path = save_batch(df_batch, i)
        print(f"Saved batch file: {output_path}")

        batch_dfs.append(df_batch)

        if i < len(keyword_batches):
            print(f"Waiting {sleep_seconds}s before next batch...")
            time.sleep(sleep_seconds)

    if not batch_dfs:
        raise RuntimeError("All batches failed")

    df_combined = combine_batches(batch_dfs)
    combined_output_path = save_raw(df_combined)

    print(f"\nSaved combined raw dataset: {combined_output_path}")
    print("Combined dataset shape:", df_combined.shape)
    print("Combined dataset columns:", df_combined.columns.tolist())

if __name__ == "__main__":
    run_ingestion()
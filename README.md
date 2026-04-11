# The Coffee Dashboard.

A data analytics project (dashboard) to understand how coffee trends evolve over time, using Google Trends.

## ☕ Live Dashboard

*Work in progress.*

## ✨ Project Overview

Coffee trends are constantly evolving — some follow predictable seasonal patterns, while others emerge as short-lived hype or long-term habits.

## ❔ Key Business Questions
- Which coffee trends are recurring vs one-time hype?
- Which trends are growing vs declining?
- Do hype trends follow a predictable lifecycle?
- Which trends transition into long-term adoption?
- Do seasonal trends peak consistenly every year?

## ☁️ Architecture

```text
Google Trends
        ↓
Python notebook (data transformation)
        ↓
CSV datasets (fact & dimensions)
        ↓
Power BI (data model)
        ↓
Interactive dashboard
```

## 🔄 Pipeline

This project follows a structured analytical workflow:
- Data ingestion (2006~2026 Google Trends via Python / pytrends)
- Data transformation (pandas)
- Star schema modelling
- Data visualization (Power BI)

## 🛠️ Tech Stack

| Layer | Tools |
|---------|----------|
| Data source    | Google Trends     |
| Data ingestion    | Python (pytrends)     |
| Data transformation    | Python (pandas)     |
| Data modelling    | Star schema     |
| Data storage    | CSV files     |
| Visualization    | Power BI     |
| Version control    | Git     |

## ⭐ Current Data Model

The project uses a star schema optimized for time-series analysis:

```text
           dim_date
               |
               |
dim_trend —— google_trends_fact
```

#### Fact table
date
keyword
search_interest

#### Dimensions
**dim_date**
year, quarter, season, month, day
week_of_year
year_month, etc.

**dim_trend**
keyword
category
style
serving_style
trend_type (core / seasonal / trendy / niche)
contains_milk
*→ may vary in the future*

## ✨ Project Status

**Work in progress.**
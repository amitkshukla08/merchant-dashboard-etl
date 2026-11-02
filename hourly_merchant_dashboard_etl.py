"""
hourly_merchant_dashboard_etl.py

Pulls month-to-date order data directly from Mongo Prod (the live
operational database behind Metabase saved question 445, "Orderdata"), via
a raw MongoDB aggregation pipeline run through Metabase's /api/dataset/json
export endpoint. Summarizes it to Date x Hour x Warehouse x Salesman x Shop
Contact grain with merchant lifecycle flags (OB / ReOB / Retention /
Reactivation / Gold-Silver-Bronze-Explorer-Hunter classification), and
writes the result to a Google Sheet.

Designed to be run every hour (cron / Windows Task Scheduler / GitHub Actions).
Each run is idempotent for the current month: it recomputes the full
month-to-date summary and overwrites the output tab, so a missed or repeated
run never double-counts anything.

--------------------------------------------------------------------------
WHY MONGO DIRECTLY, NOT REDSHIFT'S fact_orders (and not cards 444/445 either)
--------------------------------------------------------------------------
This script went through two prior data sources before landing here:

1. Saved questions 444/445 called with guessed date_from/date_to
   parameters — always returned HTTP 500 (the real parameter contract was
   never exposed via the API), so the ETL fell back to Redshift SQL.
2. Direct SQL against `public_mart_orders.fact_orders` (Redshift) — this
   worked and reconciled correctly once UTC/IST boundary and column-casing
   bugs were fixed, but `fact_orders` turned out to be a periodically
   batch-refreshed mart, not real-time: on 2026-07-06 it was found frozen
   (MAX(placed_at) unchanged across three checks minutes apart) for several
   hours, causing the dashboard to undercount that day's orders/GMV
   relative to the business's live "Order Dump" report.

The business confirmed question 445 (Mongo, "zero ETL lag") is the correct,
live source. Since its exact template-tag parameter names aren't exposed
via the API either, this script doesn't call the saved question directly —
instead it replicates 445's own aggregation pipeline (match/project/lookup
stages, and the exact excluded-status list) as an ad-hoc query against
Mongo's `orders` collection, substituting the current IST month-to-date
window (converted to UTC) on every run. See build_orders_pipeline().

--------------------------------------------------------------------------
AUTH
--------------------------------------------------------------------------
Metabase API keys require admin access. This script instead logs in with a
regular username/password via POST /api/session (works for any account that
can run queries against Mongo Prod in the Metabase UI) and uses the
resulting session token. A fresh session is requested at the start of every
run, and any request that comes back 401 (e.g. session expired mid-run)
triggers exactly one re-authentication + retry.

--------------------------------------------------------------------------
RETENTION / REACTIVATION
--------------------------------------------------------------------------
Definitions (confirmed with the business, 2026-07-07):
  Retention    = active last month AND active this month.
  Reactivation = NOT active last month, but active before last month, AND
                 active this month.

These require knowing whether a merchant ordered LAST month and/or BEFORE
last month. An earlier version of this script tried to answer that by
persisting its own buyer-state tab and building up history over time —
which meant Retention/Reactivation could only ever show 0 during the
script's first month running, since there was no prior month's state yet.
Mongo already has the full order history, so this version instead queries
last month and "before last month" directly (fetch_retention_sets()) every
run — correct from the first run, no warm-up period, no state tab needed.

--------------------------------------------------------------------------
SETUP
--------------------------------------------------------------------------
Environment variables required:
  METABASE_URL         e.g. https://metabase.fairdeal.market
  METABASE_USERNAME    your Metabase login email
  METABASE_PASSWORD    your Metabase login password
  GOOGLE_SHEET_ID       the spreadsheet ID (from its URL)
  GOOGLE_CREDS_PATH     path to a Google service-account JSON key
                        (share the target spreadsheet with that service
                        account's email as an Editor)

Optional:
  METABASE_MONGO_DB_ID  Metabase database ID for "Mongo Prod" (default 2)
  SHEET_TAB_NAME        (default "Hourly_Merchant_Summary")
  DAILY_SHEET_TAB_NAME  Date x Warehouse x Salesman rollup, no Hour column
                        (default "Daily_Merchant_Summary")

pip install requests pandas gspread google-auth
"""

import os
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# Shared connection-pooled session for every Metabase HTTP call (auth +
# all fetch_mongo_native() requests). Reused instead of a fresh
# requests.post() each time so repeated/concurrent calls to the same host
# reuse TCP/TLS connections instead of paying handshake cost every time --
# pure network-overhead trim, doesn't change what's requested or returned.
_HTTP_SESSION = requests.Session()

# ==========================================================================
# CONFIG
# ==========================================================================
METABASE_URL      = os.environ["METABASE_URL"].rstrip("/")
METABASE_USERNAME = os.environ["METABASE_USERNAME"]
METABASE_PASSWORD = os.environ["METABASE_PASSWORD"]
MONGO_DB_ID       = int(os.environ.get("METABASE_MONGO_DB_ID", "2"))  # "Mongo Prod" in Metabase

GOOGLE_SHEET_ID   = os.environ["GOOGLE_SHEET_ID"]
GOOGLE_CREDS_PATH = os.environ.get("GOOGLE_CREDS_PATH", "service_account.json")
SHEET_TAB_NAME    = os.environ.get("SHEET_TAB_NAME", "Hourly_Merchant_Summary")
DAILY_SHEET_TAB_NAME = os.environ.get("DAILY_SHEET_TAB_NAME", "Daily_Merchant_Summary")

# Permanent, incrementally-growing cache of each buyer's first delivered
# order date (see get_first_order_data()). A buyer's first-ever order date
# never changes once it happened, so this is NOT a time-based (TTL) cache —
# once a buyer is resolved, that row is kept forever and never re-queried.
# Each run only needs to resolve buyers who are new to the cache (normally
# a small incremental set: this month's active buyers minus ones already
# known), which keeps the live Mongo query small and fast even though
# `orders` itself is a huge, unindexed, multi-million-row collection that
# cannot tolerate an all-time unbounded scan (see build_first_order_pipeline()'s
# docstring for that 2026-07-08 "operation exceeded time limit" incident).
#
# History: 2026-07-08 first attempt switched to the `customerorderdatas`
# collection (build_first_order_month_pipeline(), since removed) after
# discovering it via Metabase schema exploration — but it turned out to be
# a DEAD collection (frozen since 2025-02-04, confirmed via a live probe:
# max(createdAt)/max(updatedAt) both 2025-02-04, and 0 of 8 sampled buyers
# with a real order in 2026-07 had any row in it at all), so it could never
# produce a current FirstDeliveredOrderMonth and OB/ReOB would have stayed
# 0 forever for every buyer active after Feb 2025. Reverted the same day to
# querying the live `orders` collection directly, scoped to just this
# month's active buyers via build_first_order_pipeline_for_buyers() instead
# of an all-time scan — semantically correct (real, current data) and cheap
# (bounded to a few thousand buyer IDs, not the whole collection's history).
# The tab name has a "_v2" suffix to abandon the old cache, which held
# first-order dates computed from the dead collection's scrambled (lexico-
# graphic, non-zero-padded "M-YY" string) sort and would otherwise have been
# wrongly trusted as already-resolved.
FIRST_ORDER_CACHE_TAB_NAME = os.environ.get("FIRST_ORDER_CACHE_TAB_NAME", "FirstOrderDate_Cache_v2")

# Same story for the "all buyers active before last month" scan used by
# Reactivation (see get_ordered_before_last_month()) — another unbounded,
# all-time scan of the same growing collection, same failure mode.
BEFORE_LAST_MONTH_CACHE_TAB_NAME = os.environ.get("BEFORE_LAST_MONTH_CACHE_TAB_NAME", "BeforeLastMonthBuyers_Cache")
BEFORE_LAST_MONTH_CACHE_MAX_AGE_HOURS = float(os.environ.get("BEFORE_LAST_MONTH_CACHE_MAX_AGE_HOURS", "24"))

IST = timezone(timedelta(hours=5, minutes=30))
REQUEST_TIMEOUT_SEC = 180

REQUIRED_GROUP_COLS = ["Warehouse", "Salesman Name", "Salesman Contact", "Shop Contact"]

# Order statuses excluded at the source — copied verbatim from question
# 445's own $match stage (confirmed by the business 2026-07-06) so this
# script's numbers stay reconciled with that question. Note this excludes
# more than just "cancelled" (also returned/rejected/return_* and failed).
ORDER_STATUS_EXCLUDE = [
    "cancelled", "returned", "rejected",
    "return_requested", "return_accepted", "return_pickup", "failed",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hourly_etl")


# ==========================================================================
# 1. EXTRACT
# ==========================================================================
def get_session_token() -> str:
    """
    Logs in with a regular Metabase username/password (POST /api/session).
    No admin access required — any account that can run queries against
    Mongo Prod in the Metabase UI can authenticate this way.
    """
    url = f"{METABASE_URL}/api/session"
    resp = _HTTP_SESSION.post(
        url,
        json={"username": METABASE_USERNAME, "password": METABASE_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["id"]
    log.info("Authenticated to Metabase as %s", METABASE_USERNAME)
    return token


def build_orders_pipeline(date_from_iso: str, date_to_iso: str) -> list:
    """
    MongoDB aggregation pipeline against Mongo Prod's `orders` collection.
    Mirrors Metabase saved question 445 ("Orderdata") — confirmed by the
    business (2026-07-06) to be the live, zero-ETL-lag source of truth,
    unlike `fact_orders` in Redshift, which was found to silently stall
    for hours at a time (see git history for that incident).

    date_from_iso/date_to_iso must be UTC ISO-8601 strings with millisecond
    precision, e.g. "2026-06-30T18:30:00.000Z" — computed internally by
    this script (main()), never raw user input, so direct interpolation
    into the pipeline is safe here.

    ORDER_STATUS_EXCLUDE matches exactly what question 445 excludes at the
    $match stage — do not add/remove statuses here without checking 445
    still agrees, since that question is the reconciliation baseline.
    """
    return [
        {"$match": {
            "createdAt": {"$gte": {"$date": date_from_iso}, "$lt": {"$date": date_to_iso}},
            "currentStatus.status": {"$nin": ORDER_STATUS_EXCLUDE},
        }},
        {"$project": {
            "_id": 1, "buyer": 1, "currentStatus.status": 1,
            "createdAt": 1, "delivered.date": 1, "sellerDetails.name": 1,
            "salesmanId": 1, "bill.payableAmount": 1,
        }},
        {"$lookup": {
            "from": "customers", "localField": "buyer", "foreignField": "_id",
            "pipeline": [{"$project": {"_id": 0, "name": 1, "contact": 1,
                                        "shop.name": 1, "shopName": 1, "createdAt": 1}}],
            "as": "buyerDetails",
        }},
        {"$unwind": {"path": "$buyerDetails", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "salesmen", "localField": "salesmanId", "foreignField": "_id",
            "pipeline": [{"$project": {"_id": 0, "name": 1, "contact": 1}}],
            "as": "salesmanDetails",
        }},
        {"$unwind": {"path": "$salesmanDetails", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "Order-ID": {"$toString": "$_id"},
            "Status": "$currentStatus.status",
            "Order-Date": "$createdAt",
            "Delivery-Date": "$delivered.date",
            "Warehouse": "$sellerDetails.name",
            "Salesman Name": "$salesmanDetails.name",
            "Salesman Contact": "$salesmanDetails.contact",
            "Buyer-ID": {"$toString": "$buyer"},
            "Shop Name": {"$ifNull": ["$buyerDetails.shop.name", "$buyerDetails.shopName"]},
            "Shop Contact": "$buyerDetails.contact",
            "Buyer Onboarding Date": "$buyerDetails.createdAt",
            "Payable Amount": "$bill.payableAmount",
        }},
    ]


def build_first_order_pipeline() -> list:
    """
    DEPRECATED as of 2026-07-08 — kept only for reference/rollback. This is
    an all-time, UNSCOPED $group scan over the FULL, multi-million-row
    `orders` collection (every buyer who ever ordered, not just this
    month's active ones), which started hitting Mongo's own "operation
    exceeded time limit" on essentially every attempt once that collection
    grew past some size threshold. Because it never once succeeded after
    that point and no cache ever got seeded, OB/ReOB silently read 0 on
    every run (not just "this run only" as the fallback comments claimed)
    — see build_first_order_pipeline_for_buyers() for the replacement,
    which keeps the same real, live `orders` source but scopes the scan to
    only the buyers who actually need resolving each run.
    """
    return [
        {"$match": {"currentStatus.status": "delivered"}},
        {"$group": {"_id": "$buyer", "firstOrderDate": {"$min": "$createdAt"}}},
        {"$project": {"_id": 0, "Buyer-ID": {"$toString": "$_id"}, "First Delivered Order Date": "$firstOrderDate"}},
    ]


def build_first_order_pipeline_for_buyers(buyer_ids: list) -> list:
    """
    Replacement for build_first_order_pipeline(), fixed 2026-07-08 (second
    attempt — see FIRST_ORDER_CACHE_TAB_NAME's config comment for the first
    attempt, which used a since-discovered-dead collection and was reverted
    the same day).

    Still queries the real, live `orders` collection (the only source that
    is actually current), but scopes the $match to just `buyer_ids` instead
    of scanning every buyer who ever ordered. Paired with get_first_order_
    data()'s incremental cache, `buyer_ids` on any given run is normally
    just the small set of buyers newly seen this month who aren't already
    resolved in the cache — not the full multi-million-row history — which
    is what keeps this fast enough to avoid the timeout that killed
    build_first_order_pipeline().

    buyer_ids must be a list of Buyer-ID strings (not ObjectIds — `orders.
    buyer` is stored as an ObjectId, so this pipeline converts it to a
    string via $addFields before matching, rather than requiring the
    caller to construct Mongo Extended-JSON {"$oid": ...} literals).
    """
    return [
        {"$match": {"currentStatus.status": "delivered"}},
        {"$addFields": {"buyerIdStr": {"$toString": "$buyer"}}},
        {"$match": {"buyerIdStr": {"$in": buyer_ids}}},
        {"$group": {"_id": "$buyerIdStr", "firstOrderDate": {"$min": "$createdAt"}}},
        {"$project": {"_id": 0, "Buyer-ID": "$_id", "First Delivered Order Date": "$firstOrderDate"}},
    ]


def build_buyer_activity_pipeline(date_from_iso: str, date_to_iso: str) -> list:
    """
    Distinct buyers who placed a qualifying (non-excluded-status) order in
    [date_from_iso, date_to_iso). Used to compute Retention/Reactivation
    directly from Mongo's real order history, rather than from a persisted
    state table this script builds up over time — that approach required
    the pipeline to have already been running for a full prior month before
    Retention/Reactivation could show anything but 0. Since Mongo already
    has all the history needed, there's no reason to wait.
    """
    return [
        {"$match": {
            "createdAt": {"$gte": {"$date": date_from_iso}, "$lt": {"$date": date_to_iso}},
            "currentStatus.status": {"$nin": ORDER_STATUS_EXCLUDE},
        }},
        {"$group": {"_id": "$buyer"}},
        {"$project": {"_id": 0, "Buyer-ID": {"$toString": "$_id"}}},
    ]


def fetch_mongo_native(collection: str, pipeline: list, session_token: str, max_retries: int = 2) -> pd.DataFrame:
    """
    Runs a raw MongoDB aggregation pipeline via Metabase's ad-hoc query
    EXPORT endpoint (/api/dataset/json). For Mongo, Metabase's "native"
    query object needs both `collection` and `query` (the pipeline stages,
    JSON-encoded as text) — unlike SQL engines, which only need `query`.

    The query map (including the pipeline) is sent as a JSON-encoded
    STRING inside a form field named "query" (not a JSON request body),
    matching the same export-endpoint contract used elsewhere in this
    script. The response is a plain JSON array of row objects keyed by
    the field names produced by the pipeline's final $project stage.

    Retries with exponential backoff (1s, 2s, ...) on HTTP 400/408/425/429
    or 5xx — Metabase's export endpoint surfaces query timeouts on large
    unbounded scans (e.g. an all-time buyer-history query) as a plain 400,
    and these are frequently transient (2026-07-07 incident: an unbounded
    "orders before last month" scan 400'd once and crashed the whole run,
    even though the actual order data had already been fetched fine).
    On a non-retryable or exhausted-retries failure, raises RuntimeError
    with the actual Metabase response body included, instead of the bare
    "400 Bad Request" that `raise_for_status()` alone gives — that bare
    message was useless for diagnosing what actually went wrong.
    """
    url = f"{METABASE_URL}/api/dataset/json"
    query_map = {
        "database": MONGO_DB_ID,
        "type": "native",
        "native": {"collection": collection, "query": json.dumps(pipeline)},
    }
    form = {"query": json.dumps(query_map)}

    def _do_request(token: str):
        return _HTTP_SESSION.post(url, headers={"X-Metabase-Session": token}, data=form, timeout=REQUEST_TIMEOUT_SEC)

    retryable_status = {400, 408, 425, 429, 500, 502, 503, 504}

    for attempt in range(max_retries + 1):
        resp = _do_request(session_token)
        if resp.status_code == 401:
            log.warning("Session expired mid-run — re-authenticating and retrying once")
            session_token = get_session_token()
            resp = _do_request(session_token)

        if resp.status_code >= 400:
            body_preview = resp.text[:500]
            if attempt < max_retries and resp.status_code in retryable_status:
                wait_s = 2 ** attempt
                log.warning(
                    "Mongo query on '%s' failed with HTTP %d (attempt %d/%d) — retrying in %ds. Response: %s",
                    collection, resp.status_code, attempt + 1, max_retries + 1, wait_s, body_preview,
                )
                time.sleep(wait_s)
                continue
            raise RuntimeError(
                f"Metabase query on collection '{collection}' failed with HTTP {resp.status_code} "
                f"after {attempt + 1} attempt(s). Response body: {body_preview}"
            )

        rows = resp.json()
        if isinstance(rows, dict) and "message" in rows:
            # Metabase returns a JSON object (not an array) when the query itself errored
            if attempt < max_retries:
                wait_s = 2 ** attempt
                log.warning(
                    "Metabase Mongo query error on '%s' (attempt %d/%d): %s — retrying in %ds",
                    collection, attempt + 1, max_retries + 1, rows["message"], wait_s,
                )
                time.sleep(wait_s)
                continue
            raise RuntimeError(f"Metabase Mongo query error on collection '{collection}': {rows['message']}")

        return pd.DataFrame(rows)


def fetch_mtd_orders_raw(date_from_iso: str, date_to_iso: str, session_token: str) -> pd.DataFrame:
    """
    Fetches this month's orders from Mongo Prod (live, matches question 445).
    Does NOT merge in the first-delivered-order date — that's a separate,
    independent Mongo call (see get_first_order_data()), split out on
    purpose so main() can run it concurrently with this one instead of
    waiting for it first. Use _merge_first_order_dates() (or the
    fetch_mtd_data() convenience wrapper below) to combine the two.
    """
    log.info("Fetching month-to-date order data (Mongo) for UTC window %s -> %s", date_from_iso, date_to_iso)
    orders_df = fetch_mongo_native("orders", build_orders_pipeline(date_from_iso, date_to_iso), session_token)

    missing = [c for c in REQUIRED_GROUP_COLS if c not in orders_df.columns]
    if missing:
        raise KeyError(
            f"Expected column(s) {missing} not found in query result. "
            f"Columns actually returned: {list(orders_df.columns)}. "
            f"The Mongo `orders` schema may have changed — check build_orders_pipeline()."
        )

    log.info("Fetched %d order rows, %d columns", len(orders_df), len(orders_df.columns))
    return orders_df


def _merge_first_order_dates(orders_df: pd.DataFrame, first_order_df: pd.DataFrame) -> pd.DataFrame:
    """
    Left-joins each buyer's first delivered order date (needed for OB/ReOB)
    onto the MTD orders. Pulled out of fetch_mtd_data() so main() can fetch
    orders_df and first_order_df concurrently and merge only once both are
    in hand.
    """
    if first_order_df is not None and not first_order_df.empty:
        return orders_df.merge(first_order_df, on="Buyer-ID", how="left")
    orders_df = orders_df.copy()
    orders_df["First Delivered Order Date"] = None
    return orders_df


def fetch_mtd_data(date_from_iso: str, date_to_iso: str, session_token: str,
                    first_order_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience wrapper combining fetch_mtd_orders_raw() + _merge_first_order_dates()
    for any caller that doesn't need the two fetches to run concurrently.
    first_order_df is supplied by the caller (get_first_order_data()) rather
    than fetched fresh here — that all-time, unbounded scan across the
    entire order history is expensive enough that it's cached across runs
    instead of re-run every single time (see get_first_order_data()).
    """
    orders_df = fetch_mtd_orders_raw(date_from_iso, date_to_iso, session_token)
    return _merge_first_order_dates(orders_df, first_order_df)


def _load_first_order_cache(sh):
    """
    Returns (df, fetched_at_utc) from FIRST_ORDER_CACHE_TAB_NAME, or
    (None, None) if the tab doesn't exist yet or can't be parsed. Row 1 of
    the tab stores ["_CacheFetchedAtUTC", <iso timestamp>]; row 2 is the
    real header (Buyer-ID, First Delivered Order Date); data starts row 3.
    Freshness is judged by the CALLER (get_first_order_data()) — this
    function just returns whatever's there, stale or not, so a stale cache
    can still be used as a last-resort fallback if a live refetch fails.
    """
    try:
        ws = sh.worksheet(FIRST_ORDER_CACHE_TAB_NAME)
    except gspread.WorksheetNotFound:
        return None, None
    values = ws.get_all_values()
    if not values or len(values[0]) < 2:
        return None, None
    try:
        fetched_at = datetime.strptime(values[0][1], "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc)
    except (IndexError, ValueError):
        return None, None
    if len(values) < 2 or not values[1]:
        return pd.DataFrame(columns=["Buyer-ID", "First Delivered Order Date"]), fetched_at
    header, rows = values[1], values[2:]
    return pd.DataFrame(rows, columns=header), fetched_at


def _save_first_order_cache(sh, df: pd.DataFrame) -> None:
    ws = _get_or_create_tab(sh, FIRST_ORDER_CACHE_TAB_NAME, rows=max(1000, len(df) + 10),
                             cols=max(4, len(df.columns) + 2))
    ws.clear()
    fetched_at_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    ws.update([["_CacheFetchedAtUTC", fetched_at_iso], df.columns.tolist()] + _sanitize_for_sheets(df))
    log.info("Refreshed first-order-date cache (%d buyers) at %s", len(df), fetched_at_iso)


def get_first_order_data(sh, session_token: str, buyer_ids_this_month: list) -> pd.DataFrame:
    """
    Returns a (Buyer-ID, First Delivered Order Date) DataFrame for OB/ReOB.
    Backed by a PERMANENT, incrementally-growing cache in
    FIRST_ORDER_CACHE_TAB_NAME (see that constant's config comment for the
    full history/rationale) — a buyer's first-ever delivered order date
    never changes once resolved, so once a buyer is in the cache it's kept
    forever and never re-queried.

    Each run, only buyers in `buyer_ids_this_month` who are NOT already in
    the cache get looked up live (via build_first_order_pipeline_for_buyers(),
    scoped to just those IDs — never an all-time scan of the whole `orders`
    collection). Normally this is a small incremental set (new buyers since
    the last run), which is what keeps the live query fast even though
    `orders` itself is a huge, unindexed collection.

    Never raises: if the live lookup for missing buyers fails, this logs
    and returns the cache as-is (those buyers simply have no first-order-
    date row yet, so their OB/ReOB reads 0 for this run only — same
    non-fatal degrade-gracefully pattern used elsewhere in this script, and
    self-correcting since they stay "missing" and get retried next run).
    """
    cached_df, fetched_at = _load_first_order_cache(sh)
    if cached_df is None:
        cached_df = pd.DataFrame(columns=["Buyer-ID", "First Delivered Order Date"])

    known_ids = set(cached_df["Buyer-ID"])
    missing_ids = [bid for bid in dict.fromkeys(buyer_ids_this_month) if bid and bid not in known_ids]

    if not missing_ids:
        log.info(
            "First-order-date cache already covers all %d of this month's active buyers "
            "(%d buyers cached total) — skipping the Mongo fetch this run.",
            len(set(buyer_ids_this_month)), len(cached_df),
        )
        return cached_df

    log.info(
        "First-order-date cache is missing %d of this month's %d active buyers — "
        "fetching just those from Mongo (scoped, not an all-time scan).",
        len(missing_ids), len(set(buyer_ids_this_month)),
    )
    try:
        new_rows_df = fetch_mongo_native(
            "orders", build_first_order_pipeline_for_buyers(missing_ids), session_token, max_retries=1)
    except Exception:
        log.exception(
            "Live first-order-date fetch for %d missing buyers failed — continuing with the "
            "existing %d-buyer cache as-is. Those buyers' OB/ReOB will read 0 this run only "
            "and should self-correct once a future run's fetch succeeds.",
            len(missing_ids), len(cached_df),
        )
        return cached_df

    if new_rows_df.empty:
        log.info(
            "Mongo returned no rows for the %d missing buyers (none of them have a 'delivered' "
            "order yet) — nothing to add to the cache this run.", len(missing_ids),
        )
        return cached_df

    log.info("Resolved first-order-date for %d/%d previously-missing buyers.",
              len(new_rows_df), len(missing_ids))
    merged_df = (
        pd.concat([cached_df, new_rows_df], ignore_index=True)
          .drop_duplicates(subset="Buyer-ID", keep="last")
    )
    try:
        _save_first_order_cache(sh, merged_df)
    except Exception:
        log.exception("Failed to write the first-order-date cache tab — continuing anyway, will retry next run.")
    return merged_df


# Safe lower bound for an "all orders ever before X" query — well before this
# business's launch, so it behaves like an unbounded scan without needing a
# separate no-lower-bound pipeline variant.
EPOCH_ISO = "2000-01-01T00:00:00.000Z"


def fetch_last_month_active_buyers(last_month_start_iso: str, this_month_start_iso: str,
                                    session_token: str) -> set:
    """
    Buyers who placed a qualifying order in the full previous calendar
    month (IST, converted to UTC) — a bounded, cheap, one-month-wide range
    scan, fetched live every run (no caching needed; unlike the all-time
    scans below, this one has never been slow).
    """
    log.info("Fetching last-month active buyers (Mongo) for window %s -> %s", last_month_start_iso, this_month_start_iso)
    df = fetch_mongo_native(
        "orders", build_buyer_activity_pipeline(last_month_start_iso, this_month_start_iso), session_token)
    ordered_last_month = set(df["Buyer-ID"]) if not df.empty else set()
    log.info("Last-month active buyers: %d", len(ordered_last_month))
    return ordered_last_month


def _load_buyer_set_cache(sh, tab_name: str):
    """
    Generic cache reader for a "set of Buyer-IDs as of some fetch time"
    tab, shared by get_ordered_before_last_month() (and reusable for any
    future all-time buyer-set query). Row 1: ["_CacheFetchedAtUTC", iso];
    row 2: ["Buyer-ID"] header; data from row 3. Returns (None, None) if
    the tab doesn't exist or can't be parsed.
    """
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        return None, None
    values = ws.get_all_values()
    if not values or len(values[0]) < 2:
        return None, None
    try:
        fetched_at = datetime.strptime(values[0][1], "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc)
    except (IndexError, ValueError):
        return None, None
    buyer_ids = {row[0] for row in values[2:] if row and row[0]}
    return buyer_ids, fetched_at


def _save_buyer_set_cache(sh, tab_name: str, buyer_ids: set) -> None:
    ws = _get_or_create_tab(sh, tab_name, rows=max(1000, len(buyer_ids) + 10), cols=2)
    ws.clear()
    fetched_at_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    rows = [["_CacheFetchedAtUTC", fetched_at_iso], ["Buyer-ID"]] + [[bid] for bid in sorted(buyer_ids)]
    ws.update(rows)
    log.info("Refreshed '%s' cache (%d buyers) at %s", tab_name, len(buyer_ids), fetched_at_iso)


def get_ordered_before_last_month(sh, session_token: str, last_month_start_iso: str) -> set:
    """
    Set of Buyer-IDs who placed a qualifying order at any point before last
    month started — needed for Reactivation. Cached the same way and for
    the same reason as get_first_order_data(): this is an UNBOUNDED
    all-time scan of the entire `orders` collection (same pipeline shape,
    same 2026-07-08 "operation exceeded time limit" failure mode as the
    first-order-date scan), so it's refreshed at most once every
    BEFORE_LAST_MONTH_CACHE_MAX_AGE_HOURS instead of every run. Falls back
    to a stale cache, then to an empty set (Reactivation reads 0 that run
    only), exactly like get_first_order_data()'s fallback chain — this
    function never raises.
    """
    cached_ids, fetched_at = _load_buyer_set_cache(sh, BEFORE_LAST_MONTH_CACHE_TAB_NAME)
    age_hours = None
    if fetched_at is not None:
        age_hours = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600
        if cached_ids is not None and age_hours <= BEFORE_LAST_MONTH_CACHE_MAX_AGE_HOURS:
            log.info(
                "Using cached before-last-month buyer set (%d buyers, %.1fh old) — skipping the "
                "expensive all-time Mongo scan this run.", len(cached_ids), age_hours,
            )
            return cached_ids

    log.info("Fetching all buyers active before last month (Mongo) — cache missing or stale")
    try:
        # max_retries=1 for the same reason as get_first_order_data(): this
        # unbounded scan has been failing deterministically, not
        # transiently, so a 3rd identical retry just wastes job time.
        df = fetch_mongo_native(
            "orders", build_buyer_activity_pipeline(EPOCH_ISO, last_month_start_iso), session_token, max_retries=1)
        buyer_ids = set(df["Buyer-ID"]) if not df.empty else set()
    except Exception:
        if cached_ids is not None:
            log.exception(
                "Live before-last-month fetch failed — falling back to the stale cache (%d buyers, "
                "%s) rather than crashing the run. Reactivation may lag slightly until the next successful refresh.",
                len(cached_ids), f"{age_hours:.1f}h old" if age_hours is not None else "age unknown",
            )
            return cached_ids
        log.exception(
            "Live before-last-month fetch failed and no cache exists yet — continuing without it. "
            "Reactivation will read 0 this run only."
        )
        return set()

    try:
        _save_buyer_set_cache(sh, BEFORE_LAST_MONTH_CACHE_TAB_NAME, buyer_ids)
    except Exception:
        log.exception("Failed to write the before-last-month cache tab — continuing anyway, will retry next run.")
    return buyer_ids


# ==========================================================================
# 2. TRANSFORM
# ==========================================================================
GOLD, SILVER, BRONZE, EXPLORER, HUNTER = "Gold", "Silver", "Bronze", "Explorer", "Hunter"


def classify(days: int) -> str:
    if days >= 15:
        return GOLD
    if days >= 10:
        return SILVER
    if days >= 4:
        return BRONZE
    if days >= 2:
        return EXPLORER
    if days == 1:
        return HUNTER
    return "NoOrder"


def _year_month_key(dt_series: pd.Series) -> pd.Series:
    """
    Faster equivalent of dt_series.dt.strftime('%Y-%m') for the two month-
    bucket columns (CustomerCreatedMonth / FirstDeliveredOrderMonth) that
    only ever get compared with == or < against report_month, never
    displayed. Profiling a 400K-row synthetic batch (2026-07-08, ahead of
    order-volume growth) showed pandas' generic per-element dt.strftime()
    was ~80% of _preprocess()'s entire wall time — by far the single
    biggest cost in the whole transform step, well above either
    _aggregate() call. Returns an integer YYYYMM (e.g. 202607) instead of
    the zero-padded string "2026-07" it replaces: an int YYYYMM sorts and
    equality-compares identically to that zero-padded string for every use
    in _aggregate() (OB/ReOB), so this changes nothing about the result,
    just how cheaply it's computed. NaT rows produce NaN either way, same
    as the string version's NaN, so .notna() filtering is unaffected.
    """
    return dt_series.dt.year * 100 + dt_series.dt.month


def _year_month_str_to_key(year_month: str) -> int:
    """"YYYY-MM" -> YYYYMM int, the report_month-side counterpart to _year_month_key()."""
    return int(year_month.replace("-", ""))


def _preprocess(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """
    Shared cleaning step used by both the hourly and daily summaries: date
    parsing, cancellation filtering, numeric coercion, and lifecycle-month
    derivation. Returns (cleaned_df, buyer_col_name).
    """
    df = df.copy()

    df["Order-Date"] = pd.to_datetime(df["Order-Date"], utc=True, errors="coerce")
    df = df.dropna(subset=["Order-Date"])
    df["OrderDateIST"] = df["Order-Date"].dt.tz_convert(IST)
    df["Date"] = df["OrderDateIST"].dt.date.astype(str)
    df["Hour"] = df["OrderDateIST"].dt.hour

    if "Is Cancelled" in df.columns:
        df = df[df["Is Cancelled"] != True]  # noqa: E712 (explicit bool compare; values may be numpy bool/None)
    elif "Status" in df.columns:
        df = df[~df["Status"].astype(str).str.lower().isin(["cancelled", "canceled"])]

    df["Payable Amount"] = pd.to_numeric(df.get("Payable Amount", 0), errors="coerce").fillna(0)

    has_lifecycle_cols = {"Buyer Onboarding Date", "First Delivered Order Date"}.issubset(df.columns)
    if has_lifecycle_cols:
        onboarding = pd.to_datetime(df["Buyer Onboarding Date"], utc=True, errors="coerce").dt.tz_convert(IST)
        first_order = pd.to_datetime(df["First Delivered Order Date"], utc=True, errors="coerce").dt.tz_convert(IST)
        df["CustomerCreatedMonth"] = _year_month_key(onboarding)
        df["FirstDeliveredOrderMonth"] = _year_month_key(first_order)
    else:
        log.warning("Onboarding/First-Order columns not present this run — OB/ReOB will be 0.")
        df["CustomerCreatedMonth"] = None
        df["FirstDeliveredOrderMonth"] = None

    buyer_col = "Buyer-ID" if "Buyer-ID" in df.columns else "Shop Contact"
    return df, buyer_col


def _aggregate(df: pd.DataFrame, buyer_col: str, group_cols: list, report_month: str,
                ordered_last_month: set, ordered_before_last_month: set) -> pd.DataFrame:
    """
    Groups the preprocessed order-level data to `group_cols` (e.g. with or
    without "Hour") and computes all KPI columns. Used for both the hourly
    and daily summaries — the only difference between them is `group_cols`;
    every merchant-lifecycle metric (OB/ReOB/classification/Retention/
    Reactivation) is derived per-buyer for the whole month, so it comes out
    identical at either grain, just reshaped.

    ordered_last_month/ordered_before_last_month are sets of Buyer-ID
    strings computed from Mongo's real order history (see
    fetch_last_month_active_buyers() and get_ordered_before_last_month())
    — not from any state this script persisted itself, so Retention/
    Reactivation are correct from the very first run.
    """
    agg = (df.groupby(group_cols, dropna=False)
             .agg(**{
                 "Shop Name": ("Shop Name", "last") if "Shop Name" in df.columns else ("Shop Contact", "last"),
                 "BuyerId": (buyer_col, "last"),
                 "Orders": ("Order-ID", "count") if "Order-ID" in df.columns else ("Payable Amount", "count"),
                 "GMV": ("Payable Amount", "sum"),
                 "CustomerCreatedMonth": ("CustomerCreatedMonth", "last"),
                 "FirstDeliveredOrderMonth": ("FirstDeliveredOrderMonth", "last"),
             })
             .reset_index())

    report_month_key = _year_month_str_to_key(report_month)
    agg["UniqueMerchant"] = 1
    agg["OB"] = ((agg["CustomerCreatedMonth"] == report_month_key) &
                 (agg["FirstDeliveredOrderMonth"] == report_month_key)).astype(int)
    agg["ReOB"] = ((agg["CustomerCreatedMonth"].notna()) &
                   (agg["CustomerCreatedMonth"] < report_month_key) &
                   (agg["FirstDeliveredOrderMonth"] == report_month_key)).astype(int)

    # --- Month-to-date ordering days per buyer -> classification ---
    order_days = (df.groupby(buyer_col)["Date"].nunique().rename("OrderDaysThisMonth"))
    agg = agg.merge(order_days, left_on="BuyerId", right_index=True, how="left")
    agg["OrderDaysThisMonth"] = agg["OrderDaysThisMonth"].fillna(0).astype(int)
    agg["MerchantType"] = agg["OrderDaysThisMonth"].apply(classify)

    for flag, label in [("GoldFlag", GOLD), ("SilverFlag", SILVER), ("BronzeFlag", BRONZE),
                         ("ExplorerFlag", EXPLORER), ("HunterFlag", HUNTER)]:
        agg[flag] = (agg["MerchantType"] == label).astype(int)
    agg["GSB"] = agg["MerchantType"].isin([GOLD, SILVER, BRONZE]).astype(int)
    agg["NonGSB"] = agg["MerchantType"].isin([EXPLORER, HUNTER]).astype(int)

    # --- Retention / Reactivation, computed live against real order history ---
    # Retention: active last month AND active this month.
    # Reactivation: NOT active last month, but active before last month, AND active this month.
    agg["orderedLastMonth"] = agg["BuyerId"].astype(str).isin(ordered_last_month)
    agg["orderedBeforeLastMonth"] = agg["BuyerId"].astype(str).isin(ordered_before_last_month)
    agg["orderedThisMonth"] = agg["OrderDaysThisMonth"] > 0

    agg["Retention"] = (agg["orderedLastMonth"] & agg["orderedThisMonth"]).astype(int)
    agg["Reactivation"] = ((~agg["orderedLastMonth"]) & agg["orderedBeforeLastMonth"] & agg["orderedThisMonth"]).astype(int)
    return agg


def _rollup_daily_from_hourly(hourly_summary: pd.DataFrame, base_cols: list, kpi_cols: list) -> pd.DataFrame:
    """
    Derives the daily (no-Hour) summary by rolling up the already-computed
    hourly_summary, instead of re-running _aggregate() a second time over
    the full preprocessed order-level data. This is a pure performance
    optimization (2026-07-08, ahead of order volume growth) — it produces
    the same values as calling _aggregate() directly at Date+base_cols
    grain, verified in the test suite, but does one pass over the raw
    order-level data instead of two, which matters once MTD order counts
    get large.

    Why this is safe: every KPI here other than Orders/GMV is a per-buyer
    attribute for the whole month or the whole day (MerchantType, OB, ReOB,
    Retention, Reactivation, the Gold/Silver/.../GSB/NonGSB flags) — it's
    identical across every hourly row for the same buyer on the same day,
    since none of them depend on which hour an order landed in. So:
      - Orders, GMV: SUM across hours (same as summing across all matching
        raw rows directly — sum of the hourly partial sums equals the
        total sum).
      - Everything else: MAX (or "last", for the free-text Shop Name/
        MerchantType label) across hours, which just re-selects the one
        constant value already shared by every hourly row in the group.
    Note: Date/base_cols grouping here is a strict refinement of Date/Hour/
    base_cols, so this is mathematically the same aggregation, just done in
    two cheaper cumulative steps instead of one expensive one from scratch.
    """
    sum_cols = ["Orders", "GMV"]
    last_cols = ["Shop Name", "MerchantType"]
    max_cols = [c for c in kpi_cols if c not in sum_cols + last_cols]

    agg_spec = {c: (c, "sum") for c in sum_cols}
    agg_spec.update({c: (c, "last") for c in last_cols})
    agg_spec.update({c: (c, "max") for c in max_cols})

    daily = (hourly_summary.groupby(["Date"] + base_cols, dropna=False, sort=False)
                          .agg(**agg_spec)
                          .reset_index())
    return (daily[["Date"] + base_cols + kpi_cols]
            .sort_values(["Date"] + base_cols).reset_index(drop=True))


def transform(df: pd.DataFrame, report_month: str, ordered_last_month: set,
              ordered_before_last_month: set) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (hourly_summary_df, daily_summary_df). daily_summary_df is the
    same KPIs rolled up to Date x Warehouse x Salesman x Shop Contact (no
    Hour column) — derived from hourly_summary via _rollup_daily_from_hourly()
    rather than a second full aggregation pass (see that function's docstring).
    """
    df, buyer_col = _preprocess(df)

    base_cols = ["Warehouse", "Salesman Name", "Salesman Contact", "Shop Contact"]
    kpi_cols = ["Shop Name", "Orders", "GMV", "UniqueMerchant", "OB", "ReOB", "Retention", "Reactivation",
                "MerchantType", "GoldFlag", "SilverFlag", "BronzeFlag", "ExplorerFlag", "HunterFlag",
                "GSB", "NonGSB"]

    hourly_agg = _aggregate(df, buyer_col, ["Date", "Hour"] + base_cols, report_month,
                             ordered_last_month, ordered_before_last_month)

    # Sorted by the FULL group-key column set (not just Date/Hour/Warehouse)
    # so row order is fully deterministic for rows that share a Date+Hour+
    # Warehouse but differ in Salesman/Shop Contact — needed so that
    # daily_summary (rolled up from this) always comes out in the same row
    # order regardless of which internal path computed it, matching the
    # equivalence check in the test suite bit-for-bit.
    hourly_summary = (hourly_agg[["Date", "Hour"] + base_cols + kpi_cols]
                       .sort_values(["Date", "Hour"] + base_cols).reset_index(drop=True))
    daily_summary = _rollup_daily_from_hourly(hourly_summary, base_cols, kpi_cols)
    return hourly_summary, daily_summary


# ==========================================================================
# 3. LOAD
# ==========================================================================
def _open_sheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_PATH, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(GOOGLE_SHEET_ID)


def _get_or_create_tab(sh, name: str, rows: int, cols: int):
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=name, rows=rows, cols=cols)


def _sanitize_for_sheets(df: pd.DataFrame) -> list:
    """
    Google Sheets' API requires strict JSON, which does not allow NaN/Infinity
    (unlike Python's json module, which tolerates them by default). Replaces
    any NaN/Inf with None (-> JSON null) before serializing.
    """
    clean = df.astype(object)
    clean = clean.where(pd.notna(clean), None)
    clean = clean.replace([np.inf, -np.inf], None)
    return clean.values.tolist()


def load_summary(sh, df: pd.DataFrame, tab_name: str):
    ws = _get_or_create_tab(sh, tab_name, rows=max(1000, len(df) + 10), cols=len(df.columns) + 2)
    ws.clear()
    ws.update([df.columns.tolist()] + _sanitize_for_sheets(df))
    log.info("Wrote %d rows to '%s'", len(df), tab_name)


# ==========================================================================
# MAIN
# ==========================================================================
def main():
    now_ist = datetime.now(IST)
    # Compute the IST month-to-date window, then convert both edges to UTC
    # before querying — Mongo's orders.createdAt is stored in UTC, so
    # filtering with bare IST calendar-date strings would silently miss the
    # first ~5.5 hours of each IST day (IST is UTC+5:30).
    month_start_ist = now_ist.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start_ist = (now_ist + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    # Previous calendar month's IST boundaries — needed for Retention/Reactivation.
    last_month_start_ist = (month_start_ist - timedelta(days=1)).replace(day=1)

    date_from_iso = month_start_ist.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    date_to_iso = tomorrow_start_ist.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    last_month_start_iso = last_month_start_ist.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    report_month = now_ist.strftime("%Y-%m")

    log.info("=== Hourly merchant dashboard ETL — %s (IST month start %s, UTC window %s -> %s) ===",
              report_month, month_start_ist.isoformat(), date_from_iso, date_to_iso)

    sh = _open_sheet()
    session_token = get_session_token()

    # Three of the four extracts below are fully independent of each other
    # and of the MTD orders fetch, so they're run concurrently. The
    # first-order-date fetch is the one exception: since 2026-07-08 it's
    # scoped to just this month's active buyer IDs (see get_first_order_
    # data()/build_first_order_pipeline_for_buyers()), so it now depends on
    # orders_df and runs right after it instead of inside the pool. This
    # gives up a small amount of parallelism for that one call but is
    # required for correctness — it can no longer be computed independently
    # of who actually ordered this month.
    with ThreadPoolExecutor(max_workers=3) as pool:
        orders_future = pool.submit(fetch_mtd_orders_raw, date_from_iso, date_to_iso, session_token)
        # get_ordered_before_last_month() never raises (it has its own internal
        # stale-cache/empty-set fallback chain) — only the bounded last-month
        # query below can still fail, so it's the one wrapped after .result().
        before_last_month_future = pool.submit(get_ordered_before_last_month, sh, session_token, last_month_start_iso)
        last_month_future = pool.submit(fetch_last_month_active_buyers, last_month_start_iso, date_from_iso, session_token)

        orders_df = orders_future.result()
        ordered_before_last_month = before_last_month_future.result()
        try:
            ordered_last_month = last_month_future.result()
        except Exception:
            # Don't let a transient failure on this secondary metric throw away
            # an otherwise successful Orders/GMV/etc. fetch for this run. Falls
            # back to an empty set (Retention reads 0 just for this run) and
            # self-corrects on the next successful run.
            log.exception(
                "Failed to fetch last-month active buyers — continuing with an empty set so "
                "Orders/GMV/etc. still get written this run. Retention will read 0 this run "
                "only and should self-correct on the next run."
            )
            ordered_last_month = set()

    buyer_ids_this_month = orders_df["Buyer-ID"].dropna().unique().tolist() if not orders_df.empty else []
    first_order_df = get_first_order_data(sh, session_token, buyer_ids_this_month)

    raw = _merge_first_order_dates(orders_df, first_order_df)
    hourly_summary, daily_summary = transform(raw, report_month, ordered_last_month, ordered_before_last_month)

    load_summary(sh, hourly_summary, SHEET_TAB_NAME)
    load_summary(sh, daily_summary, DAILY_SHEET_TAB_NAME)

    log.info("Done — %d hourly rows, %d daily rows pushed for %s.",
              len(hourly_summary), len(daily_summary), report_month)


if __name__ == "__main__":
    main()

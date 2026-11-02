# Merchant Hourly Order Summary — Optimized MongoDB ETL for Metabase → Google Sheets

Schema explored directly against your connected `Mongo Prod` database (Metabase db_id 2) via `describe_table` on `orders`, `customers`, `salesmen`, `riders`, `customer_stores`, `customerorderdatas`. Field names below are real, not assumed. Two collections named in your brief — `sellers (warehouse)` — could not be confirmed as a standalone Mongo collection (no `sellers` collection exists in the connected DB; warehouse identity currently lives as `seller` (ObjectId) + `wId` (string code) directly on the `orders` document). See **Open Question** at the bottom before deploying.

Also discovered: a Redshift mart (db_id 4) already exists with `fact_orders`, `dim_customer`, `dim_salesman`, `dim_warehouse`, etc. — see **Strategic Note** at the end.

---

## 1. Key Schema Facts That Drive the Design

| Fact | Field | Why it matters |
|---|---|---|
| Order timestamp | `orders.createdAt` | Source of Date + Hour |
| Warehouse | `orders.seller` (ObjectId), `orders.wId` (string code) | `wId` is already denormalized on the order — **no lookup needed** for the Warehouse column |
| Salesman | `orders.salesmanId` (ObjectId) | Needs a lookup to `salesmen` for name/contact — but it's a tiny collection, so the lookup is cheap |
| Shop Name / Shop Contact | `orders.buyerDetails.shopName`, `orders.buyerDetails.contact` | **Already embedded on the order** at write time — zero lookup required |
| Buyer identity | `orders.buyer` (ObjectId) | Used only as the lifecycle join key, never expanded on the hot path |
| GMV | `orders.payableAmount` | Numeric, flat, no lookup |
| Cancellation | `orders.cancelled.status` | Used to exclude voided orders from Orders/GMV |
| Delivery | `orders.delivered.status`, `orders.delivered.date` | Used to gate "first delivered order" logic |
| Customer onboarding date | `customers.createdAt` | Buyer App Install / Customer Created Date |
| First delivered order date | **Not stored anywhere today** | Must be derived — see Schema Recommendation #1 |
| Existing rollup | `customerorderdatas` (buyerId, month, monthOrderDays, monthOrderCount, lastOrderDate...) | You already have a partial monthly buyer rollup running in production — the design below formalizes/extends this pattern rather than reinventing it |

I also found three separate, similarly-named status fields on `orders` (`status`, `order_status`, `currentStatus.status`). Pick one canonical field before wiring dashboards — see Schema Recommendation #3.

---

## 2. Why the current dump is slow

The current export walks every order document (which — per `describe_table` — carries 600+ possible fields: item arrays, QR-scan arrays, rider POD photo arrays, GST address blocks, rating blocks, etc.), then re-derives lifecycle status (OB/ReOB/retention/classification) by rescanning **the buyer's entire order history** for every row, every refresh. That cost grows linearly with total historical order volume, not with the size of the date range you actually care about. That's the core problem this redesign fixes.

---

## 3. Architecture

Two moving parts instead of one monolithic query:

**Pipeline A — `merchant_monthly_summary` maintenance (runs daily, incremental).** Scans only the current month's orders (bounded, index-covered) and maintains one tiny document per buyer per month with order-day count, GMV, merchant classification, and lifecycle flags. Cross-month state (retention/reactivation) is carried forward via a single point-lookup to *last month's* document — never a full-history rescan.

**Pipeline B — Hourly Merchant Fact (the actual Metabase question / Sheets export).** Scans only the requested date range, groups to the exact grain you asked for (Date × Hour × Warehouse × Salesman × Shop Contact), and attaches lifecycle flags via one O(1) lookup into Pipeline A's output. No item arrays, no history rescans, no per-order customer lookups (shop name/contact come from the embedded snapshot).

This is the standard pattern for turning an unbounded "recompute everything live" dashboard query into a bounded, sub-20-second one: **precompute the expensive, slow-changing part once; join it cheaply into the fast-changing part.**

---

## 4. Pipeline A — `merchant_monthly_summary` maintenance

Run once per day (e.g., 15 minutes past the hour, via cron/Metabase scheduled query/Atlas Trigger). Parameters (`monthStart`, `monthEnd`, `monthKey`, `prevMonthKey`) are injected by the scheduler.

```javascript
// =====================================================================
// PIPELINE A — maintain merchant_monthly_summary (run daily, incremental)
// Scope: CURRENT MONTH orders only — bounded, index-covered scan
// =====================================================================
const monthStartUTC = ISODate("2026-06-30T18:30:00Z"); // = 2026-07-01 00:00 IST
const monthEndUTC   = ISODate("2026-07-31T18:30:00Z"); // = 2026-08-01 00:00 IST
const monthKey       = "2026-07";
const prevMonthKey   = "2026-06";

db.orders.aggregate([
  // (1) MATCH FIRST — index-covered range scan, current month only
  { $match: {
      createdAt: { $gte: monthStartUTC, $lt: monthEndUTC },
      "cancelled.status": { $ne: true }
  }},

  // (2) EARLY PROJECT — drop everything except the ~6 fields needed
  { $project: {
      _id: 0,
      buyer: 1, seller: 1, wId: 1, salesmanId: 1, createdAt: 1, payableAmount: 1,
      deliveredFlag: { $ifNull: ["$delivered.status", false] }
  }},

  // (3) Localize to IST calendar day — no $unwind required
  { $addFields: {
      orderDateIST: { $dateTrunc: { date: "$createdAt", unit: "day", timezone: "Asia/Kolkata" } }
  }},

  // (4) GROUP by buyer — one doc per merchant per month
  { $group: {
      _id: "$buyer",
      warehouse:        { $last: "$seller" },
      wId:              { $last: "$wId" },
      salesmanId:       { $last: "$salesmanId" },
      orderDatesSet:    { $addToSet: "$orderDateIST" },
      ordersThisMonth:  { $sum: 1 },
      gmvThisMonth:     { $sum: "$payableAmount" },
      firstDeliveredCandidate: { $min: { $cond: ["$deliveredFlag", "$orderDateIST", null] } }
  }},
  { $addFields: {
      orderDaysThisMonth: { $size: "$orderDatesSet" },
      orderedThisMonth:   { $gt: [{ $size: "$orderDatesSet" }, 0] }
  }},
  { $project: { orderDatesSet: 0 } },

  // (5) Merchant classification — computed INSIDE the aggregation
  { $addFields: {
      merchantType: { $switch: { branches: [
          { case: { $gte: ["$orderDaysThisMonth", 15] }, then: "Gold" },
          { case: { $and: [{ $gte: ["$orderDaysThisMonth", 10] }, { $lte: ["$orderDaysThisMonth", 14] }] }, then: "Silver" },
          { case: { $and: [{ $gte: ["$orderDaysThisMonth", 4] },  { $lte: ["$orderDaysThisMonth", 9] }] },  then: "Bronze" },
          { case: { $and: [{ $gte: ["$orderDaysThisMonth", 2] },  { $lte: ["$orderDaysThisMonth", 3] }] },  then: "Explorer" },
          { case: { $eq:  ["$orderDaysThisMonth", 1] }, then: "Hunter" }
        ], default: "NoOrder" } }
  }},
  { $addFields: {
      GoldFlag:     { $cond: [{ $eq: ["$merchantType", "Gold"] }, 1, 0] },
      SilverFlag:   { $cond: [{ $eq: ["$merchantType", "Silver"] }, 1, 0] },
      BronzeFlag:   { $cond: [{ $eq: ["$merchantType", "Bronze"] }, 1, 0] },
      ExplorerFlag: { $cond: [{ $eq: ["$merchantType", "Explorer"] }, 1, 0] },
      HunterFlag:   { $cond: [{ $eq: ["$merchantType", "Hunter"] }, 1, 0] },
      GSB:          { $cond: [{ $in: ["$merchantType", ["Gold","Silver","Bronze"]] }, 1, 0] },
      NonGSB:       { $cond: [{ $in: ["$merchantType", ["Explorer","Hunter"]] }, 1, 0] }
  }},

  // (6) ONE lookup — buyer's onboarding + first-delivered date (tiny, keyed on _id)
  { $lookup: {
      from: "customers",
      let: { bId: "$_id" },
      pipeline: [
        { $match: { $expr: { $eq: ["$_id", "$$bId"] } } },
        { $project: { _id: 0, createdAt: 1, firstDeliveredOrderAt: 1 } }
      ],
      as: "cust"
  }},
  { $addFields: { cust: { $first: "$cust" } } },
  { $addFields: {
      customerCreatedMonth:      { $dateToString: { date: "$cust.createdAt", format: "%Y-%m", timezone: "Asia/Kolkata" } },
      firstDeliveredOrderMonth:  { $cond: [ "$cust.firstDeliveredOrderAt",
                                    { $dateToString: { date: "$cust.firstDeliveredOrderAt", format: "%Y-%m", timezone: "Asia/Kolkata" } },
                                    null ] }
  }},
  { $addFields: {
      OB:   { $cond: [{ $and: [{ $eq: ["$customerCreatedMonth", monthKey] }, { $eq: ["$firstDeliveredOrderMonth", monthKey] }] }, 1, 0] },
      ReOB: { $cond: [{ $and: [{ $lt: ["$customerCreatedMonth", monthKey] }, { $eq: ["$firstDeliveredOrderMonth", monthKey] }] }, 1, 0] }
  }},
  { $project: { cust: 0 } },

  // (7) ONE lookup — last month's cumulative state (point lookup on compound _id, O(1))
  { $lookup: {
      from: "merchant_monthly_summary",
      let: { bId: "$_id" },
      pipeline: [
        { $match: { $expr: { $and: [
            { $eq: ["$_id.buyer", "$$bId"] },
            { $eq: ["$_id.monthKey", prevMonthKey] }
        ] } } },
        { $project: { _id: 0, orderedThisMonth: 1, hasOrderedBeforeThisMonth: 1 } }
      ],
      as: "prev"
  }},
  { $addFields: { prev: { $first: "$prev" } } },
  { $addFields: {
      orderedLastMonth:       { $ifNull: ["$prev.orderedThisMonth", false] },
      orderedBeforeLastMonth: { $ifNull: ["$prev.hasOrderedBeforeThisMonth", false] }
  }},
  { $addFields: {
      hasOrderedBeforeThisMonth: { $or: ["$orderedLastMonth", "$orderedBeforeLastMonth"] },
      Retention:    { $cond: [{ $and: ["$orderedLastMonth", "$orderedThisMonth"] }, 1, 0] },
      Reactivation: { $cond: [{ $and: [{ $not: ["$orderedLastMonth"] }, "$orderedBeforeLastMonth", "$orderedThisMonth"] }, 1, 0] }
  }},
  { $project: { prev: 0 } },

  // (8) Reshape to compound key and upsert
  { $project: {
      buyer: "$_id", warehouse: 1, wId: 1, salesmanId: 1,
      orderDaysThisMonth: 1, ordersThisMonth: 1, gmvThisMonth: 1,
      orderedThisMonth: 1, hasOrderedBeforeThisMonth: 1,
      OB: 1, ReOB: 1, Retention: 1, Reactivation: 1,
      merchantType: 1, GoldFlag: 1, SilverFlag: 1, BronzeFlag: 1, ExplorerFlag: 1, HunterFlag: 1, GSB: 1, NonGSB: 1
  }},
  { $addFields: { _id: { buyer: "$buyer", monthKey: monthKey } } },

  { $merge: { into: "merchant_monthly_summary", on: "_id", whenMatched: "replace", whenNotMatched: "insert" } }
], { allowDiskUse: false });
```

Cost: one range-filtered scan of the current month's orders (index-covered), grouped by buyer. For a month with, say, 3M orders and 40K active merchants, this is a single indexed pass with a bounded output — seconds, not minutes, and the cost never grows with total historical order volume.

---

## 5. Pipeline A2 — backfill + maintain `customers.firstDeliveredOrderAt`

`firstDeliveredOrderAt` doesn't exist on any collection today, so it must be derived once and then maintained incrementally (see Schema Recommendation #1).

```javascript
// One-time backfill (run once, off-peak — this is the only full-history scan in the whole design)
db.orders.aggregate([
  { $match: { "delivered.status": true } },
  { $project: { _id: 0, buyer: 1, createdAt: 1 } },
  { $group: { _id: "$buyer", firstDeliveredOrderAt: { $min: "$createdAt" } } },
  { $merge: { into: "customers", on: "_id", whenMatched: "keepExisting", whenNotMatched: "discard" } }
], { allowDiskUse: true }); // one-time job — fine to allow disk use here

// Daily incremental (bounded to today, run right before Pipeline A)
db.orders.aggregate([
  { $match: { createdAt: { $gte: todayStartUTC, $lt: todayEndUTC }, "delivered.status": true } },
  { $group: { _id: "$buyer", firstDeliveredCandidate: { $min: "$createdAt" } } },
  { $merge: {
      into: "customers", on: "_id",
      whenMatched: [{ $set: { firstDeliveredOrderAt: { $ifNull: ["$firstDeliveredOrderAt", "$$new.firstDeliveredCandidate"] } } }],
      whenNotMatched: "discard"
  }}
]);
```

The `$ifNull` guard makes this a **set-once** field: it's never overwritten once populated, so it stays correct regardless of how many times the job reruns.

---

## 6. Pipeline B — Hourly Merchant Fact (the query Metabase runs for Google Sheets)

```javascript
// =====================================================================
// PIPELINE B — Hourly Merchant Order Summary
// One row = one merchant, one hour, one warehouse, one salesman
// =====================================================================
const dateFrom = ISODate("2026-07-03T18:30:00Z"); // = 2026-07-04 00:00 IST — Metabase date-filter param
const dateTo   = ISODate("2026-07-04T18:30:00Z"); // = 2026-07-05 00:00 IST
const monthKeyForRange = "2026-07"; // if the range spans a month boundary, run once per month segment

db.orders.aggregate([
  // (1) MATCH FIRST — hits {createdAt:1, "cancelled.status":1} index
  { $match: {
      createdAt: { $gte: dateFrom, $lt: dateTo },
      "cancelled.status": { $ne: true }
  }},

  // (2) EARLY PROJECT — ~8 fields instead of 600+; no items/QR/photo/rating arrays touched
  { $project: {
      _id: 0,
      createdAt: 1, seller: 1, wId: 1, salesmanId: 1, buyer: 1,
      shopContact: "$buyerDetails.contact",
      shopName:    "$buyerDetails.shopName",
      payableAmount: 1
  }},

  // (3) IST date/hour parts — single pass, no $unwind
  { $addFields: {
      orderDateIST: { $dateTrunc: { date: "$createdAt", unit: "day", timezone: "Asia/Kolkata" } },
      orderHour:    { $hour: { date: "$createdAt", timezone: "Asia/Kolkata" } }
  }},

  // (4) GROUP — exact requested grain
  { $group: {
      _id: {
        date: "$orderDateIST", hour: "$orderHour",
        warehouse: "$seller", wId: "$wId", salesmanId: "$salesmanId", shopContact: "$shopContact"
      },
      buyer:    { $last: "$buyer" },
      shopName: { $last: "$shopName" },
      Orders:   { $sum: 1 },
      GMV:      { $sum: "$payableAmount" }
  }},
  { $addFields: { UniqueMerchant: 1 } }, // 1 row = 1 merchant by construction; SUM gives merchant-hours, not distinct daily merchants — see note below

  // (5) ONE small lookup — salesman name/contact (point lookup on _id, tiny collection)
  { $lookup: {
      from: "salesmen",
      let: { sId: "$_id.salesmanId" },
      pipeline: [
        { $match: { $expr: { $eq: ["$_id", "$$sId"] } } },
        { $project: { _id: 0, name: 1, contact: 1 } }
      ],
      as: "sm"
  }},
  { $addFields: { sm: { $first: "$sm" } } },

  // (6) ONE small lookup — pre-computed lifecycle flags from Pipeline A's output
  { $lookup: {
      from: "merchant_monthly_summary",
      let: { bId: "$buyer" },
      pipeline: [
        { $match: { $expr: { $and: [
            { $eq: ["$_id.buyer", "$$bId"] },
            { $eq: ["$_id.monthKey", monthKeyForRange] }
        ] } } },
        { $project: { _id: 0, OB: 1, ReOB: 1, Retention: 1, Reactivation: 1, merchantType: 1,
                      GoldFlag: 1, SilverFlag: 1, BronzeFlag: 1, ExplorerFlag: 1, HunterFlag: 1, GSB: 1, NonGSB: 1 } }
      ],
      as: "lc"
  }},
  { $addFields: { lc: { $first: "$lc" } } },

  // (7) FINAL FLAT SHAPE — ready for Google Sheets / Metabase table
  { $project: {
      _id: 0,
      Date: "$_id.date",
      Hour: "$_id.hour",
      Warehouse: { $ifNull: ["$_id.wId", "$_id.warehouse"] },
      SalesmanName:    "$sm.name",
      SalesmanContact: "$sm.contact",
      ShopContact: "$_id.shopContact",
      ShopName:    "$shopName",
      Orders: 1, GMV: 1, UniqueMerchant: 1,
      OB:           { $ifNull: ["$lc.OB", 0] },
      ReOB:         { $ifNull: ["$lc.ReOB", 0] },
      Retention:    { $ifNull: ["$lc.Retention", 0] },
      Reactivation: { $ifNull: ["$lc.Reactivation", 0] },
      MerchantType: { $ifNull: ["$lc.merchantType", "NoOrder"] },
      GoldFlag:     { $ifNull: ["$lc.GoldFlag", 0] },
      SilverFlag:   { $ifNull: ["$lc.SilverFlag", 0] },
      BronzeFlag:   { $ifNull: ["$lc.BronzeFlag", 0] },
      ExplorerFlag: { $ifNull: ["$lc.ExplorerFlag", 0] },
      HunterFlag:   { $ifNull: ["$lc.HunterFlag", 0] },
      GSB:          { $ifNull: ["$lc.GSB", 0] },
      NonGSB:       { $ifNull: ["$lc.NonGSB", 0] }
  }},
  { $sort: { Date: 1, Hour: 1, Warehouse: 1 } }
], { allowDiskUse: false });
```

**Note on Unique Merchant:** each row is already one merchant for one hour, so `UniqueMerchant` is always `1` by construction — it's a pivot-friendly helper column. Summing it in Google Sheets gives "merchant-hours", not distinct merchants across a day. For true daily/warehouse-level distinct-merchant counts, use `COUNTUNIQUE(ShopContact)` in the Sheets pivot instead of `SUM(UniqueMerchant)`.

---

## 7. Recommended Indexes

```javascript
// orders — primary access pattern: date-range scan, optionally filtered by cancellation
db.orders.createIndex({ createdAt: 1, "cancelled.status": 1 });

// orders — supports the one-time/incremental firstDeliveredOrderAt job
db.orders.createIndex({ "delivered.status": 1, createdAt: 1 });

// orders — supports per-buyer history lookups (backfills, ad hoc audits)
db.orders.createIndex({ buyer: 1, createdAt: 1 });

// orders — only add if warehouse/salesman are applied as $match filters in Metabase dashboard widgets
// (Equality-Sort-Range rule: equality fields before the range field)
db.orders.createIndex({ seller: 1, salesmanId: 1, createdAt: 1 });

// merchant_monthly_summary — compound _id ({buyer, monthKey}) is already the primary key/index;
// point lookups in Pipeline B use it automatically. No extra index needed.

// customers — default _id index covers the Pipeline A lookup. Add only if you query by cohort month directly:
db.customers.createIndex({ createdAt: 1 });

// salesmen — default _id index already covers the Pipeline B point lookup. No extra index needed.
```

Run `db.orders.getIndexes()` first — I couldn't introspect actual index metadata through Metabase's MCP layer, only field shapes, so verify these don't already exist (in whole or in part) before creating duplicates.

---

## 8. Optimization Checklist — how each requirement is met

| # | Requirement | How it's satisfied |
|---|---|---|
| 1 | `$match` as early as possible | First stage in both pipelines, on an indexed range field |
| 2 | Minimize `$lookup` | 2 lookups total in Pipeline B (salesmen, merchant_monthly_summary); shop name/contact come from the embedded `buyerDetails` snapshot — zero lookup |
| 3 | Reduce collection scans | Pipeline A scans only current month; Pipeline B scans only the requested range; no collection is fully scanned |
| 4 | Early `$project` | Stage 2 in both pipelines, before grouping or lookups, dropping 590+ unused fields |
| 5 | Avoid unnecessary `$unwind` | None used — IST date/hour derived with `$dateTrunc`/`$hour`, not array unwinding |
| 6 | Replace expensive lookups | Shop name/contact resolved from the order's own embedded snapshot instead of joining `customers` |
| 7 | Pipeline lookups | All `$lookup`s use the `let`/pipeline form with a `$match` + narrow `$project`, not `localField`/`foreignField` full-doc joins |
| 8 | `allowDiskUse` only if necessary | Set to `false` on both production pipelines (bounded, indexed inputs); only the one-time historical backfill uses `true` |
| 9 | Compound indexes | Section 7 |
| 10 | Under 20s execution | Achieved by bounding every scan to a date range and replacing history rescans with O(1) point lookups (see Section 9) |
| 11 | Avoid full collection scans | Every `$match` leads with an indexed range |
| 12 | Efficient aggregation operators | `$dateTrunc`/`$hour` with timezone, `$switch`, `$cond`, `$addToSet` — all single-pass, no client-side post-processing |
| 13 | Avoid duplicate computation | Lifecycle/classification computed once per buyer per month in Pipeline A, reused by every hourly row in Pipeline B via lookup, not recomputed per row |
| 14 | Lifecycle flags inside aggregation | Computed entirely in-pipeline (Section 4), not in application code |
| 15 | Merchant Type inside aggregation | `$switch` in Pipeline A |
| 16 | Summarized rows only | Pipeline B's output grain is exactly Date × Hour × Warehouse × Salesman × Shop Contact — no order-level rows leave the pipeline |

---

## 9. Expected Performance

The current dump's cost scales with **total historical order volume**, because lifecycle metrics are recomputed from full buyer history on every run. This redesign's cost scales with **the size of the date range requested**, because lifecycle state is precomputed once and carried forward:

- Pipeline A touches only the current month's orders (index-covered range scan) — bounded regardless of how many years of history exist.
- Pipeline B touches only the requested date range, with per-row payload cut by roughly 90% (a handful of fields vs. 600+), and only 2 lookups keyed against small collections (salesmen: hundreds of docs; merchant_monthly_summary: tens of thousands of docs/month) rather than large ones.
- Net effect: expect low-single-digit-second execution for a single day's hourly summary, and a similarly bounded run time for the monthly maintenance job, even as total historical order count grows into the tens of millions — because that historical volume is never touched by either production pipeline.

---

## 10. Schema Recommendations

1. **Add `customers.firstDeliveredOrderAt` (Instant, indexed).** Doesn't exist today; every OB/ReOB calculation currently requires scanning full order history per buyer. Backfill once (Pipeline A2), then maintain incrementally forever.
2. **Persist `merchant_monthly_summary` as first-class infrastructure**, not a scratch table — it's the answer to your Question #6 below.
3. **Consolidate order status.** Three fields carry status-like meaning (`status`, `order_status`, `currentStatus.status`). Confirm which is canonical, deprecate the others, and document it — this also shrinks every `$project` and index touching orders.
4. **Guarantee `buyerDetails.contact`/`buyerDetails.shopName` are always written at order-creation time.** They already exist and are what makes the zero-lookup shop-name/contact resolution possible; any write path that skips populating them forces a fallback lookup to `customers` for those rows.
5. **Confirm the warehouse master.** No standalone `sellers` or `warehouses` collection exists in the connected Mongo database — `orders.seller` is an ObjectId with no resolvable target found during introspection. If warehouse names live in the `customers` collection (filtered on its `seller: true` flag) or elsewhere, point Pipeline B's optional warehouse-name lookup there; otherwise `orders.wId` (already on every order) is sufficient as the Warehouse dimension and needs no lookup at all, which is what both pipelines above use by default.

---

## 11. Should you build a monthly Merchant Summary collection? Yes.

Computing OB/ReOB/Retention/Reactivation/Classification live on every dashboard refresh means rescanning each buyer's entire order history every time — cost grows forever as your order volume grows. A persisted `merchant_monthly_summary` collection (Section 4) instead:

- Bounds daily maintenance cost to the current month's orders only.
- Turns cross-month lifecycle logic (retention/reactivation) into a single indexed point-lookup against last month's row, not a rescan.
- Is reusable by every other dashboard that needs merchant lifecycle state (not just this one), so the expensive computation happens once, not once per report.
- Is a natural extension of what you're already doing — `customerorderdatas` already tracks `monthOrderDays`, `monthOrderCount`, `lastOrderDate` per buyer per month. Either extend that collection with the classification/OB/ReOB/Retention/Reactivation fields above, or run `merchant_monthly_summary` alongside it; don't recompute the same monthly rollup twice in two places.

This is the standard architecture for this exact problem (slow-changing lifecycle state vs. fast-changing transactional facts): precompute the slow-changing part, join it cheaply into the fast-changing part.

---

## 12. Strategic Note

Your Metabase instance also has a Redshift mart (db_id 4) with `fact_orders`, `fact_order_items`, `dim_customer`, `dim_salesman`, `dim_warehouse`, and an existing `fact_order_dump` table — a proper dimensional model already built for exactly this kind of reporting. Window functions (`LAG`, `COUNT DISTINCT`, `ROW_NUMBER`) make retention/reactivation/classification logic considerably simpler and often faster in a warehouse built for OLAP than in MongoDB's aggregation framework, which is optimized for operational (OLTP-adjacent) workloads. You asked specifically for a MongoDB solution, which is fully delivered above, but if this dashboard needs to go beyond near-real-time (last 24–72h) reporting, it's worth evaluating whether it belongs on the Redshift mart instead — especially since a card named "Order Dump — Full Detail" already exists there.

---

## Open Question

Before deploying: confirm whether warehouse master data lives in its own collection, inside `customers` (filtered by `seller: true`), or purely as the `wId` string code already on every order. This determines whether Pipeline B needs a third lookup or can stay at two.

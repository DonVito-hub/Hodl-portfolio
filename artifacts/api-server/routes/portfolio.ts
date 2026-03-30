import { Router, type IRouter } from "express";
import { eq, lte } from "drizzle-orm";
import { db, transactionsTable, quarterlySnapshotsTable, manualPerformanceTable, priceOverridesTable, priceCacheTable, portfolioConfigTable } from "@workspace/db";
import { captureQuarterPrices, refreshAllPricesNow } from "../jobs/quarterlyPriceFetch";
import {
  getQuoteNok,
  getYtdStartPriceNok,
  getStockMetrics,
  fetchTickerHistoryNok,
  fetchFXHistories,
  getPriceForDate,
  normalizeTicker,
  type TickerHistory,
} from "../lib/market";

const router: IRouter = Router();

interface HoldingCalc {
  ticker: string;
  name: string | null;
  assetType: string;
  quantity: number;
  totalCost: number; // in NOK
  firstBuyDate: string; // ISO date of first purchase, e.g. "2026-01-23"
}

type Transaction = {
  ticker: string;
  name: string | null;
  assetType: string;
  action: string;
  quantity: string;
  priceNok: string;
  date: string;
};

function computeHoldingsFromTx(transactions: Transaction[]): HoldingCalc[] {
  const map = new Map<string, HoldingCalc>();

  for (const tx of transactions) {
    const ticker = normalizeTicker(tx.ticker);
    const qty = Number(tx.quantity);
    const price = Number(tx.priceNok);

    if (!map.has(ticker)) {
      map.set(ticker, {
        ticker,
        name: tx.name ?? null,
        assetType: tx.assetType,
        quantity: 0,
        totalCost: 0,
        firstBuyDate: tx.action === "buy" ? tx.date : "9999-12-31",
      });
    }

    const h = map.get(ticker)!;
    if (tx.name && !h.name) h.name = tx.name;

    if (tx.action === "buy") {
      h.totalCost += qty * price;
      h.quantity += qty;
    } else if (tx.action === "sell") {
      const avgCost = h.quantity > 0 ? h.totalCost / h.quantity : price;
      h.totalCost -= qty * avgCost;
      h.quantity -= qty;
    }
  }

  return Array.from(map.values()).filter((h) => h.quantity > 0.00001);
}

async function computeHoldings(): Promise<HoldingCalc[]> {
  const transactions = await db.select().from(transactionsTable).orderBy(transactionsTable.date);
  return computeHoldingsFromTx(transactions as Transaction[]);
}

router.get("/portfolio/holdings", async (req, res): Promise<void> => {
  const holdings = await computeHoldings();

  if (holdings.length === 0) {
    res.json([]);
    return;
  }

  // Load all price sources at once: manual overrides, price cache, YTD start (Q1 snapshot), config
  const thisYear = new Date().getFullYear();
  const [overrideRows, cacheRows, ytdSnapshotRows, configRowsH] = await Promise.all([
    db.select().from(priceOverridesTable),
    db.select().from(priceCacheTable),
    db.select().from(quarterlySnapshotsTable).where(eq(quarterlySnapshotsTable.quarter, `${thisYear}-Q1`)),
    db.select().from(portfolioConfigTable),
  ]);
  const overrideMap = new Map(overrideRows.map((r) => [r.ticker, Number(r.priceNok)]));
  const cacheMap   = new Map(cacheRows.map((r) => [r.ticker, Number(r.priceNok)]));
  const ytdStartMap = new Map(ytdSnapshotRows.map((r) => [r.ticker, Number(r.priceNok)]));
  const configMapH  = new Map(configRowsH.map((r) => [r.key, r.value]));
  const privateTickers = new Set(
    (configMapH.get("private_tickers") ?? "")
      .split(",").map((t: string) => t.trim()).filter(Boolean)
  );

  const NON_MARKET = ["private", "cash"];
  const isPrivateH = (h: HoldingCalc) => NON_MARKET.includes(h.assetType) || privateTickers.has(h.ticker);
  const marketHoldings = holdings.filter((h) => !isPrivateH(h));
  const privateHoldings = holdings.filter((h) => isPrivateH(h));

  // First-time setup: for any market holding with no price at all, fetch live and cache it
  const missingTickers = marketHoldings.filter((h) => !overrideMap.has(h.ticker) && !cacheMap.has(h.ticker));
  if (missingTickers.length > 0) {
    await Promise.all(missingTickers.map(async (h) => {
      try {
        const { price: priceNok } = await getQuoteNok(h.ticker);
        if (priceNok > 0) {
          const capturedAt = new Date().toISOString().slice(0, 10);
          await db.insert(priceCacheTable)
            .values({ ticker: h.ticker, priceNok: String(priceNok), capturedAt, source: "live" })
            .onConflictDoUpdate({ target: priceCacheTable.ticker, set: { priceNok: String(priceNok), capturedAt, source: "live" } });
          cacheMap.set(h.ticker, priceNok);
        }
      } catch { /* leave price as 0 */ }
    }));
  }

  // For holdings bought before Jan 2 but with no Q1 snapshot (e.g. added to portfolio
  // after the Jan cron ran), fetch the historical Jan 2 close price on-demand and cache
  // it in quarterly_snapshots so future requests are instant.
  const jan2Str = `${thisYear}-01-02`;
  // Fetch Jan 2 price only for holdings whose first purchase was BEFORE Jan 2
  // (prior-year positions). Positions opened this year use GAV as their YTD baseline.
  const needsDynamicYtd = marketHoldings.filter(
    (h) => h.firstBuyDate <= jan2Str && !ytdStartMap.has(h.ticker) && !overrideMap.has(h.ticker),
  );
  if (needsDynamicYtd.length > 0) {
    await Promise.all(needsDynamicYtd.map(async (h) => {
      try {
        const price = await getYtdStartPriceNok(h.ticker);
        if (price && price > 0) {
          ytdStartMap.set(h.ticker, price);
          await db.insert(quarterlySnapshotsTable)
            .values({ ticker: h.ticker, quarter: `${thisYear}-Q1`, priceNok: String(price), capturedAt: jan2Str, assetType: h.assetType })
            .onConflictDoNothing();
        }
      } catch { /* ignore — YTD will remain null */ }
    }));
  }

  const getPrice = (ticker: string, avgCostPerUnit: number): number =>
    overrideMap.get(ticker) ?? cacheMap.get(ticker) ?? avgCostPerUnit;

  const marketValue  = marketHoldings.reduce((sum, h) => sum + getPrice(h.ticker, h.totalCost / (h.quantity || 1)) * h.quantity, 0);
  const privateValue = privateHoldings.reduce((sum, h) => sum + h.totalCost, 0);
  const totalValue   = marketValue + privateValue;

  const marketResults = marketHoldings.map((h) => {
    const overridePrice = overrideMap.get(h.ticker) ?? null;
    const cachedPrice   = cacheMap.get(h.ticker) ?? null;
    const avgCost  = h.quantity > 0 ? h.totalCost / h.quantity : 0;
    const currentPrice  = overridePrice ?? cachedPrice ?? avgCost;
    const isManualPrice = overrideMap.has(h.ticker);
    const currentValue  = currentPrice * h.quantity;
    const costBasis = h.totalCost;
    const gainLoss  = currentValue - costBasis;
    const gainLossPct = costBasis > 0 ? (gainLoss / costBasis) * 100 : 0;

    // YTD start: if the position was first bought AFTER Jan 2, use avgCost (GAV)
    // so YTD reflects the actual return since purchase. If bought on/before Jan 2,
    // use the Q1 snapshot (actual Jan 2 close price).
    const jan2 = `${thisYear}-01-02`;
    const boughtAfterJan2 = h.firstBuyDate > jan2;
    const snapshotStart = ytdStartMap.get(h.ticker) ?? null;
    let ytdStart: number | null;
    if (isManualPrice) {
      ytdStart = currentPrice; // manual-price holdings always show 0 YTD
    } else if (boughtAfterJan2) {
      ytdStart = avgCost; // position opened this year — YTD = return from GAV
    } else {
      ytdStart = snapshotStart; // held before year start — YTD = return from Jan 2
    }
    const ytdGainLoss = ytdStart !== null ? (currentPrice - ytdStart) * h.quantity : null;
    // YTD % — relative to the YTD baseline price (Jan 2 price or GAV if bought this year).
    // This gives the stock's own calendar-year % for holdings held since Jan 2,
    // or return-since-purchase for positions opened this year.
    const ytdGainLossPct = ytdGainLoss !== null && ytdStart !== null && ytdStart > 0
      ? ((currentPrice - ytdStart) / ytdStart) * 100
      : null;

    return {
      ticker: h.ticker,
      name: h.name ?? null,
      assetType: h.assetType,
      quantity: h.quantity,
      avgCostNok: avgCost,
      currentPriceNok: currentPrice,
      currentValueNok: currentValue,
      costBasisNok: costBasis,
      gainLossNok: gainLoss,
      gainLossPct,
      ytdStartPriceNok: ytdStart ?? null,
      ytdGainLossNok: ytdGainLoss ?? null,
      ytdGainLossPct: ytdGainLossPct ?? null,
      allocationPct: totalValue > 0 ? (currentValue / totalValue) * 100 : 0,
      isManualPrice,
      manualPriceNok: overridePrice,
    };
  });

  const privateResults = privateHoldings.map((h) => {
    const avgCost = h.quantity > 0 ? h.totalCost / h.quantity : 0;
    return {
      ticker: h.ticker,
      name: h.name ?? h.ticker,
      assetType: h.assetType,
      quantity: h.quantity,
      avgCostNok: avgCost,
      currentPriceNok: avgCost,
      currentValueNok: h.totalCost,
      costBasisNok: h.totalCost,
      gainLossNok: 0,
      gainLossPct: 0,
      ytdStartPriceNok: null,
      ytdGainLossNok: null,
      ytdGainLossPct: null,
      allocationPct: totalValue > 0 ? (h.totalCost / totalValue) * 100 : 0,
      isManualPrice: false,
      manualPriceNok: null,
    };
  });

  res.json([...marketResults, ...privateResults]);
});

router.get("/portfolio/summary", async (req, res): Promise<void> => {
  const holdings = await computeHoldings();

  if (holdings.length === 0) {
    res.json({
      totalValueNok: 0, ytdGainLossNok: 0, ytdGainLossPct: 0,
      totalCostNok: 0, allTimeGainLossNok: 0, allTimeGainLossPct: 0,
      stocksValueNok: 0, stocksCostNok: 0, stocksGainLossNok: 0, stocksGainLossPct: 0,
      stocksYtdGainLossNok: 0, stocksYtdGainLossPct: 0,
      cryptoValueNok: 0, cryptoCostNok: 0, cryptoGainLossNok: 0, cryptoGainLossPct: 0,
      cryptoYtdGainLossNok: 0, cryptoYtdGainLossPct: 0,
      privateValueNok: 0, cashValueNok: 0,
      topWinners: [], topLaggers: [], topYtdWinners: [], topYtdLaggers: [],
      portfolioPE: null, portfolioPB: null, portfolioROCE: null, portfolioFCFYield: null,
    });
    return;
  }

  // Load all price sources at once, plus portfolio config (realized gains, private tickers, metric overrides)
  const thisYear = new Date().getFullYear();
  const [overrideRows, cacheRows, ytdSnapshotRows, configRows] = await Promise.all([
    db.select().from(priceOverridesTable),
    db.select().from(priceCacheTable),
    db.select().from(quarterlySnapshotsTable).where(eq(quarterlySnapshotsTable.quarter, `${thisYear}-Q1`)),
    db.select().from(portfolioConfigTable),
  ]);
  const overrideMap  = new Map(overrideRows.map((r) => [r.ticker, Number(r.priceNok)]));
  const cacheMap     = new Map(cacheRows.map((r) => [r.ticker, Number(r.priceNok)]));
  const ytdStartMap  = new Map(ytdSnapshotRows.map((r) => [r.ticker, Number(r.priceNok)]));
  const configMap    = new Map(configRows.map((r) => [r.key, r.value]));
  const realizedGainNok = Number(configMap.get("realized_gain_nok") ?? 0);

  // Tickers explicitly flagged as "unlisted/private" by the user → valued at cost, 0 gain/loss
  const privateTickers = new Set(
    (configMap.get("private_tickers") ?? "")
      .split(",").map((t: string) => t.trim()).filter(Boolean)
  );

  const NON_MARKET_SUM = ["private", "cash"];
  const isPrivate = (h: HoldingCalc) => NON_MARKET_SUM.includes(h.assetType) || privateTickers.has(h.ticker);
  const marketHoldings  = holdings.filter((h) => !isPrivate(h));
  const privateHoldings = holdings.filter((h) =>  isPrivate(h));

  // First-time setup: fetch live for any holding with no cached price
  const missingTickers = marketHoldings.filter((h) => !overrideMap.has(h.ticker) && !cacheMap.has(h.ticker));
  if (missingTickers.length > 0) {
    await Promise.all(missingTickers.map(async (h) => {
      try {
        const { price: priceNok } = await getQuoteNok(h.ticker);
        if (priceNok > 0) {
          const capturedAt = new Date().toISOString().slice(0, 10);
          await db.insert(priceCacheTable)
            .values({ ticker: h.ticker, priceNok: String(priceNok), capturedAt, source: "live" })
            .onConflictDoUpdate({ target: priceCacheTable.ticker, set: { priceNok: String(priceNok), capturedAt, source: "live" } });
          cacheMap.set(h.ticker, priceNok);
        }
      } catch { /* leave price as 0 */ }
    }));
  }

  // Price resolution: override > cache > avgCost (fallback prevents false -100% losses
  // for private/unlisted assets with no Yahoo Finance data and no manual override)
  const getPrice = (ticker: string, avgCostPerUnit: number): number =>
    overrideMap.get(ticker) ?? cacheMap.get(ticker) ?? avgCostPerUnit;

  let totalValueNok = 0, totalCostNok = 0;
  let ytdGainLossNok = 0;
  let stocksValueNok = 0, stocksCostNok = 0;
  let cryptoValueNok = 0, cryptoCostNok = 0;
  let privateValueNok = 0, cashValueNok = 0;
  let stocksYtdGainLossNok = 0;
  let cryptoYtdGainLossNok = 0;

  const marketSummaries = marketHoldings.map((h) => {
    const isManualPrice  = overrideMap.has(h.ticker);
    const avgCostPerUnit = h.quantity > 0 ? h.totalCost / h.quantity : 0;
    const currentPrice   = getPrice(h.ticker, avgCostPerUnit);
    const currentValue   = currentPrice * h.quantity;
    const costBasis      = h.totalCost;
    const gainLossNok    = currentValue - costBasis;
    const gainLossPct    = costBasis > 0 ? (gainLossNok / costBasis) * 100 : 0;

    // YTD: if position first bought AFTER Jan 2, use GAV as start (return from purchase).
    // If held before year start, use the Jan 2 Q1 snapshot price.
    const jan2S = `${new Date().getFullYear()}-01-02`;
    const boughtAfterJan2S = h.firstBuyDate > jan2S;
    let ytdStart: number | null;
    if (isManualPrice) {
      ytdStart = currentPrice;
    } else if (boughtAfterJan2S) {
      ytdStart = avgCostPerUnit;
    } else {
      ytdStart = ytdStartMap.get(h.ticker) ?? null;
    }
    const ytdGain  = ytdStart !== null ? (currentPrice - ytdStart) * h.quantity : null;
    // YTD % = GAV-based: ytdGainNok / costBasis — your return relative to what you paid
    const ytdGainLossPct = ytdGain !== null && costBasis > 0
      ? (ytdGain / costBasis) * 100
      : null;

    totalValueNok += currentValue;
    totalCostNok  += costBasis;

    if (h.assetType === "crypto") { cryptoValueNok += currentValue; cryptoCostNok += costBasis; }
    else                          { stocksValueNok += currentValue; stocksCostNok += costBasis; }

    if (ytdGain !== null) {
      ytdGainLossNok += ytdGain;
      if (h.assetType === "crypto") cryptoYtdGainLossNok += ytdGain;
      else                          stocksYtdGainLossNok  += ytdGain;
    }

    return {
      ticker: h.ticker,
      name: h.name ?? null,
      assetType: h.assetType,
      gainLossNok,
      gainLossPct,
      ytdGainLossNok: ytdGain,
      ytdGainLossPct,
      currentValueNok: currentValue,
      currentPrice,
      costBasis,
    };
  });

  const privateSummaries = privateHoldings.map((h) => {
    totalValueNok += h.totalCost;
    totalCostNok  += h.totalCost;
    if (h.assetType === "cash") cashValueNok += h.totalCost;
    else                        privateValueNok += h.totalCost;
    return {
      ticker: h.ticker,
      name: h.name ?? h.ticker,
      assetType: h.assetType,
      gainLossNok: 0,
      gainLossPct: 0,
      currentValueNok: h.totalCost,
      currentPrice: h.totalCost / (h.quantity || 1),
    };
  });

  const holdingSummaries = [...marketSummaries, ...privateSummaries];

  // YTD %: GAV-based — ytdGainNok / costBasis (what you paid for those assets)
  const ytdGainLossPct       = totalCostNok > 0  ? (ytdGainLossNok / totalCostNok) * 100        : 0;
  const stocksYtdGainLossPct = stocksCostNok > 0 ? (stocksYtdGainLossNok / stocksCostNok) * 100 : 0;
  const cryptoYtdGainLossPct = cryptoCostNok > 0 ? (cryptoYtdGainLossNok / cryptoCostNok) * 100 : 0;
  // All-time: unrealized P&L + historical realized gains entered by the user
  const allTimeGainLossNok = (totalValueNok - totalCostNok) + realizedGainNok;
  const allTimeGainLossPct = totalCostNok > 0 ? (allTimeGainLossNok / totalCostNok) * 100 : 0;

  // Top 5 winners/laggers by all-time gain% (from actual holdings with stable cached prices)
  const sorted = [...holdingSummaries].sort((a, b) => b.gainLossPct - a.gainLossPct);
  const topWinners = sorted.slice(0, 5).filter((h) => h.gainLossPct > 0);
  const topLaggers = sorted.slice(-5).reverse().filter((h) => h.gainLossPct < 0);

  // Top 5 YTD winners/laggers — sorted by YTD % (price-based, matches broker view)
  const ytdSorted = [...marketSummaries]
    .filter((h) => h.ytdGainLossPct !== null)
    .sort((a, b) => (b.ytdGainLossPct ?? 0) - (a.ytdGainLossPct ?? 0));
  const topYtdWinners = ytdSorted.slice(0, 5).filter((h) => (h.ytdGainLossPct ?? 0) > 0);
  const topYtdLaggers = ytdSorted.slice(-5).reverse().filter((h) => (h.ytdGainLossPct ?? 0) < 0);

  // Weighted portfolio metrics — listed stocks only (use cached price for market cap weight)
  const stockHoldings = marketHoldings.filter((h) => h.assetType === "stock");
  let portfolioPE: number | null = null;
  let portfolioPB: number | null = null;
  let portfolioROCE: number | null = null;
  let portfolioFCFYield: number | null = null;

  if (stockHoldings.length > 0) {
    const stockMetrics = await Promise.all(
      stockHoldings.map(async (h) => {
        const avgCostPerUnit = h.quantity > 0 ? h.totalCost / h.quantity : 0;
        const value   = getPrice(h.ticker, avgCostPerUnit) * h.quantity;
        const metrics = await getStockMetrics(h.ticker).catch(() => ({ pe: null, pb: null, roce: null, fcfYield: null }));
        return { value, metrics };
      }),
    );

    const totalStockValue = stockMetrics.reduce((s, m) => s + m.value, 0);
    let peWeighted = 0, peWeight = 0, pbWeighted = 0, pbWeight = 0;
    let roceWeighted = 0, roceWeight = 0, fcfWeighted = 0, fcfWeight = 0;

    for (const { value, metrics } of stockMetrics) {
      const w = totalStockValue > 0 ? value / totalStockValue : 0;
      if (metrics.pe   !== null && metrics.pe > 0 && metrics.pe < 500) { peWeighted   += metrics.pe   * w; peWeight   += w; }
      if (metrics.pb   !== null && metrics.pb > 0)                      { pbWeighted   += metrics.pb   * w; pbWeight   += w; }
      if (metrics.roce !== null)                                         { roceWeighted += metrics.roce * w; roceWeight += w; }
      if (metrics.fcfYield !== null)                                     { fcfWeighted  += metrics.fcfYield * w; fcfWeight += w; }
    }

    portfolioPE       = peWeight   > 0 ? peWeighted   / peWeight   : null;
    portfolioPB       = pbWeight   > 0 ? pbWeighted   / pbWeight   : null;
    portfolioROCE     = roceWeight > 0 ? roceWeighted / roceWeight : null;
    portfolioFCFYield = fcfWeight  > 0 ? fcfWeighted  / fcfWeight  : null;
  }

  // Manual overrides from portfolio config take precedence over computed values
  const toOverride = (key: string) => { const v = Number(configMap.get(key)); return isFinite(v) && v !== 0 ? v : null; };
  if (configMap.has("override_pe"))       { const v = toOverride("override_pe");   if (v !== null) portfolioPE       = v; }
  if (configMap.has("override_pb"))       { const v = toOverride("override_pb");   if (v !== null) portfolioPB       = v; }
  if (configMap.has("override_roce"))     { const v = toOverride("override_roce"); if (v !== null) portfolioROCE     = v; }
  if (configMap.has("override_fcf"))      { const v = toOverride("override_fcf");  if (v !== null) portfolioFCFYield = v; }

  const stocksGainLossNok = stocksValueNok - stocksCostNok;
  const stocksGainLossPct = stocksCostNok > 0 ? (stocksGainLossNok / stocksCostNok) * 100 : 0;
  const cryptoGainLossNok = cryptoValueNok - cryptoCostNok;
  const cryptoGainLossPct = cryptoCostNok > 0 ? (cryptoGainLossNok / cryptoCostNok) * 100 : 0;

  res.json({
    totalValueNok, ytdGainLossNok, ytdGainLossPct,
    totalCostNok, allTimeGainLossNok, allTimeGainLossPct, realizedGainNok,
    stocksValueNok, stocksCostNok, stocksGainLossNok, stocksGainLossPct,
    stocksYtdGainLossNok, stocksYtdGainLossPct,
    cryptoValueNok, cryptoCostNok, cryptoGainLossNok, cryptoGainLossPct,
    cryptoYtdGainLossNok, cryptoYtdGainLossPct,
    privateValueNok, cashValueNok,
    topWinners, topLaggers, topYtdWinners, topYtdLaggers,
    portfolioPE, portfolioPB, portfolioROCE, portfolioFCFYield,
  });
});

// ---------------------------------------------------------------------------
// Portfolio Config (realized gains etc.)
// ---------------------------------------------------------------------------

router.get("/portfolio/config", async (_req, res): Promise<void> => {
  const rows = await db.select().from(portfolioConfigTable);
  // Return numeric keys as numbers, string keys (like private_tickers) as strings
  const STRING_KEYS = new Set(["private_tickers", "quarterly_data"]);
  const config = Object.fromEntries(
    rows.map((r) => [r.key, STRING_KEYS.has(r.key) ? r.value : Number(r.value)])
  );
  res.json(config);
});

router.post("/portfolio/config", async (req, res): Promise<void> => {
  const { key, value } = req.body as { key: string; value: number | string };
  if (!key || value === undefined || value === null) {
    res.status(400).json({ error: "key and value required" });
    return;
  }
  await db.insert(portfolioConfigTable)
    .values({ key, value: String(value) })
    .onConflictDoUpdate({ target: portfolioConfigTable.key, set: { value: String(value) } });
  res.json({ ok: true, key, value });
});

router.delete("/portfolio/config/:key", async (req, res): Promise<void> => {
  const { key } = req.params;
  await db.delete(portfolioConfigTable).where(eq(portfolioConfigTable.key, key));
  res.json({ ok: true, key });
});

// Toggle a ticker in/out of the private_tickers list
router.post("/portfolio/config/private-tickers/toggle", async (req, res): Promise<void> => {
  const { ticker } = req.body as { ticker: string };
  if (!ticker) { res.status(400).json({ error: "ticker required" }); return; }
  const [row] = await db.select().from(portfolioConfigTable).where(eq(portfolioConfigTable.key, "private_tickers"));
  const current = (row?.value ?? "").split(",").map((t: string) => t.trim()).filter(Boolean);
  const idx = current.indexOf(ticker);
  let updated: string[];
  if (idx >= 0) {
    updated = current.filter((_: string, i: number) => i !== idx);
  } else {
    updated = [...current, ticker];
  }
  const newValue = updated.join(",");
  await db.insert(portfolioConfigTable)
    .values({ key: "private_tickers", value: newValue })
    .onConflictDoUpdate({ target: portfolioConfigTable.key, set: { value: newValue } });
  res.json({ ok: true, privateTickers: updated, isPrivate: idx < 0 });
});

// ---------------------------------------------------------------------------
// Performance: quarterly, annual, YTD with/without crypto
// ---------------------------------------------------------------------------

interface PeriodBoundary {
  label: string;
  year: number;
  quarter: number | null;
  start: Date;
  end: Date; // inclusive end date
}

function buildPeriodBoundaries(): PeriodBoundary[] {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const boundaries: PeriodBoundary[] = [];

  // Annual periods since 2024
  for (let year = 2024; year <= today.getFullYear(); year++) {
    const start = new Date(year, 0, 1); // Jan 1
    if (start > today) break;
    const end = year < today.getFullYear() ? new Date(year, 11, 31) : today;
    boundaries.push({ label: year === today.getFullYear() ? `${year} YTD` : String(year), year, quarter: null, start, end });
  }

  // Quarterly periods since Q1 2024
  for (let year = 2024; year <= today.getFullYear(); year++) {
    for (let q = 1; q <= 4; q++) {
      const startMonth = (q - 1) * 3;
      const start = new Date(year, startMonth, 1);
      if (start > today) break;
      const endMonth = q * 3;
      const end = new Date(year, endMonth, 0); // last day of quarter
      const actualEnd = end > today ? today : end;
      boundaries.push({ label: `Q${q} ${year}`, year, quarter: q, start, end: actualEnd });
    }
  }

  return boundaries;
}

interface PortfolioSnapshot {
  holdings: HoldingCalc[];
  valueNok: number;
  stocksValueNok: number;
  cryptoValueNok: number;
}

function getValueFromHistory(
  holdings: HoldingCalc[],
  tickerHistories: Map<string, TickerHistory>,
  date: Date,
  filterAssetType?: string,
  overrideMap?: Map<string, number>,
): number {
  let total = 0;
  for (const h of holdings) {
    if (filterAssetType && h.assetType !== filterAssetType) continue;
    const history = tickerHistories.get(h.ticker);
    const price = history ? getPriceForDate(history, date) : null;
    if (price !== null) {
      total += price * h.quantity;
    } else if (overrideMap?.has(h.ticker)) {
      // Fallback for holdings with no Yahoo Finance history (e.g. manual-price funds)
      total += overrideMap.get(h.ticker)! * h.quantity;
    }
  }
  return total;
}

// Benchmark tickers: S&P 500 and MSCI World (Nordnet Global proxy)
const BENCHMARK_SP500 = "^GSPC";   // USD → NOK
const BENCHMARK_MSCI_WORLD = "URTH"; // iShares MSCI World ETF, USD → NOK

function computeBenchmarkPct(history: TickerHistory, start: Date, end: Date): number | null {
  const startPrice = getPriceForDate(history, start);
  const endPrice = getPriceForDate(history, end);
  if (!startPrice || !endPrice || startPrice <= 0) return null;
  return ((endPrice - startPrice) / startPrice) * 100;
}

router.get("/portfolio/performance", async (req, res): Promise<void> => {
  const allTransactions = await db.select().from(transactionsTable).orderBy(transactionsTable.date);

  // Fetch FX history first (needed for benchmark conversion too)
  const fxHistory = await fetchFXHistories("2024-01-01");

  // Fetch benchmark histories in parallel with portfolio ticker histories
  const tickers = [...new Set(allTransactions.map((t) => t.ticker))];

  const [, sp500History, nordnetHistory] = await Promise.all([
    Promise.all(
      tickers.map(async (ticker) => {
        const h = await fetchTickerHistoryNok(ticker, "2024-01-01", fxHistory);
        return { ticker, h };
      }),
    ).then((results) => {
      const tickerHistoriesLocal = new Map<string, TickerHistory>();
      results.forEach(({ ticker, h }) => tickerHistoriesLocal.set(ticker, h));
      return tickerHistoriesLocal;
    }),
    fetchTickerHistoryNok(BENCHMARK_SP500, "2024-01-01", fxHistory),
    fetchTickerHistoryNok(BENCHMARK_MSCI_WORLD, "2024-01-01", fxHistory),
  ]);

  if (allTransactions.length === 0) {
    // Still return benchmark data even with no portfolio transactions
    const now = new Date();
    const currentYear = now.getFullYear();
    const ytdStart = new Date(currentYear, 0, 1);
    const emptyYtd = {
      label: "YTD",
      year: currentYear,
      quarter: null as number | null,
      startValueNok: 0,
      endValueNok: 0,
      gainLossNok: 0,
      gainLossPct: 0,
      netCapitalFlowNok: 0,
      sp500Pct: computeBenchmarkPct(sp500History, ytdStart, now),
      nordnetGlobalPct: computeBenchmarkPct(nordnetHistory, ytdStart, now),
    };
    res.json({
      ytdTotal: emptyYtd,
      ytdStocksOnly: { ...emptyYtd, sp500Pct: null, nordnetGlobalPct: null },
      ytdCryptoOnly: { ...emptyYtd, sp500Pct: null, nordnetGlobalPct: null },
      quarterly: [],
      annual: [],
    });
    return;
  }

  // Re-build ticker histories map
  const tickerHistories = new Map<string, TickerHistory>();
  await Promise.all(
    tickers.map(async (ticker) => {
      const history = await fetchTickerHistoryNok(ticker, "2024-01-01", fxHistory);
      tickerHistories.set(ticker, history);
    }),
  );

  // Load price overrides so manual-price holdings are included in value calculation
  const priceOverrideRows = await db.select().from(priceOverridesTable);
  const priceOverrideMap = new Map(priceOverrideRows.map((r) => [r.ticker, Number(r.priceNok)]));

  const periods = buildPeriodBoundaries();

  function computePerformancePeriod(
    period: PeriodBoundary,
    filterAssetType?: string,
    includeBenchmarks = true,
  ) {
    const startDateStr = period.start.toISOString().split("T")[0];
    const txsAtStart = allTransactions.filter((t) => t.date < startDateStr) as Transaction[];
    const holdingsAtStart = computeHoldingsFromTx(txsAtStart).filter(
      (h) => !filterAssetType || h.assetType === filterAssetType,
    );

    const endDateStr = period.end.toISOString().split("T")[0];
    const txsAtEnd = allTransactions.filter((t) => t.date <= endDateStr) as Transaction[];
    const holdingsAtEnd = computeHoldingsFromTx(txsAtEnd).filter(
      (h) => !filterAssetType || h.assetType === filterAssetType,
    );

    const startValueNok = getValueFromHistory(holdingsAtStart, tickerHistories, period.start, filterAssetType, priceOverrideMap);
    const endValueNok = getValueFromHistory(holdingsAtEnd, tickerHistories, period.end, filterAssetType, priceOverrideMap);

    const txsInPeriod = allTransactions.filter(
      (t) => t.date >= startDateStr && t.date <= endDateStr && (!filterAssetType || t.assetType === filterAssetType),
    );
    let netCapitalFlowNok = 0;
    for (const tx of txsInPeriod) {
      const qty = Number(tx.quantity);
      const price = Number(tx.priceNok);
      if (tx.action === "buy") netCapitalFlowNok += qty * price;
      else if (tx.action === "sell") netCapitalFlowNok -= qty * price;
    }

    const denominator = startValueNok + netCapitalFlowNok / 2;
    const gainLossNok = endValueNok - startValueNok - netCapitalFlowNok;
    const gainLossPct = denominator > 0 ? (gainLossNok / denominator) * 100 : 0;

    return {
      label: period.label,
      year: period.year,
      quarter: period.quarter ?? null,
      startValueNok,
      endValueNok,
      gainLossNok,
      gainLossPct,
      netCapitalFlowNok,
      sp500Pct: includeBenchmarks ? computeBenchmarkPct(sp500History, period.start, period.end) : null,
      nordnetGlobalPct: includeBenchmarks ? computeBenchmarkPct(nordnetHistory, period.start, period.end) : null,
    };
  }

  const quarterlyPeriods = periods.filter((p) => p.quarter !== null);
  const annualPeriods = periods.filter((p) => p.quarter === null);

  const now = new Date();
  const currentYear = now.getFullYear();
  const ytdPeriod: PeriodBoundary = {
    label: "YTD",
    year: currentYear,
    quarter: null,
    start: new Date(currentYear, 0, 1),
    end: now,
  };

  // Load manual overrides and apply them
  const manualEntries = await db.select().from(manualPerformanceTable);
  const manualMap = new Map(manualEntries.map((e) => [e.label, e]));

  function applyManual(period: ReturnType<typeof computePerformancePeriod>) {
    const m = manualMap.get(period.label);
    if (!m) return { ...period, isManual: false };
    return {
      ...period,
      isManual: true,
      gainLossPct:      m.portfolioPct     != null ? Number(m.portfolioPct)     : period.gainLossPct,
      sp500Pct:         m.sp500Pct         != null ? Number(m.sp500Pct)         : period.sp500Pct,
      nordnetGlobalPct: m.nordnetGlobalPct != null ? Number(m.nordnetGlobalPct) : period.nordnetGlobalPct,
    };
  }

  res.json({
    ytdTotal: computePerformancePeriod(ytdPeriod, undefined, true),
    ytdStocksOnly: computePerformancePeriod(ytdPeriod, "stock", false),
    ytdCryptoOnly: computePerformancePeriod(ytdPeriod, "crypto", false),
    quarterly: quarterlyPeriods.map((p) => applyManual(computePerformancePeriod(p, undefined, true))),
    annual: annualPeriods.map((p) => applyManual(computePerformancePeriod(p, undefined, true))),
  });
});

// ─── Manual performance entries: CRUD ────────────────────────────────────────
router.get("/performance/manual", async (_req, res): Promise<void> => {
  const rows = await db.select().from(manualPerformanceTable).orderBy(manualPerformanceTable.label);
  res.json(rows);
});

router.post("/performance/manual", async (req, res): Promise<void> => {
  const { label, portfolioPct, sp500Pct, nordnetGlobalPct } = req.body as {
    label: string;
    portfolioPct?: number | null;
    sp500Pct?: number | null;
    nordnetGlobalPct?: number | null;
  };
  if (!label) { res.status(400).json({ error: "label is required" }); return; }
  const existing = await db.select().from(manualPerformanceTable).where(eq(manualPerformanceTable.label, label));
  if (existing.length > 0) {
    const updated = await db
      .update(manualPerformanceTable)
      .set({
        portfolioPct:     portfolioPct     != null ? String(portfolioPct)     : null,
        sp500Pct:         sp500Pct         != null ? String(sp500Pct)         : null,
        nordnetGlobalPct: nordnetGlobalPct != null ? String(nordnetGlobalPct) : null,
        updatedAt: new Date(),
      })
      .where(eq(manualPerformanceTable.label, label))
      .returning();
    res.json(updated[0]);
  } else {
    const inserted = await db
      .insert(manualPerformanceTable)
      .values({
        label,
        portfolioPct:     portfolioPct     != null ? String(portfolioPct)     : null,
        sp500Pct:         sp500Pct         != null ? String(sp500Pct)         : null,
        nordnetGlobalPct: nordnetGlobalPct != null ? String(nordnetGlobalPct) : null,
      })
      .returning();
    res.json(inserted[0]);
  }
});

router.delete("/performance/manual/:label", async (req, res): Promise<void> => {
  const label = decodeURIComponent(req.params.label);
  await db.delete(manualPerformanceTable).where(eq(manualPerformanceTable.label, label));
  res.json({ ok: true });
});

// ─── Price overrides ──────────────────────────────────────────────────────────
router.get("/portfolio/price-overrides", async (req, res): Promise<void> => {
  const rows = await db.select().from(priceOverridesTable);
  res.json(rows);
});

router.put("/portfolio/price-overrides/:ticker", async (req, res): Promise<void> => {
  const ticker = decodeURIComponent(req.params.ticker);
  const { priceNok } = req.body as { priceNok: number };
  if (typeof priceNok !== "number" || priceNok <= 0) {
    res.status(400).json({ error: "priceNok must be a positive number" });
    return;
  }
  const rows = await db
    .insert(priceOverridesTable)
    .values({ ticker, priceNok: String(priceNok), updatedAt: new Date() })
    .onConflictDoUpdate({
      target: priceOverridesTable.ticker,
      set: { priceNok: String(priceNok), updatedAt: new Date() },
    })
    .returning();
  res.json(rows[0]);
});

router.delete("/portfolio/price-overrides/:ticker", async (req, res): Promise<void> => {
  const ticker = decodeURIComponent(req.params.ticker);
  await db.delete(priceOverridesTable).where(eq(priceOverridesTable.ticker, ticker));
  res.json({ ok: true });
});

// ─── Manual trigger: capture quarter prices now ───────────────────────────────
router.post("/portfolio/capture-quarter-prices", async (req, res): Promise<void> => {
  const result = await captureQuarterPrices();
  res.json(result);
});

// ─── Manual trigger: refresh all current prices now (updates price cache) ────
router.post("/portfolio/prices/refresh", async (req, res): Promise<void> => {
  const result = await refreshAllPricesNow();
  res.json(result);
});

// ─── List saved quarterly snapshots ──────────────────────────────────────────
router.get("/portfolio/quarter-snapshots", async (req, res): Promise<void> => {
  const rows = await db
    .select()
    .from(quarterlySnapshotsTable)
    .orderBy(quarterlySnapshotsTable.quarter, quarterlySnapshotsTable.ticker);
  res.json(rows);
});

export default router;

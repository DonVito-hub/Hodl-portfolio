import YahooFinance from "yahoo-finance2";
import { logger } from "./logger";

const yahooFinance = new YahooFinance();

/** Maps user-friendly ticker names → Yahoo Finance symbol */
const TICKER_ALIASES: Record<string, string> = {
  // Oslo Børs
  "NORBT":    "NORBT.OL",
  "NRBT":     "NORBT.OL",   // common shorthand variant
  "PROT":     "PROT.OL",
  // Copenhagen (DKK)
  "NOVO B":   "NOVO-B.CO",
  "NOVO-B":   "NOVO-B.CO",
  "NOVOB":    "NOVO-B.CO",
  "NOVO":     "NOVO-B.CO",
  // Stockholm (SEK)
  "INVE A":   "INVE-A.ST",
  "INVE.A":   "INVE-A.ST",
  "INVE-A":   "INVE-A.ST",
  "INVEA":    "INVE-A.ST",
  // Toronto (CAD)
  "CSU":      "CSU.TO",
  "GLXY":     "GLXY.TO",
  // NYSE / US
  "BRK.B":    "BRK-B",
  "BRK/B":    "BRK-B",
  "NVIDIA":   "NVDA",
  "AMAZON":   "AMZN",
  "GOOGLE":   "GOOGL",
  "ALPHABET": "GOOGL",
  "META":     "META",
  "MICROSOFT":"MSFT",
  "APPLE":    "AAPL",
};

/** Normalise a ticker string to its Yahoo Finance symbol. */
export function normalizeTicker(raw: string): string {
  const upper = raw.trim().toUpperCase();
  return TICKER_ALIASES[upper] ?? raw.trim();
}

interface QuoteResult {
  ticker: string;
  price: number; // in NOK
  currency: string;
  name?: string;
}

// ── FX rate cache ─────────────────────────────────────────────────────────────

type FxCache = { rate: number | null; fetchedAt: number };

const FX_TTL = 5 * 60 * 1000; // 5 minutes

const fxCache: Record<string, FxCache> = {};

const FX_FALLBACKS: Record<string, number> = {
  "USDNOK=X": 10.5,
  "EURNOK=X": 11.5,
  "GBPNOK=X": 13.5,
  "DKKNOK=X": 1.54,
  "SEKNOK=X": 0.95,
  "CADNOK=X": 7.8,
};

async function getFxRate(pair: string): Promise<number> {
  const now = Date.now();
  const cached = fxCache[pair];
  if (cached?.rate && now - cached.fetchedAt < FX_TTL) return cached.rate;
  try {
    const quote = await yahooFinance.quote(pair);
    const rate = quote.regularMarketPrice ?? FX_FALLBACKS[pair] ?? 1;
    fxCache[pair] = { rate, fetchedAt: now };
    return rate;
  } catch {
    logger.warn(`Failed to fetch FX rate for ${pair}, using fallback`);
    return fxCache[pair]?.rate ?? FX_FALLBACKS[pair] ?? 1;
  }
}

async function getUsdNokRate() { return getFxRate("USDNOK=X"); }
async function getEurNokRate() { return getFxRate("EURNOK=X"); }
async function getDkkNokRate() { return getFxRate("DKKNOK=X"); }
async function getSekNokRate() { return getFxRate("SEKNOK=X"); }
async function getCadNokRate() { return getFxRate("CADNOK=X"); }

/**
 * Convert a price in the given currency to NOK.
 * Handles: NOK, USD, EUR, GBP, GBp (pence), DKK, SEK, CAD.
 */
async function toNok(price: number, currency: string): Promise<number> {
  // Handle Yahoo Finance's "GBp" (pence) before uppercasing
  if (currency === "GBp" || currency === "GBX") {
    return (price / 100) * (await getFxRate("GBPNOK=X"));
  }

  const cur = (currency ?? "USD").toUpperCase();

  switch (cur) {
    case "NOK": return price;
    case "USD": return price * (await getUsdNokRate());
    case "EUR": return price * (await getEurNokRate());
    case "GBP": return price * (await getFxRate("GBPNOK=X"));
    case "DKK": return price * (await getDkkNokRate());
    case "SEK": return price * (await getSekNokRate());
    case "CAD": return price * (await getCadNokRate());
    default:
      logger.warn(`Unknown currency "${currency}", treating as USD`);
      return price * (await getUsdNokRate());
  }
}

// ── Current quote ─────────────────────────────────────────────────────────────

export async function getQuoteNok(ticker: string): Promise<QuoteResult> {
  const sym = normalizeTicker(ticker);
  const quote = await yahooFinance.quote(sym);
  const price = quote.regularMarketPrice ?? 0;
  const currency = quote.currency ?? "USD";
  const priceNok = await toNok(price, currency);
  return {
    ticker,
    price: priceNok,
    currency,
    name: quote.longName ?? quote.shortName ?? undefined,
  };
}

// ── YTD start price ───────────────────────────────────────────────────────────

export async function getYtdStartPriceNok(ticker: string): Promise<number | null> {
  const sym = normalizeTicker(ticker);
  try {
    const startOfYear = new Date(new Date().getFullYear(), 0, 2); // Jan 2
    const endOfYear   = new Date(new Date().getFullYear(), 0, 5); // Jan 5
    const history = await yahooFinance.historical(sym, {
      period1:  startOfYear.toISOString().split("T")[0],
      period2:  endOfYear.toISOString().split("T")[0],
      interval: "1d",
    });
    if (!history || history.length === 0) return null;
    const firstClose = history[0].close;
    const quote = await yahooFinance.quote(sym);
    const currency = quote.currency ?? "USD";
    return await toNok(firstClose, currency);
  } catch (err) {
    logger.warn({ ticker: sym, err }, "Failed to get YTD start price");
    return null;
  }
}

// ── Stock metrics ─────────────────────────────────────────────────────────────

interface StockMetrics {
  pe: number | null;
  pb: number | null;
  roce: number | null;
  fcfYield: number | null;
}

export async function getStockMetrics(ticker: string): Promise<StockMetrics> {
  const sym = normalizeTicker(ticker);
  try {
    const summary = await yahooFinance.quoteSummary(sym, {
      modules: ["defaultKeyStatistics", "financialData", "summaryDetail"],
    });

    const ke = summary.defaultKeyStatistics;
    const fd = summary.financialData;
    const sd = summary.summaryDetail;

    const trailingPE = sd?.trailingPE ?? null;
    const pb = ke?.priceToBook ?? null;
    const roce = fd?.returnOnEquity ? fd.returnOnEquity * 100 : null;
    const fcf = fd?.freeCashflow ?? null;
    const marketCap = fd?.marketCap ?? null;
    const fcfYield = fcf && marketCap && marketCap > 0 ? (fcf / marketCap) * 100 : null;

    return { pe: trailingPE, pb: pb ?? null, roce, fcfYield };
  } catch (err) {
    logger.warn({ ticker: sym, err }, "Failed to get stock metrics");
    return { pe: null, pb: null, roce: null, fcfYield: null };
  }
}

// ── Historical price helpers ──────────────────────────────────────────────────

export type TickerHistory = Map<string, number>; // dateStr "YYYY-MM-DD" -> price in NOK

const historicalCache = new Map<string, { data: TickerHistory; fetchedAt: number }>();
const HISTORICAL_CACHE_TTL = 60 * 60 * 1000; // 1 hour

/**
 * Detect the currency for a (already-normalized) Yahoo Finance ticker.
 */
function detectCurrencyFromTicker(sym: string): string {
  if (sym.endsWith(".OL"))  return "NOK";
  if (sym.endsWith(".CO"))  return "DKK";
  if (sym.endsWith(".ST"))  return "SEK";
  if (sym.endsWith(".TO"))  return "CAD";
  if (sym.endsWith(".L"))   return "GBP";
  if (sym.endsWith("-USD") || sym.endsWith("=X")) return "USD";
  if (sym.endsWith("-EUR")) return "EUR";
  return "USD"; // default for US stocks
}

/**
 * Fetch historical weekly close prices for a ticker from startDate to today.
 * Returns a map of dateStr -> priceNok.
 */
export async function fetchTickerHistoryNok(
  ticker: string,
  startDate: string, // "YYYY-MM-DD"
  fxHistory: { usdNok: TickerHistory; eurNok: TickerHistory; dkkNok: TickerHistory; sekNok: TickerHistory; cadNok: TickerHistory },
): Promise<TickerHistory> {
  const sym = normalizeTicker(ticker);
  const cacheKey = `${sym}-${startDate}`;
  const cached = historicalCache.get(cacheKey);
  if (cached && Date.now() - cached.fetchedAt < HISTORICAL_CACHE_TTL) {
    return cached.data;
  }

  const currency = detectCurrencyFromTicker(sym);
  const result = new Map<string, number>();

  try {
    const today = new Date();
    const history = await yahooFinance.historical(sym, {
      period1:  startDate,
      period2:  today.toISOString().split("T")[0],
      interval: "1wk",
    });

    for (const h of history) {
      if (!h.close) continue;
      const dateStr = h.date.toISOString().split("T")[0];
      let priceNok = h.close;

      if (currency === "USD") {
        const fxRate = fxHistory.usdNok.get(dateStr) ?? getFxRateForDate(fxHistory.usdNok, h.date) ?? 10.5;
        priceNok = h.close * fxRate;
      } else if (currency === "EUR") {
        const fxRate = fxHistory.eurNok.get(dateStr) ?? getFxRateForDate(fxHistory.eurNok, h.date) ?? 11.5;
        priceNok = h.close * fxRate;
      } else if (currency === "DKK") {
        const fxRate = fxHistory.dkkNok.get(dateStr) ?? getFxRateForDate(fxHistory.dkkNok, h.date) ?? 1.54;
        priceNok = h.close * fxRate;
      } else if (currency === "SEK") {
        const fxRate = fxHistory.sekNok.get(dateStr) ?? getFxRateForDate(fxHistory.sekNok, h.date) ?? 0.95;
        priceNok = h.close * fxRate;
      } else if (currency === "CAD") {
        const fxRate = fxHistory.cadNok.get(dateStr) ?? getFxRateForDate(fxHistory.cadNok, h.date) ?? 7.8;
        priceNok = h.close * fxRate;
      }
      // NOK, GBP, others: use raw or implement further

      result.set(dateStr, priceNok);
    }
  } catch (err) {
    logger.warn({ ticker: sym, err }, "Failed to fetch historical prices");
  }

  historicalCache.set(cacheKey, { data: result, fetchedAt: Date.now() });
  return result;
}

/**
 * Fetch historical weekly FX rates for all supported currency pairs.
 */
export async function fetchFXHistories(startDate: string): Promise<{
  usdNok: TickerHistory;
  eurNok: TickerHistory;
  dkkNok: TickerHistory;
  sekNok: TickerHistory;
  cadNok: TickerHistory;
}> {
  const [usdHistory, eurHistory, dkkHistory, sekHistory, cadHistory] = await Promise.all([
    fetchRawHistory("USDNOK=X", startDate),
    fetchRawHistory("EURNOK=X", startDate),
    fetchRawHistory("DKKNOK=X", startDate),
    fetchRawHistory("SEKNOK=X", startDate),
    fetchRawHistory("CADNOK=X", startDate),
  ]);
  return { usdNok: usdHistory, eurNok: eurHistory, dkkNok: dkkHistory, sekNok: sekHistory, cadNok: cadHistory };
}

async function fetchRawHistory(ticker: string, startDate: string): Promise<TickerHistory> {
  const cacheKey = `raw-${ticker}-${startDate}`;
  const cached = historicalCache.get(cacheKey);
  if (cached && Date.now() - cached.fetchedAt < HISTORICAL_CACHE_TTL) {
    return cached.data;
  }

  const result = new Map<string, number>();
  try {
    const today = new Date();
    const history = await yahooFinance.historical(ticker, {
      period1:  startDate,
      period2:  today.toISOString().split("T")[0],
      interval: "1wk",
    });
    for (const h of history) {
      if (h.close) result.set(h.date.toISOString().split("T")[0], h.close);
    }
  } catch (err) {
    logger.warn({ ticker, err }, "Failed to fetch FX history");
  }

  historicalCache.set(cacheKey, { data: result, fetchedAt: Date.now() });
  return result;
}

/**
 * Look up the closest available price in a history map for a given date.
 * Searches within ±7 days.
 */
export function getFxRateForDate(history: TickerHistory, date: Date): number | null {
  for (let offset = 0; offset <= 7; offset++) {
    for (const sign of [0, -1, 1]) {
      const d = new Date(date);
      d.setDate(d.getDate() + sign * offset);
      const dateStr = d.toISOString().split("T")[0];
      if (history.has(dateStr)) return history.get(dateStr)!;
    }
  }
  return null;
}

/**
 * Look up a price for a given date from a ticker's history map.
 */
export function getPriceForDate(history: TickerHistory, date: Date): number | null {
  return getFxRateForDate(history, date);
}

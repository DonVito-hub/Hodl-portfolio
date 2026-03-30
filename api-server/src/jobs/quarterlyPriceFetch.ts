import cron from "node-cron";
import { db, transactionsTable, quarterlySnapshotsTable, priceCacheTable, priceOverridesTable } from "@workspace/db";
import { getQuoteNok, getYtdStartPriceNok } from "../lib/market";
import { logger } from "../lib/logger";
import { sql, eq, and, inArray } from "drizzle-orm";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function getQuarterLabel(date: Date): string {
  const y = date.getUTCFullYear();
  const q = Math.floor(date.getUTCMonth() / 3) + 1;
  return `${y}-Q${q}`;
}

function getActiveHoldings(): Promise<{ ticker: string; assetType: string }[]> {
  return db
    .execute(
      sql`
        SELECT
          ticker,
          MAX(asset_type)          AS "assetType",
          SUM(
            CASE WHEN action = 'buy' THEN quantity::numeric
                 ELSE -(quantity::numeric)
            END
          ) AS net_qty
        FROM transactions
        GROUP BY ticker
        HAVING SUM(
          CASE WHEN action = 'buy' THEN quantity::numeric
               ELSE -(quantity::numeric)
            END
        ) > 0.00001
      `,
    )
    .then((result) =>
      (result.rows as { ticker: string; assetType: string }[]).map((r) => ({
        ticker: r.ticker,
        assetType: r.assetType,
      })),
    );
}

// ─── Core capture logic ───────────────────────────────────────────────────────

export async function captureQuarterPrices(): Promise<{
  quarter: string;
  captured: number;
  failed: number;
}> {
  const now = new Date();
  const quarter = getQuarterLabel(now);
  const capturedAt = now.toISOString().slice(0, 10); // YYYY-MM-DD

  logger.info({ quarter }, "Quarterly price capture starting");

  const holdings = await getActiveHoldings();
  if (holdings.length === 0) {
    logger.info("No active holdings; skipping quarterly capture");
    return { quarter, captured: 0, failed: 0 };
  }

  let captured = 0;
  let failed = 0;

  for (const h of holdings) {
    try {
      const { price: priceNok } = await getQuoteNok(h.ticker);
      if (priceNok <= 0) {
        logger.warn({ ticker: h.ticker }, "Got zero price from Yahoo Finance; skipping cache update");
        failed++;
        continue;
      }

      // Write to quarterly snapshot history
      await db
        .insert(quarterlySnapshotsTable)
        .values({
          quarter,
          capturedAt,
          ticker: h.ticker,
          assetType: h.assetType,
          priceNok: String(priceNok),
        })
        .onConflictDoUpdate({
          target: [quarterlySnapshotsTable.quarter, quarterlySnapshotsTable.ticker],
          set: { priceNok: String(priceNok), capturedAt },
        });

      // Also update the rolling price cache (used by summary/holdings endpoints)
      await db
        .insert(priceCacheTable)
        .values({
          ticker: h.ticker,
          priceNok: String(priceNok),
          capturedAt,
          source: "quarterly",
        })
        .onConflictDoUpdate({
          target: priceCacheTable.ticker,
          set: { priceNok: String(priceNok), capturedAt, source: "quarterly" },
        });

      logger.info({ quarter, ticker: h.ticker, priceNok }, "Snapshot saved");
      captured++;
    } catch (err) {
      logger.error({ quarter, ticker: h.ticker, err }, "Failed to capture price");
      failed++;
    }
  }

  logger.info({ quarter, captured, failed }, "Quarterly price capture complete");
  return { quarter, captured, failed };
}

// ─── Manual "refresh all prices now" ──────────────────────────────────────────

export async function refreshAllPricesNow(): Promise<{
  refreshed: number;
  failed: number;
  results: { ticker: string; priceNok: number; ok: boolean }[];
}> {
  const now = new Date();
  const capturedAt = now.toISOString().slice(0, 10);

  const [holdings, overrideRows] = await Promise.all([
    getActiveHoldings(),
    db.select({ ticker: priceOverridesTable.ticker }).from(priceOverridesTable),
  ]);
  const manualTickers = new Set(overrideRows.map((r) => r.ticker));

  // Exclude non-market types and manually-priced holdings (overrides take priority anyway)
  const NON_MARKET = ["private", "cash"];
  const market = holdings.filter((h) => !NON_MARKET.includes(h.assetType) && !manualTickers.has(h.ticker));

  let refreshed = 0;
  let failed = 0;
  const results: { ticker: string; priceNok: number; ok: boolean }[] = [];

  for (const h of market) {
    try {
      const { price: priceNok } = await getQuoteNok(h.ticker);
      if (priceNok <= 0) {
        logger.warn({ ticker: h.ticker }, "Got zero price from Yahoo Finance; skipping cache update");
        failed++;
        results.push({ ticker: h.ticker, priceNok: 0, ok: false });
        continue;
      }

      await db
        .insert(priceCacheTable)
        .values({
          ticker: h.ticker,
          priceNok: String(priceNok),
          capturedAt,
          source: "live",
        })
        .onConflictDoUpdate({
          target: priceCacheTable.ticker,
          set: { priceNok: String(priceNok), capturedAt, source: "live" },
        });

      refreshed++;
      results.push({ ticker: h.ticker, priceNok, ok: true });
    } catch (err) {
      logger.error({ ticker: h.ticker, err }, "Failed to refresh price");
      failed++;
      results.push({ ticker: h.ticker, priceNok: 0, ok: false });
    }
  }

  // ── Refresh Q1 YTD snapshots with actual Jan 2 historical prices ─────────
  // Always overwrite so stale snapshots (seeded at wrong prices) are corrected.
  // We skip tickers already marked capturedAt = "${year}-01-02" only if the
  // stored date already indicates a proper historical fetch (prevents redundant
  // calls on subsequent refreshes the same day).
  const thisYear = now.getUTCFullYear();
  const ytdQuarter = `${thisYear}-Q1`;
  const ytdCapturedAt = `${thisYear}-01-02`;
  const marketTickers = market.map((h) => h.ticker);

  if (marketTickers.length > 0) {
    // Load existing Q1 rows to check which ones already have the correct date
    const existingQ1 = await db
      .select({ ticker: quarterlySnapshotsTable.ticker, capturedAt: quarterlySnapshotsTable.capturedAt })
      .from(quarterlySnapshotsTable)
      .where(
        and(
          eq(quarterlySnapshotsTable.quarter, ytdQuarter),
          inArray(quarterlySnapshotsTable.ticker, marketTickers),
        ),
      );
    const alreadyCorrect = new Set(
      existingQ1
        .filter((r) => r.capturedAt === ytdCapturedAt)
        .map((r) => r.ticker)
    );

    // Re-seed all tickers whose Q1 snapshot is missing OR was not set from Jan 2 historical data
    const toReseed = market.filter((h) => !alreadyCorrect.has(h.ticker));

    if (toReseed.length > 0) {
      logger.info({ ytdQuarter, count: toReseed.length }, "Refreshing Q1 YTD snapshots from Jan 2 historical prices");

      await Promise.all(
        toReseed.map(async (h) => {
          try {
            const ytdPrice = await getYtdStartPriceNok(h.ticker);
            if (!ytdPrice || ytdPrice <= 0) {
              logger.warn({ ticker: h.ticker }, "Could not get Jan 2 historical price; skipping Q1 update");
              return;
            }
            await db
              .insert(quarterlySnapshotsTable)
              .values({
                quarter: ytdQuarter,
                capturedAt: ytdCapturedAt,
                ticker: h.ticker,
                assetType: h.assetType,
                priceNok: String(ytdPrice),
              })
              .onConflictDoUpdate({
                target: [quarterlySnapshotsTable.quarter, quarterlySnapshotsTable.ticker],
                set: { priceNok: String(ytdPrice), capturedAt: ytdCapturedAt },
              });
            logger.info({ ticker: h.ticker, ytdQuarter, ytdPrice }, "Q1 YTD snapshot updated with Jan 2 historical price");
          } catch (err) {
            logger.warn({ ticker: h.ticker, err }, "Could not update Q1 YTD snapshot");
          }
        }),
      );
    } else {
      logger.info({ ytdQuarter }, "All Q1 YTD snapshots already set from Jan 2 historical prices");
    }
  }

  logger.info({ refreshed, failed }, "Manual price refresh complete");
  return { refreshed, failed, results };
}

// ─── Scheduler ────────────────────────────────────────────────────────────────

export function startQuarterlyPriceJob(): void {
  // 09:30 AM UTC on Jan 1, Apr 1, Jul 1, Oct 1
  const expression = "30 9 1 1,4,7,10 *";

  cron.schedule(expression, async () => {
    logger.info("Quarterly price cron triggered");
    try {
      await captureQuarterPrices();
    } catch (err) {
      logger.error({ err }, "Quarterly price cron failed");
    }
  });

  logger.info({ expression }, "Quarterly price cron job scheduled");
}

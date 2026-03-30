import { Router, type IRouter } from "express";
import multer from "multer";
import * as XLSX from "xlsx";
import { db } from "@workspace/db";
import { holdingAnalyticsTable } from "@workspace/db/schema";
import { eq } from "drizzle-orm";

const router: IRouter = Router();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

type ChartSeries = {
  metric: string;
  data: Array<{ label: string; value: number | null }>;
};

function parseExcelToCharts(buffer: Buffer): ChartSeries[] {
  const workbook = XLSX.read(buffer, { type: "buffer" });
  const allCharts: ChartSeries[] = [];

  for (const sheetName of workbook.SheetNames) {
    const sheet = workbook.Sheets[sheetName];

    // Use header:1 to get raw 2D array — works for both orientations
    const raw = XLSX.utils.sheet_to_json<(string | number | null)[]>(sheet, {
      header: 1,
      defval: null,
    });

    if (raw.length < 2) continue;

    const headerRow = raw[0];
    if (!headerRow || headerRow.length < 2) continue;

    // The first cell of the header row is either empty or a section title (e.g. "Income Statement").
    // The remaining cells are X-axis labels (e.g. "Dec '24", "Dec '23", ...).
    // Each subsequent row: col[0] = metric name, col[1..] = values per date.
    const xLabels = headerRow.slice(1).map((v) => String(v ?? "").trim());
    if (xLabels.every((l) => l === "")) continue;

    for (let r = 1; r < raw.length; r++) {
      const row = raw[r];
      if (!row) continue;

      const metricName = String(row[0] ?? "").trim();
      if (!metricName) continue;

      const data: ChartSeries["data"] = xLabels.map((label, i) => {
        const cell = row[i + 1];
        const num = cell !== null && cell !== undefined && cell !== "" ? Number(cell) : null;
        return { label, value: num === null || isNaN(num as number) ? null : (num as number) };
      });

      const hasData = data.some((d) => d.value !== null);
      if (!hasData) continue;

      // Reverse so oldest period is leftmost on chart (data often comes newest-first)
      data.reverse();

      const chartKey = sheetName !== "Sheet1" ? `${sheetName} – ${metricName}` : metricName;
      if (!allCharts.find((c) => c.metric === chartKey)) {
        allCharts.push({ metric: chartKey, data });
      }
    }
  }

  return allCharts;
}

router.get("/excel/holdings", async (req, res): Promise<void> => {
  const rows = await db.select({ ticker: holdingAnalyticsTable.ticker }).from(holdingAnalyticsTable);
  res.json(rows.map((r) => r.ticker));
});

router.get("/excel/holding/:ticker", async (req, res): Promise<void> => {
  const ticker = req.params.ticker.toUpperCase();
  const [row] = await db
    .select()
    .from(holdingAnalyticsTable)
    .where(eq(holdingAnalyticsTable.ticker, ticker));

  if (!row) {
    res.json({ ticker, filename: null, uploadedAt: null, charts: [] });
    return;
  }

  res.json({
    ticker: row.ticker,
    filename: row.filename,
    uploadedAt: row.uploadedAt,
    charts: row.charts as ChartSeries[],
  });
});

router.post("/excel/holding/:ticker", upload.single("file"), async (req, res): Promise<void> => {
  const ticker = req.params.ticker.toUpperCase();

  if (!req.file) {
    res.status(400).json({ error: "No file uploaded" });
    return;
  }

  const charts = parseExcelToCharts(req.file.buffer);

  await db
    .insert(holdingAnalyticsTable)
    .values({
      ticker,
      filename: req.file.originalname,
      uploadedAt: new Date(),
      charts,
    })
    .onConflictDoUpdate({
      target: holdingAnalyticsTable.ticker,
      set: {
        filename: req.file.originalname,
        uploadedAt: new Date(),
        charts,
      },
    });

  res.json({ ticker, filename: req.file.originalname, charts });
});

router.patch("/excel/holding/:ticker", async (req, res): Promise<void> => {
  const ticker = req.params.ticker.toUpperCase();
  const { charts } = req.body as { charts: ChartSeries[] };
  if (!Array.isArray(charts)) {
    res.status(400).json({ error: "charts must be an array" });
    return;
  }

  await db
    .insert(holdingAnalyticsTable)
    .values({ ticker, filename: null, uploadedAt: new Date(), charts })
    .onConflictDoUpdate({
      target: holdingAnalyticsTable.ticker,
      set: { charts, uploadedAt: new Date() },
    });

  res.json({ ticker, charts });
});

router.delete("/excel/holding/:ticker", async (req, res): Promise<void> => {
  const ticker = req.params.ticker.toUpperCase();
  await db.delete(holdingAnalyticsTable).where(eq(holdingAnalyticsTable.ticker, ticker));
  res.json({ ok: true });
});

export default router;

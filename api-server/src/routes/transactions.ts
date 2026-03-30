import { Router, type IRouter } from "express";
import { db, transactionsTable } from "@workspace/db";
import { eq } from "drizzle-orm";
import { normalizeTicker } from "../lib/market";
import {
  ListTransactionsResponse,
  CreateTransactionBody,
  UpdateTransactionParams,
  UpdateTransactionBody,
  UpdateTransactionResponse,
  DeleteTransactionParams,
} from "@workspace/api-zod";

const router: IRouter = Router();

router.get("/transactions", async (req, res): Promise<void> => {
  const rows = await db
    .select()
    .from(transactionsTable)
    .orderBy(transactionsTable.date);

  const mapped = rows.map((r) => ({
    id: r.id,
    ticker: r.ticker,
    name: r.name,
    assetType: r.assetType,
    action: r.action,
    quantity: Number(r.quantity),
    priceNok: Number(r.priceNok),
    date: r.date,
    notes: r.notes,
    createdAt: r.createdAt.toISOString(),
  }));

  res.json(ListTransactionsResponse.parse(mapped));
});

router.post("/transactions", async (req, res): Promise<void> => {
  const parsed = CreateTransactionBody.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const d = parsed.data;
  const [row] = await db
    .insert(transactionsTable)
    .values({
      ticker: normalizeTicker(d.ticker),
      name: d.name ?? null,
      assetType: d.assetType,
      action: d.action,
      quantity: String(d.quantity),
      priceNok: String(d.priceNok),
      date: d.date,
      notes: d.notes ?? null,
    })
    .returning();

  res.status(201).json({
    id: row.id,
    ticker: row.ticker,
    name: row.name,
    assetType: row.assetType,
    action: row.action,
    quantity: Number(row.quantity),
    priceNok: Number(row.priceNok),
    date: row.date,
    notes: row.notes,
    createdAt: row.createdAt.toISOString(),
  });
});

router.put("/transactions/:id", async (req, res): Promise<void> => {
  const rawId = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
  const params = UpdateTransactionParams.safeParse({ id: parseInt(rawId, 10) });
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const parsed = UpdateTransactionBody.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const d = parsed.data;
  const [row] = await db
    .update(transactionsTable)
    .set({
      ticker: d.ticker ? normalizeTicker(d.ticker) : undefined,
      name: d.name ?? null,
      assetType: d.assetType,
      action: d.action,
      quantity: d.quantity !== undefined ? String(d.quantity) : undefined,
      priceNok: d.priceNok !== undefined ? String(d.priceNok) : undefined,
      date: d.date,
      notes: d.notes ?? null,
    })
    .where(eq(transactionsTable.id, params.data.id))
    .returning();

  if (!row) {
    res.status(404).json({ error: "Transaction not found" });
    return;
  }

  res.json(
    UpdateTransactionResponse.parse({
      id: row.id,
      ticker: row.ticker,
      name: row.name,
      assetType: row.assetType,
      action: row.action,
      quantity: Number(row.quantity),
      priceNok: Number(row.priceNok),
      date: row.date,
      notes: row.notes,
      createdAt: row.createdAt.toISOString(),
    }),
  );
});

router.delete("/transactions/:id", async (req, res): Promise<void> => {
  const rawId = Array.isArray(req.params.id) ? req.params.id[0] : req.params.id;
  const params = DeleteTransactionParams.safeParse({ id: parseInt(rawId, 10) });
  if (!params.success) {
    res.status(400).json({ error: params.error.message });
    return;
  }

  const [row] = await db
    .delete(transactionsTable)
    .where(eq(transactionsTable.id, params.data.id))
    .returning();

  if (!row) {
    res.status(404).json({ error: "Transaction not found" });
    return;
  }

  res.sendStatus(204);
});

export default router;

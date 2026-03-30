import { Router, type IRouter, type Request, type Response } from "express";
import { GetCurrentAuthUserResponse } from "@workspace/api-zod";
import {
  clearSession,
  createSession,
  getSessionId,
  SESSION_COOKIE,
  SESSION_TTL,
} from "../lib/auth";

const router: IRouter = Router();

router.get("/auth/user", (req: Request, res: Response) => {
  res.json(
    GetCurrentAuthUserResponse.parse({
      user: req.isAuthenticated() ? req.user : null,
    }),
  );
});

router.post("/auth/login", async (req: Request, res: Response) => {
  const { password } = req.body as { password?: string };

  const correct = process.env.DASHBOARD_PASSWORD;
  if (!correct) {
    res.status(500).json({ error: "Server misconfiguration: password not set" });
    return;
  }

  if (!password || password.trim() !== correct.trim()) {
    res.status(401).json({ error: "Feil passord" });
    return;
  }

  const sid = await createSession({
    user: { id: "admin", email: null, firstName: null, lastName: null, profileImageUrl: null },
  });

  res.cookie(SESSION_COOKIE, sid, {
    httpOnly: true,
    secure: true,
    sameSite: "lax",
    path: "/",
    maxAge: SESSION_TTL,
  });

  res.json({ ok: true });
});

router.post("/auth/logout", async (req: Request, res: Response) => {
  const sid = getSessionId(req);
  await clearSession(res, sid);
  res.json({ ok: true });
});

export default router;

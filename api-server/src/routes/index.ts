import { Router, type IRouter, type Request, type Response, type NextFunction } from "express";
import healthRouter from "./health";
import authRouter from "./auth";
import transactionsRouter from "./transactions";
import portfolioRouter from "./portfolio";
import excelRouter from "./excel";

const router: IRouter = Router();

function requireAuth(req: Request, res: Response, next: NextFunction) {
  if (!req.isAuthenticated()) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }
  next();
}

router.use(healthRouter);
router.use(authRouter);
router.use(requireAuth);
router.use(transactionsRouter);
router.use(portfolioRouter);
router.use(excelRouter);

export default router;

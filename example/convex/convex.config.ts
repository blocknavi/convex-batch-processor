import batchProcessor from "convex-batch-processor/convex.config";
import { defineApp } from "convex/server";

const app = defineApp();

app.use(batchProcessor);

export default app;

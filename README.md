# convex-batch-processor

A Convex component for batch processing:

1. **Batch Accumulator** - Collect items and flush when reaching size threshold or time interval
2. **Table Iterator** - Process large tables in controlled batch sizes with pause/resume support

## Installation

```bash
npm install convex-batch-processor
```

## Setup

Add the component to your Convex app:

```typescript
// convex/convex.config.ts
import { defineApp } from "convex/server";
import batchProcessor from "convex-batch-processor/convex.config";

const app = defineApp();
app.use(batchProcessor);

export default app;
```

## Usage

### Batch Accumulator

The batch accumulator collects items and flushes them based on:
- **Size threshold**: Flush when `maxBatchSize` items collected
- **Time interval**: Flush automatically after `flushIntervalMs` since first item was added
- **Manual trigger**: Force flush via API call

```typescript
import { mutation, internalAction } from "./_generated/server";
import { internal, components } from "./_generated/api";
import { BatchProcessor } from "convex-batch-processor";
import { v } from "convex/values";

// Define the event schema
const analyticsEventValidator = v.object({
  eventName: v.string(),
  properties: v.optional(v.record(v.string(), v.string())),
  timestamp: v.number(),
});

type AnalyticsEvent = typeof analyticsEventValidator.type;

// Create a batch processor with config
// Note: Explicit type annotation is required when the file also exports Convex functions
const batchProcessor: BatchProcessor<AnalyticsEvent> = new BatchProcessor(components.batchProcessor, {
  maxBatchSize: 100,
  flushIntervalMs: 30000,
  processBatch: internal.analytics.processEventsBatch,
});

// Define the batch processor (must be an action)
export const processEventsBatch = internalAction({
  args: { items: v.array(analyticsEventValidator) },
  handler: async (ctx, { items }) => {
    await fetch("https://api.analytics.com/batch", {
      method: "POST",
      body: JSON.stringify({ events: items }),
    });
  },
});

// Add items to a batch
export const trackEvent = mutation({
  args: {
    eventName: v.string(),
    properties: v.optional(v.record(v.string(), v.string())),
  },
  handler: async (ctx, { eventName, properties }) => {
    const event: AnalyticsEvent = { eventName, properties, timestamp: Date.now() };

    return await batchProcessor.addItems(ctx, "analytics-events", [event]);
  },
});

// Manual flush
await batchProcessor.flush(ctx, "analytics-events");

// Get batch status
await batchProcessor.getBatchStatus(ctx, "analytics-events");

// Get flush history
await batchProcessor.getFlushHistory(ctx, "analytics-events");
```

The interval flush is scheduled automatically when the first item is added to a batch. No cron job is required.

### Table Iterator

The table iterator processes large datasets using a callback-based design:

1. **You provide**: `getNextBatch` (query) + `processBatch` (action)
2. **Component handles**: Scheduling, progress tracking, retries, pause/resume

```typescript
import { mutation, query, internalQuery, internalAction, internalMutation } from "./_generated/server";
import { internal, components } from "./_generated/api";
import { BatchProcessor } from "convex-batch-processor";
import { v } from "convex/values";

// Define the user schema
const userValidator = v.object({
  _id: v.id("users"),
  _creationTime: v.number(),
  name: v.string(),
  email: v.string(),
});

type User = typeof userValidator.type;

// For iterator-only usage, config is optional
const batchProcessor = new BatchProcessor(components.batchProcessor);

export const startMigration = mutation({
  args: {},
  handler: async (ctx) => {
    const jobId = `migration-${Date.now()}`;

    await batchProcessor.startIterator<User>(ctx, jobId, {
      batchSize: 100,
      delayBetweenBatchesMs: 100,
      getNextBatch: internal.migrations.getNextBatch,
      processBatch: internal.migrations.processBatch,
      onComplete: internal.migrations.onComplete,
      maxRetries: 5,
    });

    return { jobId };
  },
});

export const getNextBatch = internalQuery({
  args: {
    cursor: v.optional(v.string()),
    batchSize: v.number(),
  },
  handler: async (ctx, { cursor, batchSize }) => {
    const results = await ctx.db
      .query("users")
      .paginate({ cursor: cursor ?? null, numItems: batchSize });

    return {
      items: results.page,
      cursor: results.continueCursor,
      done: results.isDone,
    };
  },
});

export const processBatch = internalAction({
  args: { items: v.array(userValidator) },
  handler: async (ctx, { items }) => {
    for (const user of items) {
      await migrateUser(user);
    }
  },
});

export const onComplete = internalMutation({
  args: {
    jobId: v.string(),
    processedCount: v.number(),
  },
  handler: async (ctx, { jobId, processedCount }) => {
    console.log(`Migration ${jobId} completed: ${processedCount} users`);
  },
});
```

#### Job Control

```typescript
export const pause = mutation({
  args: { jobId: v.string() },
  handler: async (ctx, { jobId }) => {
    return await batchProcessor.pauseIterator(ctx, jobId);
  },
});

export const resume = mutation({
  args: { jobId: v.string() },
  handler: async (ctx, { jobId }) => {
    return await batchProcessor.resumeIterator(ctx, jobId);
  },
});

export const cancel = mutation({
  args: { jobId: v.string() },
  handler: async (ctx, { jobId }) => {
    return await batchProcessor.cancelIterator(ctx, jobId);
  },
});

export const getStatus = query({
  args: { jobId: v.string() },
  handler: async (ctx, { jobId }) => {
    return await batchProcessor.getIteratorStatus(ctx, jobId);
  },
});

export const listJobs = query({
  args: { status: v.optional(v.string()) },
  handler: async (ctx, { status }) => {
    return await batchProcessor.listIteratorJobs(ctx, { status });
  },
});
```

## API Reference

### BatchProcessor Class

#### Batch Accumulator Methods

| Method | Description |
|--------|-------------|
| `addItems(ctx, batchId, items)` | Add items to a batch |
| `flush(ctx, batchId)` | Force flush a batch |
| `getBatchStatus(ctx, batchId)` | Get batch status |
| `getFlushHistory(ctx, batchId, limit?)` | Get flush history |
| `deleteBatch(ctx, batchId)` | Delete a completed batch |

#### Table Iterator Methods

| Method | Description |
|--------|-------------|
| `startIterator(ctx, jobId, config)` | Start a new iterator job |
| `pauseIterator(ctx, jobId)` | Pause a running job |
| `resumeIterator(ctx, jobId)` | Resume a paused job |
| `cancelIterator(ctx, jobId)` | Cancel a job |
| `getIteratorStatus(ctx, jobId)` | Get job status |
| `listIteratorJobs(ctx, options?)` | List jobs |
| `deleteIteratorJob(ctx, jobId)` | Delete a completed job |

### Types

#### BatchConfig

```typescript
interface BatchConfig<T = unknown> {
  maxBatchSize: number;
  flushIntervalMs: number;
  processBatch: FunctionReference<"action", "internal", { items: T[] }>;
}
```

**`processBatch` Requirements:**

The `processBatch` **must be a Convex action** (not a mutation or plain JavaScript function). This is because:
- The batch processor is called via `ctx.runAction()` internally
- Actions can perform async operations like HTTP requests, which is the typical use case for batch processing (e.g., sending events to an external analytics service)

Pass the function reference directly:

```typescript
const batchProcessor: BatchProcessor<MyEvent> = new BatchProcessor(components.batchProcessor, {
  maxBatchSize: 100,
  flushIntervalMs: 30000,
  processBatch: internal.myModule.processBatch,
});
```

If you only need to write to the database (no external calls), you can still use an action that calls a mutation internally:

```typescript
export const processBatch = internalAction({
  args: { items: v.array(myItemValidator) },
  handler: async (ctx, { items }) => {
    await ctx.runMutation(internal.myModule.writeBatchToDb, { items });
  },
});
```

#### BatchResult

Returned by `addItems()`:

```typescript
interface BatchResult {
  batchId: string;      // Same ID you passed in
  itemCount: number;    // Total items in batch
  flushed: boolean;     // Whether batch was flushed
  status: BatchStatus;  // "accumulating" | "flushing" | "completed"
}
```

#### BatchStatusResult

Returned by `getBatchStatus()`:

```typescript
interface BatchStatusResult {
  batchId: string;  // Same ID you passed in
  batches: Array<{
    status: "accumulating" | "flushing";
    itemCount: number;
    createdAt: number;
    lastUpdatedAt: number;
  }>;
  config: {
    maxBatchSize: number;
    flushIntervalMs: number;
  };
}
```

The `batches` array contains all active batches. Typically there's one, but during flush there may be both a flushing batch and a new accumulating batch.

#### IteratorConfig

```typescript
interface IteratorConfig<T = unknown> {
  batchSize: number;
  delayBetweenBatchesMs?: number;
  getNextBatch: FunctionReference<"query", "internal", { cursor: string | undefined; batchSize: number }>;
  processBatch: FunctionReference<"action", "internal", { items: T[] }>;
  onComplete?: FunctionReference<"mutation", "internal", { jobId: string; processedCount: number }>;
  maxRetries?: number;
}
```

#### Callback Types

```typescript
interface GetNextBatchResult<T = unknown> {
  items: T[];
  cursor: string | undefined;
  done: boolean;
}

interface ProcessBatchArgs<T = unknown> {
  items: T[];
}

interface OnCompleteArgs {
  jobId: string;
  processedCount: number;
}
```

## TypeScript Notes

### Explicit Type Annotation Required

When creating a `BatchProcessor` with config in a file that also exports Convex functions (mutations, queries, actions), you must add an explicit type annotation:

```typescript
// ✅ Correct - explicit type annotation
const batchProcessor: BatchProcessor<MyEvent> = new BatchProcessor(components.batchProcessor, {
  processBatch: internal.myModule.processBatch,
  // ...
});

// ❌ Will cause error TS7022
const batchProcessor = new BatchProcessor<MyEvent>(components.batchProcessor, {
  processBatch: internal.myModule.processBatch,
  // ...
});
```

This is due to circular type inference: TypeScript can't infer the type because the `internal` API types are generated from your file's exports, creating a circular dependency. The explicit annotation breaks this cycle.

## Error Handling

### Batch Accumulator
- Failed flushes are recorded in `flushHistory`
- Items are preserved for retry (batch reverts to "accumulating" state)

### Table Iterator
- Automatic retry with exponential backoff (1s, 2s, 4s... up to 30s)
- Job marked as "failed" after `maxRetries` attempts
- Error messages preserved in job status

## Development

```bash
pnpm install
pnpm run build
pnpm test
pnpm run lint
pnpm run format
```

## License

MIT

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
import { mutation, query, internalAction } from "./_generated/server";
import { internal, components } from "./_generated/api";
import { BatchProcessor } from "convex-batch-processor";
import { v } from "convex/values";

const batchProcessor = new BatchProcessor(components.batchProcessor);

export const trackEvent = mutation({
  args: {
    eventName: v.string(),
    properties: v.any(),
  },
  handler: async (ctx, { eventName, properties }) => {
    const event = { eventName, properties, timestamp: Date.now() };

    return await batchProcessor.addItems(ctx, "analytics-events", [event], {
      maxBatchSize: 100,
      flushIntervalMs: 30000,
      onFlushHandle: internal.analytics.sendBatch.toString(),
    });
  },
});

export const flushEvents = mutation({
  args: {},
  handler: async (ctx) => {
    return await batchProcessor.flush(ctx, "analytics-events");
  },
});

export const getBatchStatus = query({
  args: {},
  handler: async (ctx) => {
    return await batchProcessor.getBatchStatus(ctx, "analytics-events");
  },
});

export const sendBatch = internalAction({
  args: { items: v.array(v.any()) },
  handler: async (ctx, { items }) => {
    await fetch("https://api.analytics.com/batch", {
      method: "POST",
      body: JSON.stringify({ events: items }),
    });
  },
});
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

const batchProcessor = new BatchProcessor(components.batchProcessor);

export const startMigration = mutation({
  args: {},
  handler: async (ctx) => {
    const jobId = `migration-${Date.now()}`;

    await batchProcessor.startIterator(ctx, jobId, {
      batchSize: 100,
      delayBetweenBatchesMs: 100,
      getNextBatchHandle: internal.migrations.getNextBatch.toString(),
      processBatchHandle: internal.migrations.processBatch.toString(),
      onCompleteHandle: internal.migrations.onComplete.toString(),
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
      .paginate({ cursor, numItems: batchSize });

    return {
      items: results.page,
      cursor: results.continueCursor,
      done: results.isDone,
    };
  },
});

export const processBatch = internalAction({
  args: { items: v.array(v.any()) },
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
| `addItems(ctx, batchId, items, config)` | Add items to a batch |
| `flush(ctx, batchId)` | Force flush a batch |
| `getBatchStatus(ctx, batchId)` | Get batch status |
| `getFlushHistory(ctx, batchId, limit?)` | Get flush history |
| `deleteBatch(ctx, batchId)` | Delete a completed batch |
| `triggerIntervalFlushes(ctx)` | Manually trigger interval-based flushes |

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
interface BatchConfig {
  maxBatchSize: number;
  flushIntervalMs: number;
  onFlushHandle?: string;
}
```

#### IteratorConfig

```typescript
interface IteratorConfig {
  batchSize: number;
  delayBetweenBatchesMs?: number;
  getNextBatchHandle: string;
  processBatchHandle: string;
  onCompleteHandle?: string;
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

interface OnFlushArgs<T = unknown> {
  items: T[];
}
```

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
npm install
npm run build
npm test
npm run lint
npm run format
```

## License

MIT

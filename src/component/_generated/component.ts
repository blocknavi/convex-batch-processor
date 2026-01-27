/* eslint-disable */
/**
 * Generated `ComponentApi` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type { FunctionReference } from "convex/server";

/**
 * A utility for referencing a Convex component's exposed API.
 *
 * Useful when expecting a parameter like `components.myComponent`.
 * Usage:
 * ```ts
 * async function myFunction(ctx: QueryCtx, component: ComponentApi) {
 *   return ctx.runQuery(component.someFile.someQuery, { ...args });
 * }
 * ```
 */
export type ComponentApi<Name extends string | undefined = string | undefined> =
  {
    lib: {
      addItems: FunctionReference<
        "mutation",
        "internal",
        {
          batchId: string;
          config: {
            flushIntervalMs: number;
            immediateFlushThreshold?: number;
            maxBatchSize?: number;
            processBatchHandle: string;
          };
          items: Array<any>;
        },
        any,
        Name
      >;
      cancelIteratorJob: FunctionReference<
        "mutation",
        "internal",
        { jobId: string },
        any,
        Name
      >;
      deleteBatch: FunctionReference<
        "mutation",
        "internal",
        { batchId: string },
        any,
        Name
      >;
      deleteIteratorJob: FunctionReference<
        "mutation",
        "internal",
        { jobId: string },
        any,
        Name
      >;
      flushBatch: FunctionReference<
        "mutation",
        "internal",
        { batchId: string },
        any,
        Name
      >;
      getAllBatchesForBaseId: FunctionReference<
        "query",
        "internal",
        { baseBatchId: string },
        any,
        Name
      >;
      getBatchStatus: FunctionReference<
        "query",
        "internal",
        { batchId: string },
        any,
        Name
      >;
      getFlushHistory: FunctionReference<
        "query",
        "internal",
        { batchId: string; limit?: number },
        any,
        Name
      >;
      getIteratorJobStatus: FunctionReference<
        "query",
        "internal",
        { jobId: string },
        any,
        Name
      >;
      listIteratorJobs: FunctionReference<
        "query",
        "internal",
        {
          limit?: number;
          status?: "pending" | "running" | "paused" | "completed" | "failed";
        },
        any,
        Name
      >;
      pauseIteratorJob: FunctionReference<
        "mutation",
        "internal",
        { jobId: string },
        any,
        Name
      >;
      resumeIteratorJob: FunctionReference<
        "mutation",
        "internal",
        { jobId: string },
        any,
        Name
      >;
      startIteratorJob: FunctionReference<
        "mutation",
        "internal",
        {
          config: {
            batchSize: number;
            delayBetweenBatchesMs?: number;
            getNextBatchHandle: string;
            maxRetries?: number;
            onCompleteHandle?: string;
            processBatchHandle: string;
          };
          jobId: string;
        },
        any,
        Name
      >;
    };
  };

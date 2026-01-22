import type {
	FunctionReference,
	GenericActionCtx,
	GenericMutationCtx,
	GenericQueryCtx,
} from "convex/server";

// =============================================================================
// Types
// =============================================================================

export type BatchStatus = "accumulating" | "flushing" | "completed";
export type JobStatus = "pending" | "running" | "paused" | "completed" | "failed";

export interface BatchConfig {
	/** Maximum number of items before auto-flush triggers */
	maxBatchSize: number;
	/** Time in ms after which batch flushes even if not full */
	flushIntervalMs: number;
	/** Function handle to call when flushing (receives { items: any[] }) */
	onFlushHandle?: string;
}

export interface IteratorConfig {
	/** Number of items to process per batch */
	batchSize: number;
	/** Delay in ms between batches (default: 100) */
	delayBetweenBatchesMs?: number;
	/** Function handle for fetching next batch (query/mutation that returns { items, cursor, done }) */
	getNextBatchHandle: string;
	/** Function handle for processing a batch (action that receives { items }) */
	processBatchHandle: string;
	/** Optional function handle called when job completes (mutation receiving { jobId, processedCount }) */
	onCompleteHandle?: string;
	/** Maximum retry attempts before marking job as failed (default: 5) */
	maxRetries?: number;
}

export interface BatchResult {
	batchId: string;
	itemCount: number;
	flushed: boolean;
	status: BatchStatus;
}

export interface FlushResult {
	batchId: string;
	itemCount: number;
	flushed: boolean;
	status?: BatchStatus;
	reason?: string;
}

export interface BatchStatusResult {
	batchId: string;
	itemCount: number;
	status: BatchStatus;
	createdAt: number;
	lastUpdatedAt: number;
	config: BatchConfig;
}

export interface JobResult {
	jobId: string;
	status: JobStatus;
}

export interface JobStatusResult {
	jobId: string;
	status: JobStatus;
	processedCount: number;
	cursor?: string;
	retryCount: number;
	errorMessage?: string;
	createdAt: number;
	lastRunAt?: number;
	config: {
		batchSize: number;
		delayBetweenBatchesMs: number;
	};
}

export interface JobListItem {
	jobId: string;
	status: JobStatus;
	processedCount: number;
	createdAt: number;
	lastRunAt?: number;
	errorMessage?: string;
}

export interface FlushHistoryItem {
	batchId: string;
	itemCount: number;
	flushedAt: number;
	durationMs: number;
	success: boolean;
	errorMessage?: string;
}

// =============================================================================
// Component API Type
// =============================================================================

/**
 * The API type for the batch processor component.
 * This is used internally to type the component reference.
 */
export interface BatchProcessorAPI {
	public: {
		// Batch Accumulator
		addItems: FunctionReference<
			"mutation",
			"public",
			{ batchId: string; items: unknown[]; config: BatchConfig },
			BatchResult
		>;
		flushBatch: FunctionReference<"mutation", "public", { batchId: string }, FlushResult>;
		getBatchStatus: FunctionReference<
			"query",
			"public",
			{ batchId: string },
			BatchStatusResult | null
		>;
		getFlushHistory: FunctionReference<
			"query",
			"public",
			{ batchId: string; limit?: number },
			FlushHistoryItem[]
		>;
		deleteBatch: FunctionReference<
			"mutation",
			"public",
			{ batchId: string },
			{ deleted: boolean; reason?: string }
		>;

		// Table Iterator
		startIteratorJob: FunctionReference<
			"mutation",
			"public",
			{ jobId: string; config: IteratorConfig },
			JobResult
		>;
		pauseIteratorJob: FunctionReference<"mutation", "public", { jobId: string }, JobResult>;
		resumeIteratorJob: FunctionReference<"mutation", "public", { jobId: string }, JobResult>;
		cancelIteratorJob: FunctionReference<
			"mutation",
			"public",
			{ jobId: string },
			JobResult & { reason?: string }
		>;
		getIteratorJobStatus: FunctionReference<
			"query",
			"public",
			{ jobId: string },
			JobStatusResult | null
		>;
		listIteratorJobs: FunctionReference<
			"query",
			"public",
			{ status?: JobStatus; limit?: number },
			JobListItem[]
		>;
		deleteIteratorJob: FunctionReference<
			"mutation",
			"public",
			{ jobId: string },
			{ deleted: boolean; reason?: string }
		>;
		triggerIntervalFlushes: FunctionReference<
			"action",
			"public",
			Record<string, never>,
			Array<{ batchId: string; flushed: boolean }>
		>;
	};
}

// =============================================================================
// BatchProcessor Client Class
// =============================================================================

/**
 * Client wrapper for the Convex Batch Processor component.
 *
 * @example
 * ```typescript
 * import { components } from "./_generated/api";
 * import { BatchProcessor } from "convex-batch-processor";
 *
 * const batchProcessor = new BatchProcessor(components.batchProcessor);
 *
 * // In a mutation/action:
 * await batchProcessor.addItems(ctx, "my-batch", [item1, item2], {
 *   maxBatchSize: 100,
 *   flushIntervalMs: 30000,
 *   onFlushHandle: "internal:myApp/batch:processBatch",
 * });
 * ```
 */
export class BatchProcessor {
	private component: BatchProcessorAPI;

	constructor(component: BatchProcessorAPI) {
		this.component = component;
	}

	// ===========================================================================
	// Batch Accumulator Methods
	// ===========================================================================

	/**
	 * Add items to a batch. Will auto-flush if the batch reaches maxBatchSize.
	 *
	 * @param ctx - Convex mutation context
	 * @param batchId - Unique identifier for the batch
	 * @param items - Array of items to add
	 * @param config - Batch configuration
	 * @returns Batch result with current status
	 */
	async addItems(
		ctx: GenericMutationCtx<any>,
		batchId: string,
		items: unknown[],
		config: BatchConfig,
	): Promise<BatchResult> {
		return await ctx.runMutation(this.component.public.addItems, {
			batchId,
			items,
			config,
		});
	}

	/**
	 * Force flush a batch regardless of size threshold.
	 *
	 * @param ctx - Convex mutation context
	 * @param batchId - Batch identifier to flush
	 * @returns Flush result
	 */
	async flush(ctx: GenericMutationCtx<any>, batchId: string): Promise<FlushResult> {
		return await ctx.runMutation(this.component.public.flushBatch, { batchId });
	}

	/**
	 * Get the current status of a batch.
	 *
	 * @param ctx - Convex query context
	 * @param batchId - Batch identifier
	 * @returns Batch status or null if not found
	 */
	async getBatchStatus(
		ctx: GenericQueryCtx<any>,
		batchId: string,
	): Promise<BatchStatusResult | null> {
		return await ctx.runQuery(this.component.public.getBatchStatus, { batchId });
	}

	/**
	 * Get flush history for a batch.
	 *
	 * @param ctx - Convex query context
	 * @param batchId - Batch identifier
	 * @param limit - Optional limit on number of history items
	 * @returns Array of flush history items
	 */
	async getFlushHistory(
		ctx: GenericQueryCtx<any>,
		batchId: string,
		limit?: number,
	): Promise<FlushHistoryItem[]> {
		return await ctx.runQuery(this.component.public.getFlushHistory, { batchId, limit });
	}

	/**
	 * Delete a batch (only if completed or empty).
	 *
	 * @param ctx - Convex mutation context
	 * @param batchId - Batch identifier to delete
	 * @returns Deletion result
	 */
	async deleteBatch(
		ctx: GenericMutationCtx<any>,
		batchId: string,
	): Promise<{ deleted: boolean; reason?: string }> {
		return await ctx.runMutation(this.component.public.deleteBatch, { batchId });
	}

	/**
	 * Trigger interval-based flushes for all batches.
	 * Call this periodically (e.g., via cron) to flush batches that
	 * haven't been updated within their flushIntervalMs.
	 *
	 * @param ctx - Convex action context
	 * @returns Array of flush results
	 */
	async triggerIntervalFlushes(
		ctx: GenericActionCtx<any>,
	): Promise<Array<{ batchId: string; flushed: boolean }>> {
		return await ctx.runAction(this.component.public.triggerIntervalFlushes, {});
	}

	// ===========================================================================
	// Table Iterator Methods
	// ===========================================================================

	/**
	 * Start a new iterator job.
	 *
	 * @param ctx - Convex mutation context
	 * @param jobId - Unique identifier for the job
	 * @param config - Iterator configuration with callback handles
	 * @returns Job result with initial status
	 *
	 * @example
	 * ```typescript
	 * await batchProcessor.startIterator(ctx, "user-migration", {
	 *   batchSize: 100,
	 *   delayBetweenBatchesMs: 100,
	 *   getNextBatchHandle: "internal:myApp/migrations:getNextBatch",
	 *   processBatchHandle: "internal:myApp/migrations:processBatch",
	 *   onCompleteHandle: "internal:myApp/migrations:migrationComplete",
	 * });
	 * ```
	 */
	async startIterator(
		ctx: GenericMutationCtx<any>,
		jobId: string,
		config: IteratorConfig,
	): Promise<JobResult> {
		return await ctx.runMutation(this.component.public.startIteratorJob, {
			jobId,
			config,
		});
	}

	/**
	 * Pause a running iterator job.
	 *
	 * @param ctx - Convex mutation context
	 * @param jobId - Job identifier to pause
	 * @returns Job result with updated status
	 */
	async pauseIterator(ctx: GenericMutationCtx<any>, jobId: string): Promise<JobResult> {
		return await ctx.runMutation(this.component.public.pauseIteratorJob, { jobId });
	}

	/**
	 * Resume a paused iterator job.
	 *
	 * @param ctx - Convex mutation context
	 * @param jobId - Job identifier to resume
	 * @returns Job result with updated status
	 */
	async resumeIterator(ctx: GenericMutationCtx<any>, jobId: string): Promise<JobResult> {
		return await ctx.runMutation(this.component.public.resumeIteratorJob, { jobId });
	}

	/**
	 * Cancel an iterator job.
	 *
	 * @param ctx - Convex mutation context
	 * @param jobId - Job identifier to cancel
	 * @returns Job result with final status
	 */
	async cancelIterator(
		ctx: GenericMutationCtx<any>,
		jobId: string,
	): Promise<JobResult & { reason?: string }> {
		return await ctx.runMutation(this.component.public.cancelIteratorJob, { jobId });
	}

	/**
	 * Get the status of an iterator job.
	 *
	 * @param ctx - Convex query context
	 * @param jobId - Job identifier
	 * @returns Job status or null if not found
	 */
	async getIteratorStatus(
		ctx: GenericQueryCtx<any>,
		jobId: string,
	): Promise<JobStatusResult | null> {
		return await ctx.runQuery(this.component.public.getIteratorJobStatus, { jobId });
	}

	/**
	 * List iterator jobs with optional status filter.
	 *
	 * @param ctx - Convex query context
	 * @param options - Optional filter options
	 * @returns Array of job list items
	 */
	async listIteratorJobs(
		ctx: GenericQueryCtx<any>,
		options?: { status?: JobStatus; limit?: number },
	): Promise<JobListItem[]> {
		return await ctx.runQuery(this.component.public.listIteratorJobs, options ?? {});
	}

	/**
	 * Delete an iterator job (only if completed or failed).
	 *
	 * @param ctx - Convex mutation context
	 * @param jobId - Job identifier to delete
	 * @returns Deletion result
	 */
	async deleteIteratorJob(
		ctx: GenericMutationCtx<any>,
		jobId: string,
	): Promise<{ deleted: boolean; reason?: string }> {
		return await ctx.runMutation(this.component.public.deleteIteratorJob, { jobId });
	}
}

// =============================================================================
// Helper Types for App Callbacks
// =============================================================================

/**
 * Return type for getNextBatch callback function
 */
export interface GetNextBatchResult<T = unknown> {
	/** Items in this batch */
	items: T[];
	/** Cursor for next batch (undefined if done) */
	cursor: string | undefined;
	/** True if this is the last batch */
	done: boolean;
}

/**
 * Arguments for getNextBatch callback function
 */
export interface GetNextBatchArgs {
	/** Current cursor position (undefined for first batch) */
	cursor: string | undefined;
	/** Number of items to fetch */
	batchSize: number;
}

/**
 * Arguments for processBatch callback function
 */
export interface ProcessBatchArgs<T = unknown> {
	/** Items to process */
	items: T[];
}

/**
 * Arguments for onComplete callback function
 */
export interface OnCompleteArgs {
	/** Job identifier */
	jobId: string;
	/** Total number of items processed */
	processedCount: number;
}

/**
 * Arguments for onFlush callback function
 */
export interface OnFlushArgs<T = unknown> {
	/** Items being flushed */
	items: T[];
}

import {
	type FunctionReference,
	type GenericMutationCtx,
	type GenericQueryCtx,
	createFunctionHandle,
} from "convex/server";

export type BatchStatus = "accumulating" | "flushing" | "completed";
export type JobStatus = "pending" | "running" | "paused" | "completed" | "failed";

// User-facing config interfaces (accept FunctionReference)
export interface BatchConfig<T = unknown> {
	maxBatchSize: number;
	flushIntervalMs: number;
	processBatch: FunctionReference<"action", "internal", { items: T[] }>;
}

export interface IteratorConfig<T = unknown> {
	batchSize: number;
	delayBetweenBatchesMs?: number;
	getNextBatch: FunctionReference<
		"query",
		"internal",
		{ cursor: string | undefined; batchSize: number }
	>;
	processBatch: FunctionReference<"action", "internal", { items: T[] }>;
	onComplete?: FunctionReference<"mutation", "internal", { jobId: string; processedCount: number }>;
	maxRetries?: number;
}

// Internal config interfaces (use string handles for component API)
interface InternalBatchConfig {
	maxBatchSize: number;
	flushIntervalMs: number;
	processBatchHandle: string;
}

interface InternalIteratorConfig {
	batchSize: number;
	delayBetweenBatchesMs?: number;
	getNextBatchHandle: string;
	processBatchHandle: string;
	onCompleteHandle?: string;
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
	batchId: string; // Client's original ID
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

export interface BatchListItem {
	batchId: string;
	baseBatchId: string;
	sequence: number;
	itemCount: number;
	status: BatchStatus;
	createdAt: number;
	lastUpdatedAt: number;
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

export interface BatchProcessorAPI {
	lib: {
		addItems: FunctionReference<
			"mutation",
			"internal",
			{ batchId: string; items: unknown[]; config: InternalBatchConfig },
			BatchResult
		>;
		flushBatch: FunctionReference<"mutation", "internal", { batchId: string }, FlushResult>;
		getBatchStatus: FunctionReference<
			"query",
			"internal",
			{ batchId: string },
			BatchStatusResult | null
		>;
		getFlushHistory: FunctionReference<
			"query",
			"internal",
			{ batchId: string; limit?: number },
			FlushHistoryItem[]
		>;
		getAllBatchesForBaseId: FunctionReference<
			"query",
			"internal",
			{ baseBatchId: string },
			BatchListItem[]
		>;
		deleteBatch: FunctionReference<
			"mutation",
			"internal",
			{ batchId: string },
			{ deleted: boolean; reason?: string }
		>;
		startIteratorJob: FunctionReference<
			"mutation",
			"internal",
			{ jobId: string; config: InternalIteratorConfig },
			JobResult
		>;
		pauseIteratorJob: FunctionReference<"mutation", "internal", { jobId: string }, JobResult>;
		resumeIteratorJob: FunctionReference<"mutation", "internal", { jobId: string }, JobResult>;
		cancelIteratorJob: FunctionReference<
			"mutation",
			"internal",
			{ jobId: string },
			JobResult & { reason?: string }
		>;
		getIteratorJobStatus: FunctionReference<
			"query",
			"internal",
			{ jobId: string },
			JobStatusResult | null
		>;
		listIteratorJobs: FunctionReference<
			"query",
			"internal",
			{ status?: JobStatus; limit?: number },
			JobListItem[]
		>;
		deleteIteratorJob: FunctionReference<
			"mutation",
			"internal",
			{ jobId: string },
			{ deleted: boolean; reason?: string }
		>;
	};
}

export class BatchProcessor<T = unknown> {
	private component: BatchProcessorAPI;
	private config?: BatchConfig<T>;
	private processBatchHandle: string | null = null;

	constructor(component: BatchProcessorAPI, config?: BatchConfig<T>) {
		this.component = component;
		this.config = config;
	}

	async addItems(ctx: GenericMutationCtx<any>, batchId: string, items: T[]): Promise<BatchResult> {
		if (!this.config) {
			throw new Error(
				"BatchProcessor config with processBatch is required to use addItems. Pass config to the constructor.",
			);
		}

		if (!this.processBatchHandle) {
			this.processBatchHandle = await createFunctionHandle(this.config.processBatch);
		}

		const internalConfig: InternalBatchConfig = {
			maxBatchSize: this.config.maxBatchSize,
			flushIntervalMs: this.config.flushIntervalMs,
			processBatchHandle: this.processBatchHandle,
		};

		return await ctx.runMutation(this.component.lib.addItems, {
			batchId,
			items,
			config: internalConfig,
		});
	}

	async flush(ctx: GenericMutationCtx<any>, batchId: string): Promise<FlushResult> {
		return await ctx.runMutation(this.component.lib.flushBatch, { batchId });
	}

	async getBatchStatus(
		ctx: GenericQueryCtx<any>,
		batchId: string,
	): Promise<BatchStatusResult | null> {
		return await ctx.runQuery(this.component.lib.getBatchStatus, { batchId });
	}

	async getFlushHistory(
		ctx: GenericQueryCtx<any>,
		batchId: string,
		limit?: number,
	): Promise<FlushHistoryItem[]> {
		return await ctx.runQuery(this.component.lib.getFlushHistory, { batchId, limit });
	}

	async getAllBatchesForBaseId(
		ctx: GenericQueryCtx<any>,
		baseBatchId: string,
	): Promise<BatchListItem[]> {
		return await ctx.runQuery(this.component.lib.getAllBatchesForBaseId, { baseBatchId });
	}

	async deleteBatch(
		ctx: GenericMutationCtx<any>,
		batchId: string,
	): Promise<{ deleted: boolean; reason?: string }> {
		return await ctx.runMutation(this.component.lib.deleteBatch, { batchId });
	}

	async startIterator<T>(
		ctx: GenericMutationCtx<any>,
		jobId: string,
		config: IteratorConfig<T>,
	): Promise<JobResult> {
		const internalConfig: InternalIteratorConfig = {
			batchSize: config.batchSize,
			delayBetweenBatchesMs: config.delayBetweenBatchesMs,
			getNextBatchHandle: await createFunctionHandle(config.getNextBatch),
			processBatchHandle: await createFunctionHandle(config.processBatch),
			onCompleteHandle: config.onComplete
				? await createFunctionHandle(config.onComplete)
				: undefined,
			maxRetries: config.maxRetries,
		};

		return await ctx.runMutation(this.component.lib.startIteratorJob, {
			jobId,
			config: internalConfig,
		});
	}

	async pauseIterator(ctx: GenericMutationCtx<any>, jobId: string): Promise<JobResult> {
		return await ctx.runMutation(this.component.lib.pauseIteratorJob, { jobId });
	}

	async resumeIterator(ctx: GenericMutationCtx<any>, jobId: string): Promise<JobResult> {
		return await ctx.runMutation(this.component.lib.resumeIteratorJob, { jobId });
	}

	async cancelIterator(
		ctx: GenericMutationCtx<any>,
		jobId: string,
	): Promise<JobResult & { reason?: string }> {
		return await ctx.runMutation(this.component.lib.cancelIteratorJob, { jobId });
	}

	async getIteratorStatus(
		ctx: GenericQueryCtx<any>,
		jobId: string,
	): Promise<JobStatusResult | null> {
		return await ctx.runQuery(this.component.lib.getIteratorJobStatus, { jobId });
	}

	async listIteratorJobs(
		ctx: GenericQueryCtx<any>,
		options?: { status?: JobStatus; limit?: number },
	): Promise<JobListItem[]> {
		return await ctx.runQuery(this.component.lib.listIteratorJobs, options ?? {});
	}

	async deleteIteratorJob(
		ctx: GenericMutationCtx<any>,
		jobId: string,
	): Promise<{ deleted: boolean; reason?: string }> {
		return await ctx.runMutation(this.component.lib.deleteIteratorJob, { jobId });
	}
}

export interface GetNextBatchResult<T = unknown> {
	items: T[];
	cursor: string | undefined;
	done: boolean;
}

export interface GetNextBatchArgs {
	cursor: string | undefined;
	batchSize: number;
}

export interface ProcessBatchArgs<T = unknown> {
	items: T[];
}

export interface OnCompleteArgs {
	jobId: string;
	processedCount: number;
}

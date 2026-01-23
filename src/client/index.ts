import {
	createFunctionHandle,
	type FunctionReference,
	type GenericMutationCtx,
	type GenericQueryCtx,
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
	getNextBatch: FunctionReference<"query", "internal", { cursor: string | undefined; batchSize: number }>;
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

export interface BatchProcessorAPI {
	public: {
		addItems: FunctionReference<
			"mutation",
			"public",
			{ batchId: string; items: unknown[]; config: InternalBatchConfig },
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
		startIteratorJob: FunctionReference<
			"mutation",
			"public",
			{ jobId: string; config: InternalIteratorConfig },
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

	async addItems(
		ctx: GenericMutationCtx<any>,
		batchId: string,
		items: T[],
	): Promise<BatchResult> {
		if (!this.config) {
			throw new Error("BatchProcessor config with processBatch is required to use addItems. Pass config to the constructor.");
		}

		if (!this.processBatchHandle) {
			this.processBatchHandle = await createFunctionHandle(this.config.processBatch);
		}

		const internalConfig: InternalBatchConfig = {
			maxBatchSize: this.config.maxBatchSize,
			flushIntervalMs: this.config.flushIntervalMs,
			processBatchHandle: this.processBatchHandle,
		};

		return await ctx.runMutation(this.component.public.addItems, {
			batchId,
			items,
			config: internalConfig,
		});
	}

	async flush(ctx: GenericMutationCtx<any>, batchId: string): Promise<FlushResult> {
		return await ctx.runMutation(this.component.public.flushBatch, { batchId });
	}

	async getBatchStatus(
		ctx: GenericQueryCtx<any>,
		batchId: string,
	): Promise<BatchStatusResult | null> {
		return await ctx.runQuery(this.component.public.getBatchStatus, { batchId });
	}

	async getFlushHistory(
		ctx: GenericQueryCtx<any>,
		batchId: string,
		limit?: number,
	): Promise<FlushHistoryItem[]> {
		return await ctx.runQuery(this.component.public.getFlushHistory, { batchId, limit });
	}

	async deleteBatch(
		ctx: GenericMutationCtx<any>,
		batchId: string,
	): Promise<{ deleted: boolean; reason?: string }> {
		return await ctx.runMutation(this.component.public.deleteBatch, { batchId });
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
			onCompleteHandle: config.onComplete ? await createFunctionHandle(config.onComplete) : undefined,
			maxRetries: config.maxRetries,
		};

		return await ctx.runMutation(this.component.public.startIteratorJob, {
			jobId,
			config: internalConfig,
		});
	}

	async pauseIterator(ctx: GenericMutationCtx<any>, jobId: string): Promise<JobResult> {
		return await ctx.runMutation(this.component.public.pauseIteratorJob, { jobId });
	}

	async resumeIterator(ctx: GenericMutationCtx<any>, jobId: string): Promise<JobResult> {
		return await ctx.runMutation(this.component.public.resumeIteratorJob, { jobId });
	}

	async cancelIterator(
		ctx: GenericMutationCtx<any>,
		jobId: string,
	): Promise<JobResult & { reason?: string }> {
		return await ctx.runMutation(this.component.public.cancelIteratorJob, { jobId });
	}

	async getIteratorStatus(
		ctx: GenericQueryCtx<any>,
		jobId: string,
	): Promise<JobStatusResult | null> {
		return await ctx.runQuery(this.component.public.getIteratorJobStatus, { jobId });
	}

	async listIteratorJobs(
		ctx: GenericQueryCtx<any>,
		options?: { status?: JobStatus; limit?: number },
	): Promise<JobListItem[]> {
		return await ctx.runQuery(this.component.public.listIteratorJobs, options ?? {});
	}

	async deleteIteratorJob(
		ctx: GenericMutationCtx<any>,
		jobId: string,
	): Promise<{ deleted: boolean; reason?: string }> {
		return await ctx.runMutation(this.component.public.deleteIteratorJob, { jobId });
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


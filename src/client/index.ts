import type {
	FunctionReference,
	GenericActionCtx,
	GenericMutationCtx,
	GenericQueryCtx,
} from "convex/server";

export type BatchStatus = "accumulating" | "flushing" | "completed";
export type JobStatus = "pending" | "running" | "paused" | "completed" | "failed";

export interface BatchConfig {
	maxBatchSize: number;
	flushIntervalMs: number;
	onFlushHandle?: string;
}

export interface IteratorConfig {
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

export class BatchProcessor {
	private component: BatchProcessorAPI;

	constructor(component: BatchProcessorAPI) {
		this.component = component;
	}

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

	async triggerIntervalFlushes(
		ctx: GenericActionCtx<any>,
	): Promise<Array<{ batchId: string; flushed: boolean }>> {
		return await ctx.runAction(this.component.public.triggerIntervalFlushes, {});
	}

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

export interface OnFlushArgs<T = unknown> {
	items: T[];
}

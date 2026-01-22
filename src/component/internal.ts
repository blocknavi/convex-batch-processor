import { internalAction, internalMutation, internalQuery } from "./_generated/server";
import { internal } from "./_generated/api";
import { v } from "convex/values";
import { createFunctionHandle, FunctionHandle } from "convex/server";

export const getBatch = internalQuery({
	args: { batchId: v.string() },
	handler: async (ctx, { batchId }) => {
		return await ctx.db
			.query("batches")
			.withIndex("by_batchId", (q) => q.eq("batchId", batchId))
			.first();
	},
});

export const executeFlush = internalAction({
	args: {
		batchDocId: v.id("batches"),
		items: v.array(v.any()),
		onFlushHandle: v.string(),
	},
	handler: async (ctx, { batchDocId, items, onFlushHandle }) => {
		const startTime = Date.now();
		let success = true;
		let errorMessage: string | undefined;

		try {
			const handle = onFlushHandle as FunctionHandle<"action", { items: unknown[] }>;
			await ctx.runAction(handle, { items });
		} catch (error) {
			success = false;
			errorMessage = error instanceof Error ? error.message : String(error);
		}

		const durationMs = Date.now() - startTime;

		await ctx.runMutation(internal.internal.recordFlushResult, {
			batchDocId,
			itemCount: items.length,
			durationMs,
			success,
			errorMessage,
		});

		return { success, errorMessage, durationMs };
	},
});

export const recordFlushResult = internalMutation({
	args: {
		batchDocId: v.id("batches"),
		itemCount: v.number(),
		durationMs: v.number(),
		success: v.boolean(),
		errorMessage: v.optional(v.string()),
	},
	handler: async (ctx, { batchDocId, itemCount, durationMs, success, errorMessage }) => {
		const batch = await ctx.db.get(batchDocId);
		if (!batch) return;

		await ctx.db.insert("flushHistory", {
			batchId: batch.batchId,
			itemCount,
			flushedAt: Date.now(),
			durationMs,
			success,
			errorMessage,
		});

		if (success) {
			await ctx.db.patch(batchDocId, {
				status: "completed",
				items: [],
				itemCount: 0,
				scheduledFlushId: undefined,
			});
		} else {
			let scheduledFlushId: typeof batch.scheduledFlushId = undefined;
			if (batch.config.flushIntervalMs > 0 && batch.config.onFlushHandle) {
				scheduledFlushId = await ctx.scheduler.runAfter(
					batch.config.flushIntervalMs,
					internal.internal.scheduledIntervalFlush,
					{ batchDocId }
				);
			}

			await ctx.db.patch(batchDocId, {
				status: "accumulating",
				scheduledFlushId,
			});
		}
	},
});

export const checkFlushTimers = internalMutation({
	args: {},
	handler: async (ctx) => {
		const now = Date.now();
		const batches = await ctx.db
			.query("batches")
			.withIndex("by_status", (q) => q.eq("status", "accumulating"))
			.collect();

		const batchesToFlush: Array<{ batchDocId: string; batchId: string }> = [];

		for (const batch of batches) {
			const timeSinceLastUpdate = now - batch.lastUpdatedAt;
			if (
				timeSinceLastUpdate >= batch.config.flushIntervalMs &&
				batch.itemCount > 0 &&
				batch.config.onFlushHandle
			) {
				batchesToFlush.push({
					batchDocId: batch._id,
					batchId: batch.batchId,
				});
			}
		}

		return batchesToFlush;
	},
});

export const markBatchFlushing = internalMutation({
	args: { batchDocId: v.id("batches") },
	handler: async (ctx, { batchDocId }) => {
		const batch = await ctx.db.get(batchDocId);
		if (!batch || batch.status !== "accumulating" || batch.itemCount === 0) {
			return null;
		}

		await ctx.db.patch(batchDocId, {
			status: "flushing",
			scheduledFlushId: undefined,
		});

		return {
			items: batch.items,
			onFlushHandle: batch.config.onFlushHandle,
		};
	},
});

export const scheduledIntervalFlush = internalAction({
	args: { batchDocId: v.id("batches") },
	handler: async (ctx, { batchDocId }) => {
		const batchData = await ctx.runMutation(internal.internal.markBatchFlushing, {
			batchDocId,
		});

		if (!batchData || !batchData.onFlushHandle) {
			return { flushed: false, reason: "Batch not ready for flush" };
		}

		const result = await ctx.runAction(internal.internal.executeFlush, {
			batchDocId,
			items: batchData.items,
			onFlushHandle: batchData.onFlushHandle,
		});

		return { flushed: true, ...result };
	},
});

export const getIteratorJob = internalQuery({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		return await ctx.db
			.query("iteratorJobs")
			.withIndex("by_jobId", (q) => q.eq("jobId", jobId))
			.first();
	},
});

export const processNextBatch = internalAction({
	args: { jobDocId: v.id("iteratorJobs") },
	handler: async (ctx, { jobDocId }) => {
		const job = await ctx.runQuery(internal.internal.getIteratorJobById, { jobDocId });
		if (!job || job.status !== "running") {
			return { processed: false, reason: "Job not found or not running" };
		}

		const maxRetries = job.config.maxRetries ?? 5;

		try {
			const getNextBatchHandle = job.config.getNextBatchHandle as FunctionHandle<
				"query",
				{ cursor: string | undefined; batchSize: number }
			>;

			const batchResult = await ctx.runQuery(getNextBatchHandle, {
				cursor: job.cursor ?? undefined,
				batchSize: job.config.batchSize,
			});

			const { items, cursor: nextCursor, done } = batchResult as {
				items: unknown[];
				cursor: string | undefined;
				done: boolean;
			};

			if (items.length > 0) {
				const processBatchHandle = job.config.processBatchHandle as FunctionHandle<
					"action",
					{ items: unknown[] }
				>;

				await ctx.runAction(processBatchHandle, { items });
			}

			const newProcessedCount = job.processedCount + items.length;

			if (done) {
				await ctx.runMutation(internal.internal.markJobCompleted, {
					jobDocId,
					processedCount: newProcessedCount,
				});

				if (job.config.onCompleteHandle) {
					const onCompleteHandle = job.config.onCompleteHandle as FunctionHandle<
						"mutation",
						{ jobId: string; processedCount: number }
					>;
					await ctx.runMutation(onCompleteHandle, {
						jobId: job.jobId,
						processedCount: newProcessedCount,
					});
				}

				return { processed: true, done: true, processedCount: newProcessedCount };
			}

			await ctx.runMutation(internal.internal.updateJobProgress, {
				jobDocId,
				cursor: nextCursor,
				processedCount: newProcessedCount,
			});

			await ctx.scheduler.runAfter(
				job.config.delayBetweenBatchesMs,
				internal.internal.processNextBatch,
				{ jobDocId }
			);

			return { processed: true, done: false, processedCount: newProcessedCount };
		} catch (error) {
			const errorMessage = error instanceof Error ? error.message : String(error);
			const newRetryCount = job.retryCount + 1;

			if (newRetryCount >= maxRetries) {
				await ctx.runMutation(internal.internal.markJobFailed, {
					jobDocId,
					errorMessage,
					retryCount: newRetryCount,
				});
				return { processed: false, reason: "Max retries exceeded", error: errorMessage };
			}

			const backoffMs = Math.min(1000 * Math.pow(2, newRetryCount), 30000);
			await ctx.runMutation(internal.internal.incrementRetryCount, {
				jobDocId,
				retryCount: newRetryCount,
				errorMessage,
			});

			await ctx.scheduler.runAfter(backoffMs, internal.internal.processNextBatch, { jobDocId });

			return { processed: false, reason: "Retrying", error: errorMessage, retryCount: newRetryCount };
		}
	},
});

export const getIteratorJobById = internalQuery({
	args: { jobDocId: v.id("iteratorJobs") },
	handler: async (ctx, { jobDocId }) => {
		return await ctx.db.get(jobDocId);
	},
});

export const updateJobProgress = internalMutation({
	args: {
		jobDocId: v.id("iteratorJobs"),
		cursor: v.optional(v.string()),
		processedCount: v.number(),
	},
	handler: async (ctx, { jobDocId, cursor, processedCount }) => {
		await ctx.db.patch(jobDocId, {
			cursor,
			processedCount,
			lastRunAt: Date.now(),
			retryCount: 0,
		});
	},
});

export const markJobCompleted = internalMutation({
	args: {
		jobDocId: v.id("iteratorJobs"),
		processedCount: v.number(),
	},
	handler: async (ctx, { jobDocId, processedCount }) => {
		await ctx.db.patch(jobDocId, {
			status: "completed",
			processedCount,
			lastRunAt: Date.now(),
		});
	},
});

export const markJobFailed = internalMutation({
	args: {
		jobDocId: v.id("iteratorJobs"),
		errorMessage: v.string(),
		retryCount: v.number(),
	},
	handler: async (ctx, { jobDocId, errorMessage, retryCount }) => {
		await ctx.db.patch(jobDocId, {
			status: "failed",
			errorMessage,
			retryCount,
			lastRunAt: Date.now(),
		});
	},
});

export const incrementRetryCount = internalMutation({
	args: {
		jobDocId: v.id("iteratorJobs"),
		retryCount: v.number(),
		errorMessage: v.string(),
	},
	handler: async (ctx, { jobDocId, retryCount, errorMessage }) => {
		await ctx.db.patch(jobDocId, {
			retryCount,
			errorMessage,
			lastRunAt: Date.now(),
		});
	},
});

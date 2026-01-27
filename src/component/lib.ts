import {
	mutation,
	query,
	internalAction,
	internalMutation,
	internalQuery,
} from "./_generated/server";
import { internal } from "./_generated/api";
import type { Doc } from "./_generated/dataModel";
import { v } from "convex/values";
import { FunctionHandle } from "convex/server";

// ============================================================================
// Batch Accumulator - Public API
// ============================================================================

export const addItems = mutation({
	args: {
		batchId: v.string(),
		items: v.array(v.any()),
		config: v.object({
			maxBatchSize: v.number(),
			flushIntervalMs: v.number(),
			processBatchHandle: v.string(),
		}),
	},
	handler: async (ctx, { batchId, items, config }) => {
		const now = Date.now();

		// Parse base batch ID (strip sequence if present)
		const baseBatchId = batchId.includes("::")
			? batchId.split("::")[0]
			: batchId;

		// Find an accumulating batch for this base ID
		let batch = await ctx.db
			.query("batches")
			.withIndex("by_baseBatchId_status", (q) =>
				q.eq("baseBatchId", baseBatchId).eq("status", "accumulating")
			)
			.first();

		let isNewBatch = false;
		if (!batch) {
			isNewBatch = true;

			// Find highest sequence number for this base ID
			const latestBatch = await ctx.db
				.query("batches")
				.withIndex("by_baseBatchId_status", (q) => q.eq("baseBatchId", baseBatchId))
				.order("desc")
				.first();

			const nextSequence = latestBatch ? latestBatch.sequence + 1 : 0;
			const newBatchId = `${baseBatchId}::${nextSequence}`;

			const batchDocId = await ctx.db.insert("batches", {
				batchId: newBatchId,
				baseBatchId,
				sequence: nextSequence,
				items: [],
				itemCount: 0,
				createdAt: now,
				lastUpdatedAt: now,
				status: "accumulating",
				config,
			});
			batch = await ctx.db.get(batchDocId);
		}

		if (!batch) {
			throw new Error(`Failed to create batch for ${baseBatchId}`);
		}

		const newItems = [...batch.items, ...items];
		const newItemCount = newItems.length;

		if (newItemCount >= config.maxBatchSize) {
			if (batch.scheduledFlushId) {
				await ctx.scheduler.cancel(batch.scheduledFlushId);
			}

			await ctx.db.patch(batch._id, {
				items: newItems,
				itemCount: newItemCount,
				lastUpdatedAt: now,
				status: "flushing",
				scheduledFlushId: undefined,
			});

			await ctx.scheduler.runAfter(0, internal.lib.executeFlush, {
				batchDocId: batch._id,
				items: newItems,
				processBatchHandle: config.processBatchHandle,
			});

			return {
				batchId: baseBatchId,
				itemCount: newItemCount,
				flushed: true,
				status: "flushing",
			};
		}

		let scheduledFlushId = batch.scheduledFlushId;
		const shouldScheduleFlush =
			config.flushIntervalMs > 0 &&
			!scheduledFlushId &&
			(isNewBatch || batch.itemCount === 0);

		if (shouldScheduleFlush) {
			scheduledFlushId = await ctx.scheduler.runAfter(
				config.flushIntervalMs,
				internal.lib.scheduledIntervalFlush,
				{ batchDocId: batch._id }
			);
		}

		await ctx.db.patch(batch._id, {
			items: newItems,
			itemCount: newItemCount,
			lastUpdatedAt: now,
			config,
			scheduledFlushId,
		});

		return {
			batchId: baseBatchId,
			itemCount: newItemCount,
			flushed: false,
			status: "accumulating",
		};
	},
});

export const flushBatch = mutation({
	args: { batchId: v.string() },
	handler: async (ctx, { batchId }) => {
		// First try exact match (for full batch IDs like "base::0")
		let batch = await ctx.db
			.query("batches")
			.withIndex("by_batchId", (q) => q.eq("batchId", batchId))
			.first();

		// If not found, try as base batch ID and find the accumulating batch
		if (!batch) {
			batch = await ctx.db
				.query("batches")
				.withIndex("by_baseBatchId_status", (q) =>
					q.eq("baseBatchId", batchId).eq("status", "accumulating")
				)
				.first();
		}

		if (!batch) {
			throw new Error(`Batch ${batchId} not found`);
		}

		if (batch.status !== "accumulating") {
			throw new Error(`Batch ${batch.baseBatchId} is not in accumulating state (current: ${batch.status})`);
		}

		if (batch.itemCount === 0) {
			return { batchId, itemCount: 0, flushed: false, reason: "Batch is empty" };
		}

		if (!batch.config.processBatchHandle) {
			throw new Error(`Batch ${batchId} has no processBatchHandle configured`);
		}

		if (batch.scheduledFlushId) {
			await ctx.scheduler.cancel(batch.scheduledFlushId);
		}

		await ctx.db.patch(batch._id, {
			status: "flushing",
			scheduledFlushId: undefined,
		});

		await ctx.scheduler.runAfter(0, internal.lib.executeFlush, {
			batchDocId: batch._id,
			items: batch.items,
			processBatchHandle: batch.config.processBatchHandle,
		});

		return {
			batchId,
			itemCount: batch.itemCount,
			flushed: true,
			status: "flushing",
		};
	},
});

export const getBatchStatus = query({
	args: { batchId: v.string() },
	handler: async (ctx, { batchId }) => {
		// Parse base batch ID (strip sequence if present)
		const baseBatchId = batchId.includes("::")
			? batchId.split("::")[0]
			: batchId;

		// Find all active (accumulating or flushing) batches for this base ID
		const accumulatingBatches = await ctx.db
			.query("batches")
			.withIndex("by_baseBatchId_status", (q) =>
				q.eq("baseBatchId", baseBatchId).eq("status", "accumulating")
			)
			.collect();

		const flushingBatches = await ctx.db
			.query("batches")
			.withIndex("by_baseBatchId_status", (q) =>
				q.eq("baseBatchId", baseBatchId).eq("status", "flushing")
			)
			.collect();

		const activeBatches = [...flushingBatches, ...accumulatingBatches];

		if (activeBatches.length === 0) {
			return null;
		}

		// Use config from any batch (they should all have the same config)
		const config = activeBatches[0].config;

		return {
			batchId: baseBatchId,
			batches: activeBatches.map((batch) => ({
				status: batch.status as "accumulating" | "flushing",
				itemCount: batch.itemCount,
				createdAt: batch.createdAt,
				lastUpdatedAt: batch.lastUpdatedAt,
			})),
			config: {
				maxBatchSize: config.maxBatchSize,
				flushIntervalMs: config.flushIntervalMs,
			},
		};
	},
});

export const getAllBatchesForBaseId = query({
	args: { baseBatchId: v.string() },
	handler: async (ctx, { baseBatchId }) => {
		const batches = await ctx.db
			.query("batches")
			.withIndex("by_baseBatchId_status", (q) => q.eq("baseBatchId", baseBatchId))
			.collect();

		return batches.map((batch) => ({
			batchId: batch.batchId,
			baseBatchId: batch.baseBatchId,
			sequence: batch.sequence,
			itemCount: batch.itemCount,
			status: batch.status,
			createdAt: batch.createdAt,
			lastUpdatedAt: batch.lastUpdatedAt,
		}));
	},
});

export const getFlushHistory = query({
	args: {
		batchId: v.string(),
		limit: v.optional(v.number()),
	},
	handler: async (ctx, { batchId, limit }) => {
		let query = ctx.db
			.query("flushHistory")
			.withIndex("by_batchId", (q) => q.eq("batchId", batchId))
			.order("desc");

		if (limit) {
			return await query.take(limit);
		}

		return await query.collect();
	},
});

export const deleteBatch = mutation({
	args: { batchId: v.string() },
	handler: async (ctx, { batchId }) => {
		const batch = await ctx.db
			.query("batches")
			.withIndex("by_batchId", (q) => q.eq("batchId", batchId))
			.first();

		if (!batch) {
			return { deleted: false, reason: "Batch not found" };
		}

		if (batch.status === "flushing") {
			return { deleted: false, reason: "Cannot delete batch while flushing" };
		}

		if (batch.status === "accumulating" && batch.itemCount > 0) {
			return { deleted: false, reason: "Cannot delete batch with pending items" };
		}

		if (batch.scheduledFlushId) {
			await ctx.scheduler.cancel(batch.scheduledFlushId);
		}

		await ctx.db.delete(batch._id);
		return { deleted: true };
	},
});

// ============================================================================
// Batch Accumulator - Internal Functions
// ============================================================================

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
		processBatchHandle: v.string(),
	},
	handler: async (ctx, { batchDocId, items, processBatchHandle }) => {
		const startTime = Date.now();
		let success = true;
		let errorMessage: string | undefined;

		try {
			const handle = processBatchHandle as FunctionHandle<"action", { items: unknown[] }>;
			await ctx.runAction(handle, { items });
		} catch (error) {
			success = false;
			errorMessage = error instanceof Error ? error.message : String(error);
		}

		const durationMs = Date.now() - startTime;

		await ctx.runMutation(internal.lib.recordFlushResult, {
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
			batchId: batch.baseBatchId, // Store client's original ID, not internal sequence
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

			// Clean up old completed batches for the same base ID
			// Keep only the most recent completed batch to reduce clutter
			const completedBatches = await ctx.db
				.query("batches")
				.withIndex("by_baseBatchId_status", (q) =>
					q.eq("baseBatchId", batch.baseBatchId).eq("status", "completed")
				)
				.collect();

			// Sort by sequence number descending and delete all but the most recent
			const sortedCompleted = completedBatches.sort((a, b) => b.sequence - a.sequence);
			for (let i = 1; i < sortedCompleted.length; i++) {
				await ctx.db.delete(sortedCompleted[i]._id);
			}
		} else {
			let scheduledFlushId: typeof batch.scheduledFlushId = undefined;
			if (batch.config.flushIntervalMs > 0 && batch.config.processBatchHandle) {
				scheduledFlushId = await ctx.scheduler.runAfter(
					batch.config.flushIntervalMs,
					internal.lib.scheduledIntervalFlush,
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
			processBatchHandle: batch.config.processBatchHandle,
		};
	},
});

export const scheduledIntervalFlush = internalAction({
	args: { batchDocId: v.id("batches") },
	handler: async (ctx, { batchDocId }): Promise<{
		flushed: boolean;
		reason?: string;
		success?: boolean;
		errorMessage?: string;
		durationMs?: number;
	}> => {
		const batchData: { items: unknown[]; processBatchHandle: string } | null = await ctx.runMutation(internal.lib.markBatchFlushing, {
			batchDocId,
		});

		if (!batchData || !batchData.processBatchHandle) {
			return { flushed: false, reason: "Batch not ready for flush" };
		}

		const result: { success: boolean; errorMessage?: string; durationMs: number } = await ctx.runAction(internal.lib.executeFlush, {
			batchDocId,
			items: batchData.items,
			processBatchHandle: batchData.processBatchHandle,
		});

		return { flushed: true, ...result };
	},
});

// ============================================================================
// Table Iterator - Public API
// ============================================================================

export const startIteratorJob = mutation({
	args: {
		jobId: v.string(),
		config: v.object({
			batchSize: v.number(),
			delayBetweenBatchesMs: v.optional(v.number()),
			getNextBatchHandle: v.string(),
			processBatchHandle: v.string(),
			onCompleteHandle: v.optional(v.string()),
			maxRetries: v.optional(v.number()),
		}),
	},
	handler: async (ctx, { jobId, config }) => {
		const existingJob = await ctx.db
			.query("iteratorJobs")
			.withIndex("by_jobId", (q) => q.eq("jobId", jobId))
			.first();

		if (existingJob) {
			throw new Error(`Job ${jobId} already exists`);
		}

		const now = Date.now();

		const jobDocId = await ctx.db.insert("iteratorJobs", {
			jobId,
			cursor: undefined,
			processedCount: 0,
			status: "running",
			config: {
				batchSize: config.batchSize,
				delayBetweenBatchesMs: config.delayBetweenBatchesMs ?? 100,
				getNextBatchHandle: config.getNextBatchHandle,
				processBatchHandle: config.processBatchHandle,
				onCompleteHandle: config.onCompleteHandle,
				maxRetries: config.maxRetries,
			},
			retryCount: 0,
			createdAt: now,
			lastRunAt: now,
		});

		await ctx.scheduler.runAfter(0, internal.lib.processNextBatch, { jobDocId });

		return { jobId, status: "running" };
	},
});

export const pauseIteratorJob = mutation({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		const job = await ctx.db
			.query("iteratorJobs")
			.withIndex("by_jobId", (q) => q.eq("jobId", jobId))
			.first();

		if (!job) {
			throw new Error(`Job ${jobId} not found`);
		}

		if (job.status !== "running") {
			throw new Error(`Job ${jobId} is not running (current: ${job.status})`);
		}

		await ctx.db.patch(job._id, {
			status: "paused",
		});

		return { jobId, status: "paused" };
	},
});

export const resumeIteratorJob = mutation({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		const job = await ctx.db
			.query("iteratorJobs")
			.withIndex("by_jobId", (q) => q.eq("jobId", jobId))
			.first();

		if (!job) {
			throw new Error(`Job ${jobId} not found`);
		}

		if (job.status !== "paused") {
			throw new Error(`Job ${jobId} is not paused (current: ${job.status})`);
		}

		await ctx.db.patch(job._id, {
			status: "running",
			retryCount: 0,
		});

		await ctx.scheduler.runAfter(0, internal.lib.processNextBatch, { jobDocId: job._id });

		return { jobId, status: "running" };
	},
});

export const cancelIteratorJob = mutation({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		const job = await ctx.db
			.query("iteratorJobs")
			.withIndex("by_jobId", (q) => q.eq("jobId", jobId))
			.first();

		if (!job) {
			throw new Error(`Job ${jobId} not found`);
		}

		if (job.status === "completed" || job.status === "failed") {
			return { jobId, status: job.status, reason: "Job already finished" };
		}

		await ctx.db.patch(job._id, {
			status: "failed",
			errorMessage: "Cancelled by user",
		});

		return { jobId, status: "failed" };
	},
});

export const getIteratorJobStatus = query({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		const job = await ctx.db
			.query("iteratorJobs")
			.withIndex("by_jobId", (q) => q.eq("jobId", jobId))
			.first();

		if (!job) {
			return null;
		}

		return {
			jobId: job.jobId,
			status: job.status,
			processedCount: job.processedCount,
			cursor: job.cursor,
			retryCount: job.retryCount,
			errorMessage: job.errorMessage,
			createdAt: job.createdAt,
			lastRunAt: job.lastRunAt,
			config: {
				batchSize: job.config.batchSize,
				delayBetweenBatchesMs: job.config.delayBetweenBatchesMs,
			},
		};
	},
});

export const listIteratorJobs = query({
	args: {
		status: v.optional(
			v.union(
				v.literal("pending"),
				v.literal("running"),
				v.literal("paused"),
				v.literal("completed"),
				v.literal("failed")
			)
		),
		limit: v.optional(v.number()),
	},
	handler: async (ctx, { status, limit }) => {
		let queryBuilder;

		if (status) {
			queryBuilder = ctx.db
				.query("iteratorJobs")
				.withIndex("by_status", (q) => q.eq("status", status));
		} else {
			queryBuilder = ctx.db.query("iteratorJobs");
		}

		const jobs = limit ? await queryBuilder.take(limit) : await queryBuilder.collect();

		return jobs.map((job) => ({
			jobId: job.jobId,
			status: job.status,
			processedCount: job.processedCount,
			createdAt: job.createdAt,
			lastRunAt: job.lastRunAt,
			errorMessage: job.errorMessage,
		}));
	},
});

export const deleteIteratorJob = mutation({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		const job = await ctx.db
			.query("iteratorJobs")
			.withIndex("by_jobId", (q) => q.eq("jobId", jobId))
			.first();

		if (!job) {
			return { deleted: false, reason: "Job not found" };
		}

		if (job.status === "running" || job.status === "paused") {
			return { deleted: false, reason: "Cannot delete active job" };
		}

		await ctx.db.delete(job._id);
		return { deleted: true };
	},
});

// ============================================================================
// Table Iterator - Internal Functions
// ============================================================================

export const getIteratorJob = internalQuery({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		return await ctx.db
			.query("iteratorJobs")
			.withIndex("by_jobId", (q) => q.eq("jobId", jobId))
			.first();
	},
});

export const getIteratorJobById = internalQuery({
	args: { jobDocId: v.id("iteratorJobs") },
	handler: async (ctx, { jobDocId }) => {
		return await ctx.db.get(jobDocId);
	},
});

export const processNextBatch = internalAction({
	args: { jobDocId: v.id("iteratorJobs") },
	handler: async (ctx, { jobDocId }): Promise<{
		processed: boolean;
		done?: boolean;
		processedCount?: number;
		reason?: string;
		error?: string;
		retryCount?: number;
	}> => {
		const job: Doc<"iteratorJobs"> | null = await ctx.runQuery(internal.lib.getIteratorJobById, { jobDocId });
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
				await ctx.runMutation(internal.lib.markJobCompleted, {
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

			await ctx.runMutation(internal.lib.updateJobProgress, {
				jobDocId,
				cursor: nextCursor,
				processedCount: newProcessedCount,
			});

			await ctx.scheduler.runAfter(
				job.config.delayBetweenBatchesMs,
				internal.lib.processNextBatch,
				{ jobDocId }
			);

			return { processed: true, done: false, processedCount: newProcessedCount };
		} catch (error) {
			const errorMessage = error instanceof Error ? error.message : String(error);
			const newRetryCount = job.retryCount + 1;

			if (newRetryCount >= maxRetries) {
				await ctx.runMutation(internal.lib.markJobFailed, {
					jobDocId,
					errorMessage,
					retryCount: newRetryCount,
				});
				return { processed: false, reason: "Max retries exceeded", error: errorMessage };
			}

			const backoffMs = Math.min(1000 * Math.pow(2, newRetryCount), 30000);
			await ctx.runMutation(internal.lib.incrementRetryCount, {
				jobDocId,
				retryCount: newRetryCount,
				errorMessage,
			});

			await ctx.scheduler.runAfter(backoffMs, internal.lib.processNextBatch, { jobDocId });

			return { processed: false, reason: "Retrying", error: errorMessage, retryCount: newRetryCount };
		}
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

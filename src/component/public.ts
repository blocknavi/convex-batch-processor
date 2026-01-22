import { mutation, query, action } from "./_generated/server";
import { internal } from "./_generated/api";
import { v } from "convex/values";

// =============================================================================
// Batch Accumulator Public API
// =============================================================================

/**
 * Add items to a batch. Will auto-flush if the batch reaches maxBatchSize.
 */
export const addItems = mutation({
	args: {
		batchId: v.string(),
		items: v.array(v.any()),
		config: v.object({
			maxBatchSize: v.number(),
			flushIntervalMs: v.number(),
			onFlushHandle: v.optional(v.string()),
		}),
	},
	handler: async (ctx, { batchId, items, config }) => {
		const now = Date.now();

		// Get or create batch
		let batch = await ctx.db
			.query("batches")
			.withIndex("by_batchId", (q) => q.eq("batchId", batchId))
			.first();

		if (!batch) {
			// Create new batch
			const batchDocId = await ctx.db.insert("batches", {
				batchId,
				items: [],
				itemCount: 0,
				createdAt: now,
				lastUpdatedAt: now,
				status: "accumulating",
				config,
			});
			batch = await ctx.db.get(batchDocId);
		}

		if (!batch || batch.status !== "accumulating") {
			throw new Error(`Batch ${batchId} is not in accumulating state`);
		}

		// Add items to batch
		const newItems = [...batch.items, ...items];
		const newItemCount = newItems.length;

		// Check if we need to auto-flush
		if (newItemCount >= config.maxBatchSize && config.onFlushHandle) {
			// Mark as flushing
			await ctx.db.patch(batch._id, {
				items: newItems,
				itemCount: newItemCount,
				lastUpdatedAt: now,
				status: "flushing",
			});

			// Schedule the flush action
			await ctx.scheduler.runAfter(0, internal.internal.executeFlush, {
				batchDocId: batch._id,
				items: newItems,
				onFlushHandle: config.onFlushHandle,
			});

			return {
				batchId,
				itemCount: newItemCount,
				flushed: true,
				status: "flushing",
			};
		}

		// Just add items without flushing
		await ctx.db.patch(batch._id, {
			items: newItems,
			itemCount: newItemCount,
			lastUpdatedAt: now,
			config, // Update config in case it changed
		});

		return {
			batchId,
			itemCount: newItemCount,
			flushed: false,
			status: "accumulating",
		};
	},
});

/**
 * Force flush a batch regardless of size threshold
 */
export const flushBatch = mutation({
	args: { batchId: v.string() },
	handler: async (ctx, { batchId }) => {
		const batch = await ctx.db
			.query("batches")
			.withIndex("by_batchId", (q) => q.eq("batchId", batchId))
			.first();

		if (!batch) {
			throw new Error(`Batch ${batchId} not found`);
		}

		if (batch.status !== "accumulating") {
			throw new Error(`Batch ${batchId} is not in accumulating state (current: ${batch.status})`);
		}

		if (batch.itemCount === 0) {
			return { batchId, itemCount: 0, flushed: false, reason: "Batch is empty" };
		}

		if (!batch.config.onFlushHandle) {
			throw new Error(`Batch ${batchId} has no onFlushHandle configured`);
		}

		// Mark as flushing
		await ctx.db.patch(batch._id, {
			status: "flushing",
		});

		// Schedule the flush action
		await ctx.scheduler.runAfter(0, internal.internal.executeFlush, {
			batchDocId: batch._id,
			items: batch.items,
			onFlushHandle: batch.config.onFlushHandle,
		});

		return {
			batchId,
			itemCount: batch.itemCount,
			flushed: true,
			status: "flushing",
		};
	},
});

/**
 * Get the current status of a batch
 */
export const getBatchStatus = query({
	args: { batchId: v.string() },
	handler: async (ctx, { batchId }) => {
		const batch = await ctx.db
			.query("batches")
			.withIndex("by_batchId", (q) => q.eq("batchId", batchId))
			.first();

		if (!batch) {
			return null;
		}

		return {
			batchId: batch.batchId,
			itemCount: batch.itemCount,
			status: batch.status,
			createdAt: batch.createdAt,
			lastUpdatedAt: batch.lastUpdatedAt,
			config: batch.config,
		};
	},
});

/**
 * Get flush history for a batch
 */
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

/**
 * Delete a batch (only if completed or accumulating with no items)
 */
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

		await ctx.db.delete(batch._id);
		return { deleted: true };
	},
});

// =============================================================================
// Table Iterator Public API
// =============================================================================

/**
 * Start a new iterator job
 */
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
		// Check if job already exists
		const existingJob = await ctx.db
			.query("iteratorJobs")
			.withIndex("by_jobId", (q) => q.eq("jobId", jobId))
			.first();

		if (existingJob) {
			throw new Error(`Job ${jobId} already exists`);
		}

		const now = Date.now();

		// Create the job
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

		// Schedule the first batch processing
		await ctx.scheduler.runAfter(0, internal.internal.processNextBatch, { jobDocId });

		return { jobId, status: "running" };
	},
});

/**
 * Pause a running iterator job
 */
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

/**
 * Resume a paused iterator job
 */
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
			retryCount: 0, // Reset retry count on resume
		});

		// Schedule the next batch processing
		await ctx.scheduler.runAfter(0, internal.internal.processNextBatch, { jobDocId: job._id });

		return { jobId, status: "running" };
	},
});

/**
 * Cancel an iterator job
 */
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

		// Just mark as paused - no way to cancel scheduled functions
		// The processNextBatch will check status and stop if not running
		await ctx.db.patch(job._id, {
			status: "failed",
			errorMessage: "Cancelled by user",
		});

		return { jobId, status: "failed" };
	},
});

/**
 * Get the status of an iterator job
 */
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

/**
 * List all iterator jobs with optional status filter
 */
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

/**
 * Delete an iterator job (only if completed or failed)
 */
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

// =============================================================================
// Batch Flush Timer Action
// =============================================================================

/**
 * Action to check and trigger interval-based flushes
 * This should be called periodically (e.g., via cron) to flush batches
 * that haven't been updated within their flushIntervalMs
 */
export const triggerIntervalFlushes = action({
	args: {},
	handler: async (ctx) => {
		// Get batches that need flushing
		const batchesToFlush = await ctx.runMutation(internal.internal.checkFlushTimers);

		const results: Array<{ batchId: string; flushed: boolean }> = [];

		for (const { batchDocId, batchId } of batchesToFlush) {
			// Get batch data and mark as flushing
			const batchData = await ctx.runMutation(internal.internal.markBatchFlushing, {
				batchDocId: batchDocId as any,
			});

			if (batchData && batchData.onFlushHandle) {
				// Execute flush
				await ctx.runAction(internal.internal.executeFlush, {
					batchDocId: batchDocId as any,
					items: batchData.items,
					onFlushHandle: batchData.onFlushHandle,
				});
				results.push({ batchId, flushed: true });
			} else {
				results.push({ batchId, flushed: false });
			}
		}

		return results;
	},
});

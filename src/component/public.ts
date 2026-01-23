import { mutation, query, action } from "./_generated/server";
import { internal } from "./_generated/api";
import { v } from "convex/values";

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

		let batch = await ctx.db
			.query("batches")
			.withIndex("by_batchId", (q) => q.eq("batchId", batchId))
			.first();

		let isNewBatch = false;
		if (!batch) {
			isNewBatch = true;
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

			await ctx.scheduler.runAfter(0, internal.internal.executeFlush, {
				batchDocId: batch._id,
				items: newItems,
				processBatchHandle: config.processBatchHandle,
			});

			return {
				batchId,
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
				internal.internal.scheduledIntervalFlush,
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
			batchId,
			itemCount: newItemCount,
			flushed: false,
			status: "accumulating",
		};
	},
});

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

		await ctx.scheduler.runAfter(0, internal.internal.executeFlush, {
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

		await ctx.scheduler.runAfter(0, internal.internal.processNextBatch, { jobDocId });

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

		await ctx.scheduler.runAfter(0, internal.internal.processNextBatch, { jobDocId: job._id });

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


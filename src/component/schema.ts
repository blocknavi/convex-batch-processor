import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
	batches: defineTable({
		batchId: v.string(),
		items: v.array(v.any()),
		itemCount: v.number(),
		createdAt: v.number(),
		lastUpdatedAt: v.number(),
		status: v.union(v.literal("accumulating"), v.literal("flushing"), v.literal("completed")),
		config: v.object({
			maxBatchSize: v.number(),
			flushIntervalMs: v.number(),
			processBatchHandle: v.string(),
		}),
		scheduledFlushId: v.optional(v.id("_scheduled_functions")),
	})
		.index("by_batchId", ["batchId"])
		.index("by_status", ["status"]),

	iteratorJobs: defineTable({
		jobId: v.string(),
		cursor: v.optional(v.string()),
		processedCount: v.number(),
		status: v.union(
			v.literal("pending"),
			v.literal("running"),
			v.literal("paused"),
			v.literal("completed"),
			v.literal("failed")
		),
		config: v.object({
			batchSize: v.number(),
			delayBetweenBatchesMs: v.number(),
			getNextBatchHandle: v.string(),
			processBatchHandle: v.string(),
			onCompleteHandle: v.optional(v.string()),
			maxRetries: v.optional(v.number()),
		}),
		retryCount: v.number(),
		errorMessage: v.optional(v.string()),
		createdAt: v.number(),
		lastRunAt: v.optional(v.number()),
	})
		.index("by_jobId", ["jobId"])
		.index("by_status", ["status"]),

	flushHistory: defineTable({
		batchId: v.string(),
		itemCount: v.number(),
		flushedAt: v.number(),
		durationMs: v.number(),
		success: v.boolean(),
		errorMessage: v.optional(v.string()),
	}).index("by_batchId", ["batchId"]),
});

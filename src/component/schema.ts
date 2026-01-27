import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
	batches: defineTable({
		batchId: v.string(), // Full ID with sequence: "base::0"
		baseBatchId: v.string(), // Base ID: "base"
		sequence: v.number(), // Sequence number: 0, 1, 2...
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
		flushStartedAt: v.optional(v.number()),
	})
		.index("by_batchId", ["batchId"])
		.index("by_baseBatchId_status", ["baseBatchId", "status"])
		.index("by_status", ["status"]),

	batchItems: defineTable({
		batchDocId: v.id("batches"),
		items: v.array(v.any()),
		itemCount: v.number(),
		createdAt: v.number(),
	})
		.index("by_batchDocId", ["batchDocId"])
		.index("by_batchDocId_createdAt", ["batchDocId", "createdAt"]),

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
		batchId: v.string(), // Client's original ID (baseBatchId)
		itemCount: v.number(),
		flushedAt: v.number(),
		durationMs: v.number(),
		success: v.boolean(),
		errorMessage: v.optional(v.string()),
	}).index("by_batchId", ["batchId"]),
});

import { BatchProcessor } from "convex-batch-processor";
import type {
	GetNextBatchArgs,
	GetNextBatchResult,
	OnCompleteArgs,
	OnFlushArgs,
	ProcessBatchArgs,
} from "convex-batch-processor";
import { v } from "convex/values";
import { components, internal } from "./_generated/api";
import {
	internalAction,
	internalMutation,
	internalQuery,
	mutation,
	query,
} from "./_generated/server";

const batchProcessor = new BatchProcessor(components.batchProcessor);

export const trackEvent = mutation({
	args: {
		eventName: v.string(),
		properties: v.optional(v.any()),
	},
	handler: async (ctx, { eventName, properties }) => {
		const event = {
			eventName,
			properties,
			timestamp: Date.now(),
		};

		return await batchProcessor.addItems(ctx, "analytics-events", [event], {
			maxBatchSize: 100,
			flushIntervalMs: 30000,
			onFlushHandle: internal.usage.sendAnalyticsBatch.toString(),
		});
	},
});

export const flushAnalytics = mutation({
	args: {},
	handler: async (ctx) => {
		return await batchProcessor.flush(ctx, "analytics-events");
	},
});

export const getAnalyticsStatus = query({
	args: {},
	handler: async (ctx) => {
		return await batchProcessor.getBatchStatus(ctx, "analytics-events");
	},
});

export const sendAnalyticsBatch = internalAction({
	args: { items: v.array(v.any()) },
	handler: async (_ctx, { items }: OnFlushArgs) => {
		console.log(`Sending ${items.length} analytics events`);
	},
});

export const startUserMigration = mutation({
	args: {
		batchSize: v.optional(v.number()),
	},
	handler: async (ctx, { batchSize }) => {
		const jobId = `user-migration-${Date.now()}`;

		await batchProcessor.startIterator(ctx, jobId, {
			batchSize: batchSize ?? 100,
			delayBetweenBatchesMs: 100,
			getNextBatchHandle: internal.usage.getNextUserBatch.toString(),
			processBatchHandle: internal.usage.processUserBatch.toString(),
			onCompleteHandle: internal.usage.migrationComplete.toString(),
			maxRetries: 5,
		});

		return { jobId };
	},
});

export const getNextUserBatch = internalQuery({
	args: {
		cursor: v.optional(v.string()),
		batchSize: v.number(),
	},
	handler: async (ctx, { cursor, batchSize }: GetNextBatchArgs): Promise<GetNextBatchResult> => {
		const results = await ctx.db.query("users").paginate({
			cursor: cursor ?? null,
			numItems: batchSize,
		});

		return {
			items: results.page,
			cursor: results.continueCursor ?? undefined,
			done: results.isDone,
		};
	},
});

export const processUserBatch = internalAction({
	args: { items: v.array(v.any()) },
	handler: async (_ctx, { items }: ProcessBatchArgs) => {
		for (const user of items) {
			console.log(`Processing user: ${(user as any)._id}`);
		}
	},
});

export const migrationComplete = internalMutation({
	args: {
		jobId: v.string(),
		processedCount: v.number(),
	},
	handler: async (_ctx, { jobId, processedCount }: OnCompleteArgs) => {
		console.log(`Migration ${jobId} completed: ${processedCount} users processed`);
	},
});

export const getMigrationStatus = query({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		return await batchProcessor.getIteratorStatus(ctx, jobId);
	},
});

export const pauseMigration = mutation({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		return await batchProcessor.pauseIterator(ctx, jobId);
	},
});

export const resumeMigration = mutation({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		return await batchProcessor.resumeIterator(ctx, jobId);
	},
});

export const listMigrations = query({
	args: {
		status: v.optional(
			v.union(
				v.literal("pending"),
				v.literal("running"),
				v.literal("paused"),
				v.literal("completed"),
				v.literal("failed"),
			),
		),
	},
	handler: async (ctx, { status }) => {
		return await batchProcessor.listIteratorJobs(ctx, { status });
	},
});

export const checkBatchFlushes = internalAction({
	args: {},
	handler: async (ctx) => {
		return await batchProcessor.triggerIntervalFlushes(ctx);
	},
});

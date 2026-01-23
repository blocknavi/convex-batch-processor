import { BatchProcessor } from "convex-batch-processor";
import type { GetNextBatchArgs, GetNextBatchResult, OnCompleteArgs } from "convex-batch-processor";
import { v } from "convex/values";
import { components, internal } from "./_generated/api";
import {
	internalAction,
	internalMutation,
	internalQuery,
	mutation,
	query,
} from "./_generated/server";

// Define the analytics event schema
const analyticsEventValidator = v.object({
	eventName: v.string(),
	properties: v.optional(v.record(v.string(), v.union(v.string(), v.number(), v.boolean()))),
	timestamp: v.number(),
});

type AnalyticsEvent = typeof analyticsEventValidator.type;

// Create a batch processor for analytics events
const analyticsBatchProcessor = new BatchProcessor<AnalyticsEvent>(components.batchProcessor, {
	maxBatchSize: 100,
	flushIntervalMs: 30000,
	processBatch: internal.usage.sendAnalyticsBatch,
});

export const trackEvent = mutation({
	args: {
		eventName: v.string(),
		properties: v.optional(v.record(v.string(), v.union(v.string(), v.number(), v.boolean()))),
	},
	handler: async (ctx, { eventName, properties }) => {
		const event: AnalyticsEvent = {
			eventName,
			properties,
			timestamp: Date.now(),
		};

		return await analyticsBatchProcessor.addItems(ctx, "analytics-events", [event]);
	},
});

export const flushAnalytics = mutation({
	args: {},
	handler: async (ctx) => {
		return await analyticsBatchProcessor.flush(ctx, "analytics-events");
	},
});

export const getAnalyticsStatus = query({
	args: {},
	handler: async (ctx) => {
		return await analyticsBatchProcessor.getBatchStatus(ctx, "analytics-events");
	},
});

export const sendAnalyticsBatch = internalAction({
	args: { items: v.array(analyticsEventValidator) },
	handler: async (_ctx, { items }) => {
		console.log(`Sending ${items.length} analytics events`);
	},
});

// Define user schema for the iterator example
const userValidator = v.object({
	_id: v.id("users"),
	_creationTime: v.number(),
	name: v.string(),
	email: v.string(),
});

type User = typeof userValidator.type;

// Create a batch processor for iterator jobs (no config needed for iterators)
const iteratorProcessor = new BatchProcessor(components.batchProcessor);

export const startUserMigration = mutation({
	args: {
		batchSize: v.optional(v.number()),
	},
	handler: async (ctx, { batchSize }) => {
		const jobId = `user-migration-${Date.now()}`;

		await iteratorProcessor.startIterator<User>(ctx, jobId, {
			batchSize: batchSize ?? 100,
			delayBetweenBatchesMs: 100,
			getNextBatch: internal.usage.getNextUserBatch,
			processBatch: internal.usage.processUserBatch,
			onComplete: internal.usage.migrationComplete,
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
	handler: async (ctx, { cursor, batchSize }: GetNextBatchArgs): Promise<GetNextBatchResult<User>> => {
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
	args: { items: v.array(userValidator) },
	handler: async (_ctx, { items }) => {
		for (const user of items) {
			console.log(`Processing user: ${user._id}`);
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
		return await iteratorProcessor.getIteratorStatus(ctx, jobId);
	},
});

export const pauseMigration = mutation({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		return await iteratorProcessor.pauseIterator(ctx, jobId);
	},
});

export const resumeMigration = mutation({
	args: { jobId: v.string() },
	handler: async (ctx, { jobId }) => {
		return await iteratorProcessor.resumeIterator(ctx, jobId);
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
		return await iteratorProcessor.listIteratorJobs(ctx, { status });
	},
});

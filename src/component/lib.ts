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

// Declare console for TypeScript (available at runtime in Convex)
declare const console: {
	log: (...args: unknown[]) => void;
	error: (...args: unknown[]) => void;
	warn: (...args: unknown[]) => void;
};

// ============================================================================
// Batch Accumulator - Public API
// ============================================================================

export const addItems = mutation({
	args: {
		batchId: v.string(),
		items: v.array(v.any()),
		config: v.object({
			immediateFlushThreshold: v.optional(v.number()),
			/** @deprecated Use immediateFlushThreshold instead */
			maxBatchSize: v.optional(v.number()),
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

		// 1. Find accumulating batch (READ only)
		let batch = await ctx.db
			.query("batches")
			.withIndex("by_baseBatchId_status", (q) =>
				q.eq("baseBatchId", baseBatchId).eq("status", "accumulating")
			)
			.first();

		// 2. If no batch, create one WITH timer (one-time INSERT)
		if (!batch) {
			// Find highest sequence number for this base ID
			const latestBatch = await ctx.db
				.query("batches")
				.withIndex("by_baseBatchId_sequence", (q) => q.eq("baseBatchId", baseBatchId))
				.order("desc")
				.first();

			const nextSequence = latestBatch ? latestBatch.sequence + 1 : 0;
			const newBatchId = `${baseBatchId}::${nextSequence}`;

			const batchDocId = await ctx.db.insert("batches", {
				batchId: newBatchId,
				baseBatchId,
				sequence: nextSequence,
				createdAt: now,
				lastUpdatedAt: now,
				status: "accumulating",
				config,
			});
			batch = (await ctx.db.get(batchDocId))!;

			// Schedule timer at creation (not on every add)
			if (config.flushIntervalMs > 0) {
				const scheduledFlushId = await ctx.scheduler.runAfter(
					config.flushIntervalMs,
					internal.lib.maybeFlush,
					{ batchDocId: batch._id, force: true }
				);
				await ctx.db.patch(batch._id, { scheduledFlushId });
			}
		}

		// 3. INSERT items (NEVER conflicts - always a new document)
		await ctx.db.insert("batchItems", {
			batchDocId: batch._id,
			items,
			itemCount: items.length,
			createdAt: now,
		});

		// 4. Schedule flush check ONLY if this single call could complete a batch
		//    DO NOT query batchItems to count - that causes OCC conflicts when
		//    multiple concurrent addItems all read the same index.
		//
		//    Dual-trigger pattern:
		//    - SIZE trigger: items.length >= immediateFlushThreshold (handled here)
		//    - TIME trigger: flushIntervalMs timer (scheduled at batch creation)
		//
		//    For high-throughput small items, the interval timer handles flushing.
		//    For large single calls, we trigger immediate flush check.
		const threshold = config.immediateFlushThreshold ?? config.maxBatchSize;
		if (threshold !== undefined && items.length >= threshold) {
			await ctx.scheduler.runAfter(0, internal.lib.maybeFlush, {
				batchDocId: batch._id,
			});
		}

		// 5. Return success - NO PATCH, NO COUNT QUERY!
		//    We return the count of items added in THIS call only.
		//    Total count can be obtained via getBatchStatus query if needed.
		return {
			batchId: baseBatchId,
			itemCount: items.length,
			flushed: false, // Flush happens via interval timer or large batch detection
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

		// Compute item count from batchItems
		const batchItemDocs = await ctx.db
			.query("batchItems")
			.withIndex("by_batchDocId", (q) => q.eq("batchDocId", batch._id))
			.collect();
		const itemCount = batchItemDocs.reduce((sum, doc) => sum + doc.itemCount, 0);

		if (itemCount === 0) {
			return { batchId, itemCount: 0, flushed: false, reason: "Batch is empty" };
		}

		if (!batch.config.processBatchHandle) {
			throw new Error(`Batch ${batchId} has no processBatchHandle configured`);
		}

		// Schedule maybeFlush to handle the transition (avoids OCC in user-facing mutation)
		// Pass force: true to bypass threshold check for manual flushes
		await ctx.scheduler.runAfter(0, internal.lib.maybeFlush, {
			batchDocId: batch._id,
			force: true,
		});

		return {
			batchId,
			itemCount,
			flushed: true, // Will be flushed by maybeFlush
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

		// Compute itemCount and lastUpdatedAt from batchItems for each batch
		const batchesWithCounts = await Promise.all(
			activeBatches.map(async (batch) => {
				const batchItemDocs = await ctx.db
					.query("batchItems")
					.withIndex("by_batchDocId", (q) => q.eq("batchDocId", batch._id))
					.collect();
				const itemCount = batchItemDocs.reduce((sum, doc) => sum + doc.itemCount, 0);
				// Compute lastUpdatedAt as max of batchItems.createdAt, or fall back to batch.lastUpdatedAt
				const lastUpdatedAt = batchItemDocs.length > 0
					? Math.max(...batchItemDocs.map((doc) => doc.createdAt))
					: batch.lastUpdatedAt;
				return {
					status: batch.status as "accumulating" | "flushing",
					itemCount,
					createdAt: batch.createdAt,
					lastUpdatedAt,
				};
			})
		);

		return {
			batchId: baseBatchId,
			batches: batchesWithCounts,
			config: {
				immediateFlushThreshold: config.immediateFlushThreshold,
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

		// Compute itemCount and lastUpdatedAt from batchItems for each batch
		return Promise.all(
			batches.map(async (batch) => {
				const batchItemDocs = await ctx.db
					.query("batchItems")
					.withIndex("by_batchDocId", (q) => q.eq("batchDocId", batch._id))
					.collect();
				const itemCount = batchItemDocs.reduce((sum, doc) => sum + doc.itemCount, 0);
				// Compute lastUpdatedAt as max of batchItems.createdAt, or fall back to batch.lastUpdatedAt
				const lastUpdatedAt =
					batchItemDocs.length > 0
						? Math.max(...batchItemDocs.map((doc) => doc.createdAt))
						: batch.lastUpdatedAt;
				return {
					batchId: batch.batchId,
					baseBatchId: batch.baseBatchId,
					sequence: batch.sequence,
					itemCount,
					status: batch.status,
					createdAt: batch.createdAt,
					lastUpdatedAt,
				};
			})
		);
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

		// Compute item count from batchItems
		const batchItems = await ctx.db
			.query("batchItems")
			.withIndex("by_batchDocId", (q) => q.eq("batchDocId", batch._id))
			.collect();
		const itemCount = batchItems.reduce((sum, doc) => sum + doc.itemCount, 0);

		if (batch.status === "accumulating" && itemCount > 0) {
			return { deleted: false, reason: "Cannot delete batch with pending items" };
		}

		if (batch.scheduledFlushId) {
			await ctx.scheduler.cancel(batch.scheduledFlushId);
		}

		// Delete associated batchItems
		for (const item of batchItems) {
			await ctx.db.delete(item._id);
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

export const collectBatchItems = internalQuery({
	args: { batchDocId: v.id("batches") },
	handler: async (ctx, { batchDocId }) => {
		const batch = await ctx.db.get(batchDocId);
		if (!batch) {
			return { items: [], flushStartedAt: undefined };
		}

		const flushStartedAt = batch.flushStartedAt ?? Date.now();

		// Get all batchItems created before flushStartedAt
		const batchItemDocs = await ctx.db
			.query("batchItems")
			.withIndex("by_batchDocId_createdAt", (q) =>
				q.eq("batchDocId", batchDocId).lt("createdAt", flushStartedAt + 1)
			)
			.collect();

		// Flatten all items from the batchItem documents
		const items: unknown[] = [];
		for (const doc of batchItemDocs) {
			items.push(...doc.items);
		}

		return { items, flushStartedAt };
	},
});

// Type for flush transition result
type FlushTransitionResult =
	| { flushed: true; itemCount: number; processBatchHandle: string }
	| { flushed: false; reason: string };

/**
 * maybeFlush - Attempts to transition a batch from "accumulating" to "flushing" state.
 *
 * This is an internal action scheduled by addItems (when threshold is reached) or
 * flushBatch (for manual flushes). It serves as a lightweight coordinator that
 * delegates the actual state transition to doFlushTransition (a mutation).
 *
 * ## Why this architecture?
 *
 * 1. **OCC is handled automatically by Convex**: When doFlushTransition (a mutation)
 *    encounters an OCC conflict, Convex automatically retries it. We don't need
 *    external retry logic like ActionRetrier for database operations.
 *
 * 2. **Race conditions are handled gracefully**: Multiple maybeFlush calls can be
 *    scheduled concurrently (e.g., rapid addItems calls all hitting threshold).
 *    The first one to execute wins the race and transitions the batch to "flushing".
 *    Subsequent calls see status !== "accumulating" and return early with
 *    reason: "not_accumulating". This is expected behavior, not an error.
 *
 * 3. **Mutations can't call actions directly**: Convex mutations are deterministic
 *    and can't have side effects. To execute the user's processBatchHandle (an action),
 *    we need this action layer. The flow is:
 *      mutation (addItems) → schedules action (maybeFlush)
 *      action (maybeFlush) → calls mutation (doFlushTransition)
 *      mutation (doFlushTransition) → schedules action (executeFlush)
 *
 * 4. **Non-blocking for callers**: addItems returns immediately after scheduling
 *    maybeFlush. Users don't wait for the flush to complete.
 *
 * ## Failure scenarios
 *
 * - If maybeFlush fails completely (rare), the batch stays in "accumulating" state.
 *   The next addItems call that hits threshold will schedule another maybeFlush.
 * - If a scheduled interval flush exists, it will also attempt the transition.
 *
 * @param batchDocId - The batch document ID to potentially flush
 * @param force - If true, flush regardless of threshold (used by manual flush and interval timer)
 */
export const maybeFlush = internalAction({
	args: {
		batchDocId: v.id("batches"),
		force: v.optional(v.boolean()),
	},
	handler: async (ctx, { batchDocId, force }): Promise<void> => {
		console.log("[maybeFlush] START", { batchDocId, force });

		// Call the mutation directly - Convex handles OCC retries automatically.
		// If another maybeFlush already transitioned this batch, the mutation
		// returns { flushed: false, reason: "not_accumulating" } which is fine.
		const result = await ctx.runMutation(internal.lib.doFlushTransition, {
			batchDocId,
			force: force ?? false,
		});

		console.log("[maybeFlush] doFlushTransition result", result);

		// If the transition succeeded, execute the flush directly
		// (Scheduling from mutation was unreliable in component context)
		if (result.flushed) {
			console.log("[maybeFlush] Calling executeFlush directly");
			await ctx.runAction(internal.lib.executeFlush, {
				batchDocId,
				processBatchHandle: result.processBatchHandle,
			});
			console.log("[maybeFlush] executeFlush completed");
		}
	},
});

/**
 * doFlushTransition - The actual state machine transition from "accumulating" to "flushing".
 *
 * This mutation is the source of truth for batch state transitions. It's designed to be
 * idempotent and race-condition safe:
 *
 * - Returns early if batch is already flushing/completed (another caller won the race)
 * - Returns early if batch is empty or below threshold (unless force=true)
 * - On success, atomically updates status and returns processBatchHandle for caller to execute
 *
 * Note: This mutation returns the processBatchHandle instead of scheduling executeFlush.
 * The calling action (maybeFlush) is responsible for calling executeFlush directly.
 * This design avoids issues with scheduling actions from within component mutations.
 *
 * OCC (Optimistic Concurrency Control) note:
 * If two doFlushTransition calls race, Convex detects the conflict when both try to
 * patch the same batch document. One succeeds, the other is auto-retried by Convex.
 * On retry, it sees status="flushing" and returns { flushed: false, reason: "not_accumulating" }.
 */
export const doFlushTransition = internalMutation({
	args: {
		batchDocId: v.id("batches"),
		force: v.optional(v.boolean()),
	},
	handler: async (ctx, { batchDocId, force }): Promise<FlushTransitionResult> => {
		console.log("[doFlushTransition] START", { batchDocId, force });

		const batch = await ctx.db.get(batchDocId);
		console.log("[doFlushTransition] Batch state", {
			exists: !!batch,
			status: batch?.status,
			scheduledFlushId: batch?.scheduledFlushId
		});

		// Already flushing or completed? Nothing to do. This handles the race condition
		// where multiple maybeFlush calls are scheduled - the first one wins.
		if (!batch || batch.status !== "accumulating") {
			console.log("[doFlushTransition] EARLY RETURN - not_accumulating");
			return { flushed: false, reason: "not_accumulating" };
		}

		// Check actual count from batchItems
		const batchItemDocs = await ctx.db
			.query("batchItems")
			.withIndex("by_batchDocId", (q) => q.eq("batchDocId", batchDocId))
			.collect();
		const totalCount = batchItemDocs.reduce((sum, doc) => sum + doc.itemCount, 0);
		console.log("[doFlushTransition] Item count", { totalCount, batchItemDocs: batchItemDocs.length });

		// Empty batch? Nothing to flush.
		if (totalCount === 0) {
			console.log("[doFlushTransition] EARLY RETURN - empty");
			return { flushed: false, reason: "empty" };
		}

		// Not at threshold? Skip only if not forced (interval flush uses force=true).
		const threshold = batch.config.immediateFlushThreshold ?? batch.config.maxBatchSize;
		if (!force && threshold !== undefined && totalCount < threshold) {
			console.log("[doFlushTransition] EARLY RETURN - below_threshold");
			return { flushed: false, reason: "below_threshold" };
		}

		// Cancel scheduled timer if exists
		if (batch.scheduledFlushId) {
			console.log("[doFlushTransition] Cancelling scheduled timer", batch.scheduledFlushId);
			await ctx.scheduler.cancel(batch.scheduledFlushId);
		}

		// Transition to flushing
		const now = Date.now();
		await ctx.db.patch(batchDocId, {
			status: "flushing",
			flushStartedAt: now,
			lastUpdatedAt: now,
			scheduledFlushId: undefined,
		});
		console.log("[doFlushTransition] Patched batch to flushing");

		// Return the processBatchHandle so maybeFlush can call executeFlush directly
		// (Scheduling from mutation was unreliable in component context)
		console.log("[doFlushTransition] Returning processBatchHandle for direct execution");
		return {
			flushed: true,
			itemCount: totalCount,
			processBatchHandle: batch.config.processBatchHandle
		};
	},
});

export const executeFlush = internalAction({
	args: {
		batchDocId: v.id("batches"),
		processBatchHandle: v.string(),
	},
	handler: async (ctx, { batchDocId, processBatchHandle }) => {
		console.log("[executeFlush] ENTERED", { batchDocId, processBatchHandle });

		const startTime = Date.now();
		let success = true;
		let errorMessage: string | undefined;

		// Collect items from batchItems table
		console.log("[executeFlush] Collecting batch items...");
		const { items, flushStartedAt } = await ctx.runQuery(internal.lib.collectBatchItems, {
			batchDocId,
		});
		console.log("[executeFlush] Collected items", { count: items.length });

		if (items.length === 0) {
			await ctx.runMutation(internal.lib.recordFlushResult, {
				batchDocId,
				itemCount: 0,
				durationMs: 0,
				success: true,
				flushStartedAt,
			});
			return { success: true, durationMs: 0 };
		}

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
			flushStartedAt,
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
		flushStartedAt: v.optional(v.number()),
	},
	handler: async (ctx, { batchDocId, itemCount, durationMs, success, errorMessage, flushStartedAt }) => {
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
			// Delete all batchItems that were included in this flush (created before flushStartedAt)
			const cutoffTime = flushStartedAt ?? batch.flushStartedAt ?? Date.now();
			const batchItemsToDelete = await ctx.db
				.query("batchItems")
				.withIndex("by_batchDocId_createdAt", (q) =>
					q.eq("batchDocId", batchDocId).lt("createdAt", cutoffTime + 1)
				)
				.collect();

			for (const item of batchItemsToDelete) {
				await ctx.db.delete(item._id);
			}

			// Check for stranded items (added after flushStartedAt)
			const remainingItems = await ctx.db
				.query("batchItems")
				.withIndex("by_batchDocId", (q) => q.eq("batchDocId", batchDocId))
				.collect();
			const remainingCount = remainingItems.reduce((sum, item) => sum + item.itemCount, 0);

			if (remainingCount > 0) {
				// Don't complete - keep accumulating for stranded items
				await ctx.db.patch(batchDocId, {
					status: "accumulating",
					flushStartedAt: undefined,
					lastUpdatedAt: Date.now(),
				});

				// Schedule another maybeFlush if at threshold
				const threshold = batch.config.immediateFlushThreshold ?? batch.config.maxBatchSize;
				if (threshold !== undefined && remainingCount >= threshold) {
					await ctx.scheduler.runAfter(0, internal.lib.maybeFlush, { batchDocId });
				} else if (batch.config.flushIntervalMs > 0) {
					// Re-schedule interval timer
					const scheduledFlushId = await ctx.scheduler.runAfter(
						batch.config.flushIntervalMs,
						internal.lib.maybeFlush,
						{ batchDocId, force: true }
					);
					await ctx.db.patch(batchDocId, { scheduledFlushId });
				}
			} else {
				// No stranded items - mark completed
				await ctx.db.patch(batchDocId, {
					status: "completed",
					flushStartedAt: undefined,
					lastUpdatedAt: Date.now(),
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
					// Also delete batchItems for old completed batches
					const oldBatchItems = await ctx.db
						.query("batchItems")
						.withIndex("by_batchDocId", (q) => q.eq("batchDocId", sortedCompleted[i]._id))
						.collect();
					for (const item of oldBatchItems) {
						await ctx.db.delete(item._id);
					}
					await ctx.db.delete(sortedCompleted[i]._id);
				}
			}
		} else {
			// Failure case - revert to accumulating
			let scheduledFlushId: typeof batch.scheduledFlushId = undefined;
			if (batch.config.flushIntervalMs > 0 && batch.config.processBatchHandle) {
				scheduledFlushId = await ctx.scheduler.runAfter(
					batch.config.flushIntervalMs,
					internal.lib.maybeFlush,
					{ batchDocId, force: true }
				);
			}

			await ctx.db.patch(batchDocId, {
				status: "accumulating",
				flushStartedAt: undefined,
				scheduledFlushId,
			});
		}
	},
});

/**
 * scheduledIntervalFlush - Timer-triggered flush that runs after flushIntervalMs.
 *
 * Scheduled once when a batch is created (if flushIntervalMs > 0). Uses force=true
 * to flush regardless of whether the batch has reached immediateFlushThreshold.
 * This ensures batches don't sit indefinitely waiting for more items.
 */
export const scheduledIntervalFlush = internalAction({
	args: { batchDocId: v.id("batches") },
	handler: async (ctx, { batchDocId }): Promise<void> => {
		// Use force=true to flush regardless of threshold
		await ctx.runMutation(internal.lib.doFlushTransition, {
			batchDocId,
			force: true,
		});
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

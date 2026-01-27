import { describe, expect, test } from "vitest";
import {
	type BatchConfig,
	BatchProcessor,
	type GetNextBatchArgs,
	type GetNextBatchResult,
	type IteratorConfig,
	type OnCompleteArgs,
	type ProcessBatchArgs,
} from "./index";

describe("BatchProcessor client", () => {
	test("exports BatchProcessor class", () => {
		expect(BatchProcessor).toBeDefined();
		expect(typeof BatchProcessor).toBe("function");
	});

	test("BatchProcessor constructor accepts component API without config", () => {
		// Create a mock component API
		const mockComponent = {
			lib: {
				addItems: {} as any,
				flushBatch: {} as any,
				getBatchStatus: {} as any,
				getFlushHistory: {} as any,
				deleteBatch: {} as any,
				startIteratorJob: {} as any,
				pauseIteratorJob: {} as any,
				resumeIteratorJob: {} as any,
				cancelIteratorJob: {} as any,
				getIteratorJobStatus: {} as any,
				listIteratorJobs: {} as any,
				deleteIteratorJob: {} as any,
			},
		};

		const processor = new BatchProcessor(mockComponent);
		expect(processor).toBeInstanceOf(BatchProcessor);
	});

	test("BatchProcessor constructor accepts component API with config", () => {
		const mockComponent = {
			lib: {
				addItems: {} as any,
				flushBatch: {} as any,
				getBatchStatus: {} as any,
				getFlushHistory: {} as any,
				deleteBatch: {} as any,
				startIteratorJob: {} as any,
				pauseIteratorJob: {} as any,
				resumeIteratorJob: {} as any,
				cancelIteratorJob: {} as any,
				getIteratorJobStatus: {} as any,
				listIteratorJobs: {} as any,
				deleteIteratorJob: {} as any,
			},
		};

		const processor = new BatchProcessor(mockComponent, {
			maxBatchSize: 100,
			flushIntervalMs: 30000,
			processBatch: {} as any, // Mock FunctionReference
		});
		expect(processor).toBeInstanceOf(BatchProcessor);
	});
});

describe("type exports", () => {
	test("BatchConfig type is usable", () => {
		const config: BatchConfig = {
			maxBatchSize: 100,
			flushIntervalMs: 30000,
			processBatch: {} as any, // FunctionReference is opaque, use mock
		};
		expect(config.maxBatchSize).toBe(100);
	});

	test("IteratorConfig type is usable", () => {
		const config: IteratorConfig = {
			batchSize: 50,
			delayBetweenBatchesMs: 100,
			getNextBatch: {} as any, // FunctionReference mock
			processBatch: {} as any, // FunctionReference mock
			onComplete: {} as any, // FunctionReference mock
			maxRetries: 3,
		};
		expect(config.batchSize).toBe(50);
	});

	test("GetNextBatchResult type is usable", () => {
		const result: GetNextBatchResult<{ id: number }> = {
			items: [{ id: 1 }, { id: 2 }],
			cursor: "cursor-123",
			done: false,
		};
		expect(result.items.length).toBe(2);
	});

	test("GetNextBatchArgs type is usable", () => {
		const args: GetNextBatchArgs = {
			cursor: "cursor-123",
			batchSize: 100,
		};
		expect(args.batchSize).toBe(100);
	});

	test("ProcessBatchArgs type is usable", () => {
		const args: ProcessBatchArgs<{ id: number }> = {
			items: [{ id: 1 }],
		};
		expect(args.items.length).toBe(1);
	});

	test("OnCompleteArgs type is usable", () => {
		const args: OnCompleteArgs = {
			jobId: "job-123",
			processedCount: 1000,
		};
		expect(args.processedCount).toBe(1000);
	});
});

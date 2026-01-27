# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.7] - 2026-01-27

### Fixed
- Fixed `BatchProcessorAPI` type to use `"internal"` visibility instead of `"public"` to match Convex-generated component APIs
- **Fixed OCC conflicts in high-throughput scenarios**: Removed batchItems count query from `addItems` mutation. Now uses dual-trigger pattern:
  - **Time trigger**: Interval timer flushes after `flushIntervalMs` (handles accumulating small items)
  - **Size trigger**: Immediate flush when single call adds `>= maxBatchSize` items (handles large batches)
- Fixed action scheduling reliability in component context by calling `executeFlush` directly from `maybeFlush` instead of scheduling

### Changed
- **BREAKING**: `addItems()` now returns `itemCount` as the number of items added in THIS call, not the total count. Use `getBatchStatus()` to get total counts if needed.
- Internal: Moved batch items to separate `batchItems` table for append-only writes (no client API changes)
- Internal: Simplified flush mechanism to leverage Convex's built-in OCC retry instead of external action-retrier
- Internal: Removed `@convex-dev/action-retrier` dependency (fewer dependencies, simpler architecture)
- Internal: Removed unused `getBatchItemCount` internal query
- **BREAKING**: Moved batch processor config from `addItems()` to constructor
  - Before: `new BatchProcessor(component)` then `addItems(ctx, id, items, config)`
  - After: `new BatchProcessor(component, config)` then `addItems(ctx, id, items)`
- **BREAKING**: Renamed `onFlush` to `processBatch` for consistency with iterator API
- **BREAKING**: Removed `OnFlushArgs` type - use `ProcessBatchArgs` instead
- **BREAKING**: Simplified `BatchResult` - removed `actualBatchId` field (was always identical to `batchId`)
- **BREAKING**: Redesigned `BatchStatusResult` to support multiple active batches:
  - Now returns `batches` array showing all active batches (flushing + accumulating)
  - Removed `baseBatchId` and `sequence` fields (internal implementation details)
  - Simplified `config` to only include `maxBatchSize` and `flushIntervalMs`
- Config is optional when only using iterator functionality

## [0.2.0] - 2025-01-22

### Added
- Testing infrastructure with vitest
- Client type tests for BatchProcessor class and exported types
- Generated component types export (`./_generated/component`)
- Publishing infrastructure (PUBLISHING.md, release scripts)

### Changed
- Updated Convex dependency to ^1.31.6
- Simplified tsconfig.build.json with extends pattern
- Cleaned up code comments

## [0.1.0] - 2024-01-01

### Added
- Initial release
- Batch accumulator with auto-flush and interval-based flushing
- Table iterator with pause/resume/cancel support
- Retry logic with exponential backoff for iterator jobs
- Flush history tracking
- TypeScript client wrapper with full type support

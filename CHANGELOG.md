# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Fixed `BatchProcessorAPI` type to use `"internal"` visibility instead of `"public"` to match Convex-generated component APIs

### Changed
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

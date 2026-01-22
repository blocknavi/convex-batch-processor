# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Testing infrastructure with vitest
- Client type tests for BatchProcessor class and exported types
- Generated component types export (`./_generated/component`)
- Publishing infrastructure (PUBLISHING.md, release scripts)

### Changed
- Updated Convex dependency to ^1.31.6
- Simplified tsconfig.build.json with extends pattern

## [0.1.0] - 2024-01-01

### Added
- Initial release
- Batch accumulator with auto-flush and interval-based flushing
- Table iterator with pause/resume/cancel support
- Retry logic with exponential backoff for iterator jobs
- Flush history tracking
- TypeScript client wrapper with full type support

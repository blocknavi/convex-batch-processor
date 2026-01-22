# Publishing Guide

This document describes how to publish new versions of `convex-batch-processor` to npm.

## Prerequisites

1. Ensure you have npm publish access to the package
2. Ensure you're logged in to npm: `npm login`
3. Ensure you're on the `main` branch with a clean working directory

## Pre-release Checklist

Before publishing, ensure:

- [ ] All tests pass: `pnpm run test`
- [ ] Linting passes: `pnpm run lint`
- [ ] Build succeeds: `pnpm run build:clean`
- [ ] Update `CHANGELOG.md` with release notes

## Publishing an Alpha Release

For testing new features before a stable release:

```bash
pnpm run alpha
```

This will:
1. Bump the version with an alpha prerelease tag (e.g., `0.1.1-alpha.0`)
2. Publish to npm with the `alpha` tag
3. Push the version commit and tag to git

Install alpha versions with:
```bash
npm install convex-batch-processor@alpha
```

## Publishing a Stable Release

For production-ready releases:

```bash
pnpm run release
```

This will:
1. Run the preversion checks (install, build, test, lint)
2. Bump the patch version (e.g., `0.1.0` -> `0.1.1`)
3. Publish to npm with the `latest` tag
4. Push the version commit and tag to git

## Manual Version Bumps

For minor or major version bumps:

```bash
# Minor version (0.1.0 -> 0.2.0)
npm version minor && npm publish && git push --follow-tags

# Major version (0.1.0 -> 1.0.0)
npm version major && npm publish && git push --follow-tags
```

## Post-publish

After publishing:

1. Verify the package is available: `npm view convex-batch-processor`
2. Test installation in a fresh project
3. Update any dependent projects if needed

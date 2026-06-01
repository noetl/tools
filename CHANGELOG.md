# Changelog

All notable changes to this project will be documented in this file.

## [2.10.0](https://github.com/noetl/tools/compare/v2.9.0...v2.10.0) (2026-06-01)

### Features

* **arrow_codec:** add try_encode_tabular_json for R-2.2 tabular outputs ([0655fc0](https://github.com/noetl/tools/commit/0655fc0acad7e3797ef4e410de7e0ea724975036)), closes [noetl/worker#24](https://github.com/noetl/worker/issues/24) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [2.9.0] (2026-05-31)

### Versioning realignment (no behaviour change)

Bump from `1.1.1` to `2.9.0` to skip past the yanked `2.8.7` release on crates.io.  Until this release the local source tree and the published crate were on diverging version tracks: the repo had been reset to a `1.x.x` line for the R-1.x development cycle, but the lone `2.8.7` publish (from March 2026, before the reset) was still the only crates.io artifact — so downstream consumers (noetl-worker, noetl-server) pinned to `noetl-tools = "2.8.7"` couldn't reach any of the 1.x fixes via crates.io.

This release publishes the 1.1.1 code (including the EE-3 kind-validation shell-tool fix) under the version `2.9.0` so the existing `^2.8.7` SemVer constraint on downstream crates picks it up via `cargo update`.  `2.8.7` is yanked post-publish.

Includes everything from 1.1.0 and 1.1.1:

- **arrow:** add arrow-rs to noetl-tools (R-2.1)
- **shell:** default shell to sh instead of bash for Alpine compat (closes [noetl/tools#3](https://github.com/noetl/tools/issues/3))
- **ci:** trigger release.yml from semantic-release + add required perms

Refs [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30).

## [1.1.1](https://github.com/noetl/tools/compare/v1.1.0...v1.1.1) (2026-05-31)

### Bug Fixes

* **shell:** default shell to sh instead of bash for Alpine compat ([58c0b8a](https://github.com/noetl/tools/commit/58c0b8acaf343808795b1fa63d3c7909509b3b33)), closes [noetl/tools#3](https://github.com/noetl/tools/issues/3) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

## [1.1.0](https://github.com/noetl/tools/compare/v1.0.0...v1.1.0) (2026-05-30)

### Features

* **arrow:** add arrow-rs to noetl-tools (R-2.1) ([765aef5](https://github.com/noetl/tools/commit/765aef59553e96ff4b957afe2284b4710c1b674e)), closes [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30) [noetl/cli#19](https://github.com/noetl/cli/issues/19)

## 1.0.0 (2026-03-02)

### Bug Fixes

* make release input parsing event-safe ([b21d853](https://github.com/noetl/tools/commit/b21d853d48f7e3fa4652087b8b9658310300f14b))
* release workflows on push and semantic auth ([678361c](https://github.com/noetl/tools/commit/678361ca18631233fd0b9fde5cafef08f785b7ac))
* remove secret expressions from workflow conditions ([9017e4e](https://github.com/noetl/tools/commit/9017e4ec67c7d641bd023213e353af864535cae9))

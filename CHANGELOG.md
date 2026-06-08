# Changelog

All notable changes to this project will be documented in this file.

## [2.24.0](https://github.com/noetl/tools/compare/v2.23.1...v2.24.0) (2026-06-08)

### Features

* **python:** inject input_data global + support top-level return ([4329e87](https://github.com/noetl/tools/commit/4329e8743a293ab3d59a851932c946279da32176)), closes [#65](https://github.com/noetl/tools/issues/65) [noetl/ai-meta#71](https://github.com/noetl/ai-meta/issues/71)

## [2.23.1](https://github.com/noetl/tools/compare/v2.23.0...v2.23.1) (2026-06-08)

### Bug Fixes

* **artifact:** accept `args:` as alias for `input:` in tool config ([be42bfe](https://github.com/noetl/tools/commit/be42bfed849c5dd9b4b26be9d63c5ffcb4c985d6)), closes [noetl/ai-meta#68](https://github.com/noetl/ai-meta/issues/68) [noetl/ai-meta#56](https://github.com/noetl/ai-meta/issues/56) [noetl/ai-meta#68](https://github.com/noetl/ai-meta/issues/68)

## [2.23.0](https://github.com/noetl/tools/compare/v2.22.0...v2.23.0) (2026-06-07)

### Features

* **python:** support legacy main() function convention ([9f8550c](https://github.com/noetl/tools/commit/9f8550cb0871928a74d66f2f8156a0d77f67c71e)), closes [noetl/ai-meta#65](https://github.com/noetl/ai-meta/issues/65) [#38](https://github.com/noetl/tools/issues/38) [noetl/ai-meta#65](https://github.com/noetl/ai-meta/issues/65)

## [2.22.0](https://github.com/noetl/tools/compare/v2.21.0...v2.22.0) (2026-06-07)

### Features

* **python:** external script loaders for file/gcs/http source types ([834166e](https://github.com/noetl/tools/commit/834166efa97e85bae9354fab0d2c7ecc5e5d56e9)), closes [#63](https://github.com/noetl/tools/issues/63) [noetl/ai-meta#65](https://github.com/noetl/ai-meta/issues/65) [#63](https://github.com/noetl/tools/issues/63) [noetl/ai-meta#65](https://github.com/noetl/ai-meta/issues/65)

## [2.21.0](https://github.com/noetl/tools/compare/v2.20.0...v2.21.0) (2026-06-07)

### Features

* **tools:** Tool::Container + ToolResult.pending_callback marker (Round 3 of [#43](https://github.com/noetl/tools/issues/43)) ([565bc03](https://github.com/noetl/tools/commit/565bc032c2baaf87a7cf28ea00cb6f9b3de1ec59)), closes [#36](https://github.com/noetl/tools/issues/36)

## [2.20.0](https://github.com/noetl/tools/compare/v2.19.3...v2.20.0) (2026-06-07)

### Features

* **tools:** add 'artifact' tool kind as get-only result_fetch alias ([3e8a57e](https://github.com/noetl/tools/commit/3e8a57e1d7d81fe88a974e23030034f889810260)), closes [noetl/tools#34](https://github.com/noetl/tools/issues/34) [noetl/ai-meta#64](https://github.com/noetl/ai-meta/issues/64) [#34](https://github.com/noetl/tools/issues/34) [noetl/ai-meta#64](https://github.com/noetl/ai-meta/issues/64)

## [2.19.3](https://github.com/noetl/tools/compare/v2.19.2...v2.19.3) (2026-06-06)

### Bug Fixes

* **python:** accept nested script.source.code (inline) shape ([d444345](https://github.com/noetl/tools/commit/d444345763a768909e2de4c577e1b62e60cc9381)), closes [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54) [noetl/ai-meta#63](https://github.com/noetl/ai-meta/issues/63) [#32](https://github.com/noetl/tools/issues/32) [noetl/ai-meta#63](https://github.com/noetl/ai-meta/issues/63)

## [2.19.2](https://github.com/noetl/tools/compare/v2.19.1...v2.19.2) (2026-06-06)

### Bug Fixes

* **secrets:** remove the standalone secrets tool + provider module ([f2591ad](https://github.com/noetl/tools/commit/f2591adf58a79a894e6ade58a2e69827c7c448b7)), closes [noetl/ai-meta#61](https://github.com/noetl/ai-meta/issues/61) [noetl/ai-meta#61](https://github.com/noetl/ai-meta/issues/61)

## [2.19.1](https://github.com/noetl/tools/compare/v2.19.0...v2.19.1) (2026-06-06)

### Bug Fixes

* **secrets:** make `secrets` the single tool kind (drop secret_manager alias) ([09a033c](https://github.com/noetl/tools/commit/09a033c69aa56d9c5093377057a86ab95061927d)), closes [noetl/ai-meta#61](https://github.com/noetl/ai-meta/issues/61)

## [2.19.0](https://github.com/noetl/tools/compare/v2.18.5...v2.19.0) (2026-06-05)

### Features

* **secrets:** add GCP Secret Manager provider behind a SecretProvider trait ([469bfc7](https://github.com/noetl/tools/commit/469bfc73db6c32c4307d22dcdc88c8cc0ed34d77)), closes [noetl/ai-meta#61](https://github.com/noetl/ai-meta/issues/61)

## [2.18.5](https://github.com/noetl/tools/compare/v2.18.4...v2.18.5) (2026-06-05)

### Bug Fixes

* **postgres,duckdb:** dollar-quote-aware statement splitter ([3bcea06](https://github.com/noetl/tools/commit/3bcea06641705e6519670dc23bc2930e7a6f809f)), closes [noetl/tools#24](https://github.com/noetl/tools/issues/24) [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54)

## [2.18.4](https://github.com/noetl/tools/compare/v2.18.3...v2.18.4) (2026-06-05)

### Bug Fixes

* **duckdb:** accept command: as alias for query: (parity with postgres) ([e10e53c](https://github.com/noetl/tools/commit/e10e53c619856e3bdfe0456453969de5e8476fa9)), closes [noetl/tools#23](https://github.com/noetl/tools/issues/23) [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54)
* **http:** coerce non-string params/headers/form values ([4d4c785](https://github.com/noetl/tools/commit/4d4c7852244390c6d9c3ae231d669b0f0cc71941)), closes [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54)

## [2.18.3](https://github.com/noetl/tools/compare/v2.18.2...v2.18.3) (2026-06-05)

### Bug Fixes

* **postgres,duckdb:** support canonical v10 SQL shapes ([40be79e](https://github.com/noetl/tools/commit/40be79e85ae3db16cd1306b5e2fe04337c51dc25)), closes [noetl/tools#23](https://github.com/noetl/tools/issues/23) [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54)

## [2.18.2](https://github.com/noetl/tools/compare/v2.18.1...v2.18.2) (2026-06-05)

### Bug Fixes

* **postgres:** surface the real SQL error instead of generic "db error" ([f3321d9](https://github.com/noetl/tools/commit/f3321d9b514ce4729e3d14ee1ddb3eca0a7fc077)), closes [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54) [noetl/tools#21](https://github.com/noetl/tools/issues/21) [noetl/ai-meta#54](https://github.com/noetl/ai-meta/issues/54)

## [2.18.1](https://github.com/noetl/tools/compare/v2.18.0...v2.18.1) (2026-06-05)

### Bug Fixes

* **task_sequence:** accept worker-envelope `tool_config` shape ([7bde2d9](https://github.com/noetl/tools/commit/7bde2d92a3fbfd6ac95c535b25aa6c074b48e691)), closes [noetl/tools#19](https://github.com/noetl/tools/issues/19)

## [2.18.0](https://github.com/noetl/tools/compare/v2.17.1...v2.18.0) (2026-06-05)

### Features

* **task_sequence:** add tool for pipeline / sequential-task execution ([faf1d0c](https://github.com/noetl/tools/commit/faf1d0ce3aca72446e8d7578f9bd36680927222f)), closes [noetl/server#63](https://github.com/noetl/server/issues/63) [#1](https://github.com/noetl/tools/issues/1) [#2](https://github.com/noetl/tools/issues/2) [noetl/tools#15](https://github.com/noetl/tools/issues/15)

## [2.17.1](https://github.com/noetl/tools/compare/v2.17.0...v2.17.1) (2026-06-05)

### Bug Fixes

* **python:** capture user code's `result` global as the tool's data field ([082a294](https://github.com/noetl/tools/commit/082a2945b26de34cb0ad108dcff8d4713b10d435)), closes [noetl/ai-meta#59](https://github.com/noetl/ai-meta/issues/59) [noetl/tools#16](https://github.com/noetl/tools/issues/16) [noetl/tools#16](https://github.com/noetl/tools/issues/16)

## [2.17.0](https://github.com/noetl/tools/compare/v2.16.0...v2.17.0) (2026-06-03)

### Features

* **nats:** add bounded js_consume operation ([7247417](https://github.com/noetl/tools/commit/7247417e47a178e3e3fa56b4a740becabb3a2166)), closes [noetl/ai-meta#52](https://github.com/noetl/ai-meta/issues/52) [noetl/ai-meta#46](https://github.com/noetl/ai-meta/issues/46) [noetl/ai-meta#52](https://github.com/noetl/ai-meta/issues/52)

## [2.16.0](https://github.com/noetl/tools/compare/v2.15.0...v2.16.0) (2026-06-02)

### Features

* **mcp:** add McpTool — MCP (Model Context Protocol) JSON-RPC bridge ([3f2e8fc](https://github.com/noetl/tools/commit/3f2e8fc79e26a8c24fc8dd656f825e270e970fc5)), closes [noetl/ai-meta#39](https://github.com/noetl/ai-meta/issues/39)

## [2.15.0](https://github.com/noetl/tools/compare/v2.14.0...v2.15.0) (2026-06-02)

### Features

* **nats:** add NatsTool — KV / Object Store / JetStream-publish tool kind ([b40dcca](https://github.com/noetl/tools/commit/b40dcca31d32605d09ae6ac26532281621520075)), closes [noetl/ai-meta#38](https://github.com/noetl/ai-meta/issues/38) [noetl/ai-meta#38](https://github.com/noetl/ai-meta/issues/38)

## [2.14.0](https://github.com/noetl/tools/compare/v2.13.0...v2.14.0) (2026-06-01)

### Features

* **result_fetch:** mTLS client identity config (R-2.3 Phase C2.4) ([7e2bd53](https://github.com/noetl/tools/commit/7e2bd532fc746036008bb0a89bb6e1768acead68)), closes [noetl/cli#47](https://github.com/noetl/cli/issues/47) [noetl/noetl#648](https://github.com/noetl/noetl/issues/648) [noetl/noetl#648](https://github.com/noetl/noetl/issues/648) [noetl/cli#47](https://github.com/noetl/cli/issues/47) [noetl/ai-meta#33](https://github.com/noetl/ai-meta/issues/33)

## [2.13.0](https://github.com/noetl/tools/compare/v2.12.0...v2.13.0) (2026-06-01)

### Features

* **result_fetch:** bearer-token + TLS-CA config (R-2.3 Phase C2.3) ([7a2896d](https://github.com/noetl/tools/commit/7a2896d64a63cc337ef26cc79253b07fad860a88)), closes [noetl/ai-meta#33](https://github.com/noetl/ai-meta/issues/33)

## [2.12.0](https://github.com/noetl/tools/compare/v2.11.0...v2.12.0) (2026-06-01)

### Features

* **template:** .result accessor proxy for cross-runtime parity ([4ee8b07](https://github.com/noetl/tools/commit/4ee8b078d224fbafd430ca511f15a3fe27f8ec43))

### Bug Fixes

* **result_fetch:** use http/https scheme for Flight endpoint (tonic compat) ([4118251](https://github.com/noetl/tools/commit/411825178ffe72645c3eb9497bd79bc467be0003))

## [2.11.0](https://github.com/noetl/tools/compare/v2.10.0...v2.11.0) (2026-06-01)

### Features

* **tools:** result_fetch tool kind — playbook-driven cross-step fetch ([df92c2a](https://github.com/noetl/tools/commit/df92c2afdf566ec5fd4f7ab6d674dbe110bc008d)), closes [noetl/cli#43](https://github.com/noetl/cli/issues/43) [noetl/ai-meta#30](https://github.com/noetl/ai-meta/issues/30)

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

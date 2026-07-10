# Changelog

All notable changes to this project will be documented in this file.

## [3.19.3](https://github.com/noetl/tools/compare/v3.19.2...v3.19.3) (2026-07-10)

### Bug Fixes

* **transfer:** coerce http->postgres params to target column types (i64->int4) ([#84](https://github.com/noetl/tools/issues/84)) ([28b7526](https://github.com/noetl/tools/commit/28b7526f4bc60983600cffc48dcc543ba6b66d65)), closes [noetl/ai-meta#183](https://github.com/noetl/ai-meta/issues/183)

## [3.19.2](https://github.com/noetl/tools/compare/v3.19.1...v3.19.2) (2026-07-09)

### Bug Fixes

* **transfer:** assemble http->postgres target connection from alias fields ([#83](https://github.com/noetl/tools/issues/83)) ([024b280](https://github.com/noetl/tools/commit/024b280c46edf33ed0bbc1ef1770787b4bc88db0))

## [3.19.1](https://github.com/noetl/tools/compare/v3.19.0...v3.19.1) (2026-06-27)

### Performance Improvements

* **subscription:** reuse NATS connection across poll/ack in NatsSource ([#79](https://github.com/noetl/tools/issues/79)) ([14d095c](https://github.com/noetl/tools/commit/14d095c0e56b33faab2766ca7bf47f2d8ce4a768)), closes [noetl/ai-meta#130](https://github.com/noetl/ai-meta/issues/130)

## [3.19.0](https://github.com/noetl/tools/compare/v3.18.1...v3.19.0) (2026-06-26)

### Features

* **container:** G1 GPU placement + G2 poll-to-terminal helper (SLM platform [#144](https://github.com/noetl/tools/issues/144)/[#145](https://github.com/noetl/tools/issues/145)) ([#82](https://github.com/noetl/tools/issues/82)) ([db865d2](https://github.com/noetl/tools/commit/db865d2f63ce307737e94b15e525b2380004cd0d))

## [3.18.1](https://github.com/noetl/tools/compare/v3.18.0...v3.18.1) (2026-06-25)

### Bug Fixes

* **playbook:** explicit payload must not be shadowed by injected empty args ([#136](https://github.com/noetl/tools/issues/136)) ([#81](https://github.com/noetl/tools/issues/81)) ([cd54568](https://github.com/noetl/tools/commit/cd54568f308bc70b3f66471e18f862b051c516b8))

## [3.18.0](https://github.com/noetl/tools/compare/v3.17.0...v3.18.0) (2026-06-25)

### Features

* **playbook:** return child playbook result data in blocking mode ([#136](https://github.com/noetl/tools/issues/136)) ([#80](https://github.com/noetl/tools/issues/80)) ([fb960f4](https://github.com/noetl/tools/commit/fb960f4b69c8a5904ccbc1206685d9737a97cc96))

## [3.17.0](https://github.com/noetl/tools/compare/v3.16.0...v3.17.0) (2026-06-23)

### Features

* **registry:** side_effecting tool classification ([#104](https://github.com/noetl/tools/issues/104) Phase E) ([#78](https://github.com/noetl/tools/issues/78)) ([41120a3](https://github.com/noetl/tools/commit/41120a30a39b9cbd0608b2c2fa59d599336a7662))

## [3.16.0](https://github.com/noetl/tools/compare/v3.15.0...v3.16.0) (2026-06-22)

### Features

* **locator:** ResultCoordinates::parse / from_locator ([#104](https://github.com/noetl/tools/issues/104) Phase B) ([740a2c2](https://github.com/noetl/tools/commit/740a2c2290be7db6dace3b94c7d640d399061506))

## [3.15.0](https://github.com/noetl/tools/compare/v3.14.2...v3.15.0) (2026-06-22)

### Features

* **locator:** extract noetl-locator slim crate ([#104](https://github.com/noetl/tools/issues/104) Phase A) ([e91be8b](https://github.com/noetl/tools/commit/e91be8bc1093d8360d30bbbfbf199d365b9f111b))

## [3.14.2](https://github.com/noetl/tools/compare/v3.14.1...v3.14.2) (2026-06-22)

### Bug Fixes

* **postgres:** serialize temporal/identity types in pg_value_to_json ([#75](https://github.com/noetl/tools/issues/75)) ([06302ac](https://github.com/noetl/tools/commit/06302ac73af3a4457de361e0fb4d26824a892f54)), closes [noetl/ai-meta#95](https://github.com/noetl/ai-meta/issues/95) [noetl/ai-meta#95](https://github.com/noetl/ai-meta/issues/95)

## [3.14.1](https://github.com/noetl/tools/compare/v3.14.0...v3.14.1) (2026-06-22)

### Performance Improvements

* **task_sequence:** reuse proxied context per sub-task to cut per-iteration CPU ([#127](https://github.com/noetl/tools/issues/127)) ([#74](https://github.com/noetl/tools/issues/74)) ([9dd9aa6](https://github.com/noetl/tools/commit/9dd9aa63fc41efac8f3c36b49428bf56d5924dcb)), closes [#124](https://github.com/noetl/tools/issues/124) [#125](https://github.com/noetl/tools/issues/125)

## [3.14.0](https://github.com/noetl/tools/compare/v3.13.1...v3.14.0) (2026-06-21)

### Features

* **task_sequence:** honor do jump / break / retry control-flow verbs ([#73](https://github.com/noetl/tools/issues/73)) ([62d0948](https://github.com/noetl/tools/commit/62d0948916c5f7e07b0c2fa3d43a6b5f9b7755f8)), closes [noetl/ai-meta#125](https://github.com/noetl/ai-meta/issues/125)

## [3.13.1](https://github.com/noetl/tools/compare/v3.13.0...v3.13.1) (2026-06-21)

### Bug Fixes

* **http:** expose parsed response body under data (Python-era contract) ([#72](https://github.com/noetl/tools/issues/72)) ([86f0216](https://github.com/noetl/tools/commit/86f0216e9e9e725e127f2c64db428ee0db784ede)), closes [noetl/ai-meta#126](https://github.com/noetl/ai-meta/issues/126)

## [3.13.0](https://github.com/noetl/tools/compare/v3.12.0...v3.13.0) (2026-06-19)

### Features

* **subscription:** deferred (ack-after-processing) ack capability ([#71](https://github.com/noetl/tools/issues/71)) ([8972ec1](https://github.com/noetl/tools/commit/8972ec1cec9540ab9cb1fea421902fd06ef5fdef)), closes [noetl/ai-meta#103](https://github.com/noetl/ai-meta/issues/103) [noetl/ai-meta#103](https://github.com/noetl/ai-meta/issues/103) [noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104)

## [3.12.0](https://github.com/noetl/tools/compare/v3.11.0...v3.12.0) (2026-06-17)

### Features

* **locator:** two-level cursor fan-out coordinate (frame, row) (v3.12.0) ([#70](https://github.com/noetl/tools/issues/70)) ([dcee629](https://github.com/noetl/tools/commit/dcee6299fefd40a88ae3af8076e67c23c76a31a1)), closes [noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104) [noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104)

## [3.11.0](https://github.com/noetl/tools/compare/v3.10.1...v3.11.0) (2026-06-17)

### Features

* **locator:** shared Resource Locator + cell/shard derivation (v3.11.0) ([#68](https://github.com/noetl/tools/issues/68)) ([230a42e](https://github.com/noetl/tools/commit/230a42e6090262566c906f31546a59ae04422753)), closes [noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104) [noetl/ai-meta#104](https://github.com/noetl/ai-meta/issues/104)

## [3.10.1](https://github.com/noetl/tools/compare/v3.10.0...v3.10.1) (2026-06-15)

### Bug Fixes

* **postgres:** skip -- line comments in the statement splitter ([#66](https://github.com/noetl/tools/issues/66)) ([7da2d38](https://github.com/noetl/tools/commit/7da2d383c0d65d3e6972dcde182effcc3c17b3de)), closes [noetl/ai-meta#100](https://github.com/noetl/ai-meta/issues/100) [noetl/ai-meta#100](https://github.com/noetl/ai-meta/issues/100)

## [3.10.0](https://github.com/noetl/tools/compare/v3.9.2...v3.10.0) (2026-06-15)

### Features

* **transfer:** Snowflake<->Postgres directions + credential-alias resolution ([#65](https://github.com/noetl/tools/issues/65)) ([8dd0d70](https://github.com/noetl/tools/commit/8dd0d7002c9ee2656a38b137faa31713a1a8473d)), closes [noetl/ai-meta#99](https://github.com/noetl/ai-meta/issues/99) [noetl/ai-meta#99](https://github.com/noetl/ai-meta/issues/99)

## [3.9.2](https://github.com/noetl/tools/compare/v3.9.1...v3.9.2) (2026-06-15)

### Bug Fixes

* **snowflake:** SQL-API session context in body; split multi-statement commands ([#64](https://github.com/noetl/tools/issues/64)) ([07d4d7d](https://github.com/noetl/tools/commit/07d4d7d9939e5a70ae79668df357f8af5ea5fd85)), closes [noetl/ai-meta#98](https://github.com/noetl/ai-meta/issues/98) [noetl/ai-meta#98](https://github.com/noetl/ai-meta/issues/98)

## [3.9.1](https://github.com/noetl/tools/compare/v3.9.0...v3.9.1) (2026-06-15)

### Bug Fixes

* **snowflake:** set User-Agent header on the HTTP client ([#63](https://github.com/noetl/tools/issues/63)) ([c0e71f3](https://github.com/noetl/tools/commit/c0e71f307d619f0dd187d5135cbb7ee05d79ed68)), closes [noetl/ai-meta#98](https://github.com/noetl/ai-meta/issues/98)

## [3.9.0](https://github.com/noetl/tools/compare/v3.8.0...v3.9.0) (2026-06-15)

### Features

* **snowflake:** key-pair (JWT) authentication ([#62](https://github.com/noetl/tools/issues/62)) ([bb72792](https://github.com/noetl/tools/commit/bb7279206ac65e33bbdca26e35c0173d6d48383a)), closes [noetl/ai-meta#98](https://github.com/noetl/ai-meta/issues/98)

## [3.8.0](https://github.com/noetl/tools/compare/v3.7.1...v3.8.0) (2026-06-12)

### Features

* extract shared noetl-directives crate (de-vendor directive engine) ([85bb725](https://github.com/noetl/tools/commit/85bb7250b75a02ab622b1c04bd6d4fb70e77a738)), closes [noetl/ai-meta#92](https://github.com/noetl/ai-meta/issues/92)

## [3.7.1](https://github.com/noetl/tools/compare/v3.7.0...v3.7.1) (2026-06-12)

### Bug Fixes

* pin time =0.3.47 to unblock async-nats build under rustc 1.92 ([a4fd883](https://github.com/noetl/tools/commit/a4fd883dee90d2a2ab99e808ce57e076e8687534)), closes [noetl/ai-meta#94](https://github.com/noetl/ai-meta/issues/94) [noetl/ai-meta#93](https://github.com/noetl/ai-meta/issues/93)

## [3.7.0](https://github.com/noetl/tools/compare/v3.6.0...v3.7.0) (2026-06-12)

### Features

* spool cross-restart recovery helpers (recv_seq high-water) ([3fbc287](https://github.com/noetl/tools/commit/3fbc2877417ba5b98b3f74da895aad1b6c15c315)), closes [noetl/ai-meta#93](https://github.com/noetl/ai-meta/issues/93)

## [3.6.0](https://github.com/noetl/tools/compare/v3.5.0...v3.6.0) (2026-06-12)

### Features

* s3 store-and-forward spool backend (SigV4, S3-compatible) ([2d4b5f1](https://github.com/noetl/tools/commit/2d4b5f1616de3f9b2585c267ab688028a6ed4608)), closes [noetl/ai-meta#94](https://github.com/noetl/ai-meta/issues/94) [noetl/ai-meta#94](https://github.com/noetl/ai-meta/issues/94)

## [3.5.0](https://github.com/noetl/tools/compare/v3.4.0...v3.5.0) (2026-06-12)

### Features

* **spool:** GCS store-and-forward backend (gcs feature) ([033b726](https://github.com/noetl/tools/commit/033b7266ece109c9f0d49d41afc421841311b3fc)), closes [noetl/tools#55](https://github.com/noetl/tools/issues/55) [noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90)

## [3.4.0](https://github.com/noetl/tools/compare/v3.3.0...v3.4.0) (2026-06-12)

### Features

* store-and-forward spool engine + per-downstream circuit breaker ([#90](https://github.com/noetl/tools/issues/90) Phase 4) ([#54](https://github.com/noetl/tools/issues/54)) ([cf70960](https://github.com/noetl/tools/commit/cf70960e8e1a8b4c210daabed14e5fffe1ef7b0f))

## [3.3.0](https://github.com/noetl/tools/compare/v3.2.0...v3.3.0) (2026-06-11)

### Features

* header-directive engine + public build_source factory ([#90](https://github.com/noetl/tools/issues/90) Phase 2) ([#52](https://github.com/noetl/tools/issues/52)) ([49ddbe4](https://github.com/noetl/tools/commit/49ddbe4528c4510f58d6ef82e653935cd54a6b39))

## [3.2.0](https://github.com/noetl/tools/compare/v3.1.1...v3.2.0) (2026-06-11)

### Features

* **subscription:** bounded-drain subscription tool + source-client abstraction ([#50](https://github.com/noetl/tools/issues/50)) ([438b1b4](https://github.com/noetl/tools/commit/438b1b4dc14b14ed6fd2669e90763c9cbd0335b6)), closes [noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90) [noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90)

## [3.1.1](https://github.com/noetl/tools/compare/v3.1.0...v3.1.1) (2026-06-11)

### Bug Fixes

* **task_sequence:** expose sibling sub-tool results to later sub-tools ([#48](https://github.com/noetl/tools/issues/48)) ([e931ca0](https://github.com/noetl/tools/commit/e931ca0e1f3f58dd997090487246b6ff67fa6a1f)), closes [noetl/ai-meta#87](https://github.com/noetl/ai-meta/issues/87)

## [3.1.0](https://github.com/noetl/tools/compare/v3.0.0...v3.1.0) (2026-06-10)

### Features

* evaluate spec.policy.rules on pipeline tool items in task_sequence ([3c3a919](https://github.com/noetl/tools/commit/3c3a9193a48a656d09cfd17d27e8fdfec177870c))
* propagate policy-rule set mutations via _context_updates in result data ([5a4ea10](https://github.com/noetl/tools/commit/5a4ea1047a0c36c27a255003a3d444f90c8e660d))

### Bug Fixes

* handle YAML boolean when: true in policy rules + tojson fallback for object templates ([8836f0f](https://github.com/noetl/tools/commit/8836f0f70b95a2a96dc0732ec32a5085a9a52351)), closes [noetl/ai-meta#69](https://github.com/noetl/ai-meta/issues/69) [noetl/ai-meta#69](https://github.com/noetl/ai-meta/issues/69)

## [3.0.0](https://github.com/noetl/tools/compare/v2.24.2...v3.0.0) (2026-06-09)

### ⚠ BREAKING CHANGES

* playbook YAML that references {{ _prev }} or
{{ _results }} in multi-tool steps must migrate to the set:/input:
pattern.

### Features

* replace _prev/_results with set:/input: forward-only data binding ([92716a5](https://github.com/noetl/tools/commit/92716a514bcdd9f60fc728629632016351757ba2)), closes [noetl/ai-meta#77](https://github.com/noetl/ai-meta/issues/77)

## [2.24.2](https://github.com/noetl/tools/compare/v2.24.1...v2.24.2) (2026-06-09)

### Bug Fixes

* resolve all clippy warnings under -D warnings gate ([524fbc4](https://github.com/noetl/tools/commit/524fbc4c335328c8c938b18925a76333b90898eb)), closes [#42](https://github.com/noetl/tools/issues/42)

## [2.24.1](https://github.com/noetl/tools/compare/v2.24.0...v2.24.1) (2026-06-08)

### Bug Fixes

* **playbook:** terminate polling on status: COMPLETED/FAILED/CANCELLED ([8e94f4b](https://github.com/noetl/tools/commit/8e94f4b73aaf28c133efd031bfb4df5dbe8900eb)), closes [noetl/ai-meta#75](https://github.com/noetl/ai-meta/issues/75)

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

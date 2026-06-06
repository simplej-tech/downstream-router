# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build commands

Single-module Gradle project (Groovy DSL). Java 21 toolchain. Version catalog at `gradle/libs.versions.toml`.

```bash
./gradlew build -x test                                     # compile + bootJar
./gradlew bootRun                                           # run the app
./gradlew integrationTest                                   # CB + backpressure IT (EmbeddedKafka + WireMock)
./gradlew test                                              # unit tests (none yet)
./gradlew build --refresh-dependencies                      # after a sibling lib republishes
```

After a change in a sibling lib: `cd ../<that-repo> && ./gradlew publishToMavenLocal && cd - && ./gradlew build --refresh-dependencies`.

Sibling deps (via mavenLocal):
- `com.example:common-configs:0.1.0` — envelope encryption (sibling repo `common-configs`)
- `com.example:bio-query-client:0.1.0` — `BioQueryClient` over ES low-level `RestClient`
- `com.example:match-client:0.1.0` — `MatchClient` over OpenAPI okhttp-gson
- `com.example:social-client:0.1.0` — `SocialClient`, same OpenAPI okhttp-gson shape

## Ecosystem position

This repo is the **standard-downstream consumer** in the 4-repo playground ecosystem:

| Repo | Role |
|---|---|
| `main-router` | router: reads `requests`, routes to `standard-downstream` or `social-express` by header |
| `downstream-router` (this) | consumer: reads `standard-downstream`, calls bio + match + social |
| `publisher-cli` | one-shot CLI test producer |
| `downstream-service` | mock HTTP backend (`/process`, `/match`, `/social`) |

Plus the sibling libs (`common-configs`, `downstream-client-playground`).

## Architecture

Single Kafka consumer that decrypts incoming messages, dispatches each to an async processor, and calls three downstream HTTP clients (bio → match → social) — each wrapped with a Resilience4j circuit breaker. When any breaker opens, a backpressure listener pauses the Kafka consumer; resumes on HALF_OPEN so probes can flow.

Consumed topic: `standard-downstream`.

### Source layout

- `com.example.downstream.StandardDownstreamApplication` — Spring Boot entry point (`@EnableKafka`).
- `com.example.downstream.config.KafkaAppConfig` — `@EnableAsync` + `AsyncConfigurer` providing `Executors.newVirtualThreadPerTaskExecutor()` as the `@Async` executor. Profile-gated `@Profile("!single-thread-async")` so the IT can substitute a deterministic single-thread executor.
- `com.example.downstream.StandardDownstreamListener` — `@KafkaListener(id="standard-downstream-processor")`, `@Transactional("kafkaTransactionManager")`. Decrypts + parses each record on the listener thread; hands the `RequestMessage` to `AsyncProcessor.processRecord(...)` and returns immediately.
- `com.example.downstream.AsyncProcessor` — `@Component`; `processRecord(RequestMessage)` is `@Async void` and calls bio → match → social on a virtual thread.
- `com.example.downstream.model.RequestMessage` — wire DTO (id, destination, payload). Inlined per repo (not shared via a lib) so each repo is standalone.
- `com.example.downstream.client.*` — CB-wrapping decorators around the three lib clients (`BioQueryClientCircuitBreaker`, `MatchClientCircuitBreaker`, `SocialClientCircuitBreaker`) and the profile-gated configs that pick CB-wrapped vs plain via the `no-cb` profile.
- `com.example.downstream.backpressure.*` — `CircuitBreakerStateListener` + `KafkaBackpressureCoordinator` pause/resume the listener container based on CB state transitions.
- `com.example.downstream.ratelimit.*` — local rate-limit wiring. `RateLimiterConfig` defines three named beans (`bioRateLimiter`, `matchRateLimiter`, `socialRateLimiter`) each profile-gated on `kafka-rate-limit-enabled`; `DownstreamRateLimiters` composes them. The wrapper interface + impls live in the `common-configs` lib (generic), the named bean choices live here (caller-specific). Each real-Guava bean is built as `new RateLimiterWrapperImpl(RateLimiter.create(rate))` — the lib's wrapper takes a fully-constructed `RateLimiter`, so the Guava factory choice (stable vs. warm-up) stays per-bean here. `AsyncProcessor` calls `rateLimiters.<service>().acquire()` immediately before each downstream HTTP call.

### Encryption pipeline

Delegated entirely to `common-configs`. The listener's value parameter type is `Result<byte[], Pair<Exception, byte[]>>`, exposing both success (decrypted plaintext) and error (exception + raw ciphertext) without throwing. `kafka.encryption.*` in `application.yml` configures KMS endpoint and cache.

### Transactional consume

`application.yml` declares `spring.kafka.producer.transaction-id-prefix=standard-downstream-tx-` so Spring Boot wires a `kafkaTransactionManager` bean; the listener's `@Transactional("kafkaTransactionManager")` binds the consumer-offset commit to that producer transaction (consume-process-produce style, even though we don't currently produce). Embedded broker tests need `transaction.state.log.replication.factor=1` + `min.isr=1` + `offsets.topic.replication.factor=1` to satisfy the single-broker transaction protocol.

### Async dispatch + delivery semantics

Because `processRecord` is `@Async void`, dispatch returns immediately and the Kafka transaction commits when `onBatch` returns — **before** the downstream HTTP work runs on the virtual thread. That makes downstream calls effectively **at-most-once**: a JVM crash between dispatch and async completion leaves the offset committed but the work undone. Acceptable for fire-and-forget side effects; not for "every message must complete."

There is no `@Retry` and no `@CircuitBreaker(fallbackMethod=...)`. When bio (or match or social) throws, the exception propagates out of `processRecord`. Spring's `AsyncUncaughtExceptionHandler` (default: log) catches it on the executor thread. The listener never sees the exception, so the transaction still commits. If you need to count or surface failures, register a custom `AsyncUncaughtExceptionHandler` bean.

A consequence: if bio fails for a message, match and social are NOT called for that message — the throw aborts the rest of `processRecord`. This differs from the older fallback-based design where bio returned a fallback value and downstream calls continued.

### Error classification (for CB recording)

- **bio** records `BioQueryException` (5xx) via yml `record-exceptions`. The lib throws `BioQueryClientErrorException` for 4xx, which is intentionally NOT recorded.
- **match** and **social** use `CircuitBreakerErrorClassifier` + per-instance `CircuitBreakerConfigCustomizer` beans: a 5xx `ApiException` or any non-`ApiException` is recorded; 4xx is ignored. The classifier unwraps cause chains and checks both the match and social generated `ApiException` types (distinct generated classes per OpenAPI module).

### Backpressure rule

Listener is **paused while any CB gating it is OPEN/FORCED_OPEN**, resumed on CLOSED and (when `resume-on-half-open=true`) HALF_OPEN. Resuming on HALF_OPEN is required — a paused consumer produces no probe calls, so the breaker would otherwise be stuck. Requires `automatic-transition-from-open-to-half-open-enabled: true` per CB instance so the timer drives OPEN→HALF_OPEN.

With async + virtual threads, the pause is requested as soon as a CB transitions, but the listener may have already polled and dispatched several more messages by then. Those dispatched tasks complete (or short-circuit at the CB pre-check) on the executor — they don't get retroactively paused. `containerPaused == true` becomes observable at the next poll attempt after pause is requested.

## Integration tests

Layout:
- `src/main/...` — production code.
- `src/testFixtures/java/com/example/downstream/it/...` — shared IT framework (WireMock stubs, response fixtures, scenario orchestrator). Reused by anyone consuming this module's testFixtures.
- `src/test/integration/java/com/example/downstream/it/...` — the integration test(s).

The IT framework has three layers:
- **`fixtures/*Responses`** — static `ok()` factories returning the lib's response DTO shape; used as WireMock response bodies.
- **`stubs/*Stubs`** — per-service stubbing + verification API: `returnsHealthy()`, `returns(<Response>)`, `failsWith(int)`, `fails5xx()`, `fails4xx()`, `verifyCalled(int)`, `verifyCalledFor(<Request>)`, `verifyNotCalled()`, `callCount()`. `DownstreamStubs` composes them.
- **`Scenarios`** — whole-system one-call setups: `allHealthy(stubs)`, `bioFails(stubs)`, `matchFails(stubs)`, `socialFails(stubs)`. Each scenario calls `stubs.resetAll()` first so per-phase counts are clean.

`TestEncryptionConfig` (in the integration source set) swaps the KMS-backed `DataKeyProvider` for a fixed-key stub so the IT runs without LocalStack.

### Single-thread async profile (test-only)

Prod uses virtual threads per task (unbounded concurrency); under that executor, call counts during a burst are non-deterministic because some tasks' CB pre-check beats the CB-OPEN state propagation. The IT activates `spring.profiles.include=single-thread-async`, which:
- profile-gates the prod `KafkaAppConfig` off (its `@Profile("!single-thread-async")` condition flips false)
- enables the nested `@TestConfiguration SingleThreadAsyncConfig` (`@Profile("single-thread-async")`) inside `StandardDownstreamProcessorCircuitBreakerIT`, which provides an `AsyncConfigurer` returning `Executors.newSingleThreadExecutor()`.

Spring permits exactly one `AsyncConfigurer` bean — the profile flip ensures only one is active at a time.

### IT phase counts

The IT publishes exactly the minimum number of messages needed to trigger each CB state transition. Going over the minimum opens a race window between the async-thread's "CB opens → pause requested" event and the listener thread's next-poll cycle — the trailing record can stay uncommitted and replay after the CB recovers, breaking the count assertions in the next phase.

Phase 1 (warm-1, all healthy): bio=1, match=1, social=1. The success lands in bio's sliding window.

Phase 2 (3 fail-N messages, bio downstream returning 5xx): window=[ok, fail, fail, fail] = 4 calls, 75% > 50% → CB OPEN on the 3rd failure. Expected: **bio=3, match=0, social=0**. With no fallback, each failing bio call throws straight out of `processRecord` — match and social never run.

Phase 3 (2 probes, all healthy): permitted-half-open=2, so probe-0 and probe-1 are both gated into the HALF_OPEN window; both succeed → CB → CLOSED on the 2nd. Expected: bio=2, match=2, social=2.

## When making changes

- **Tuning CB config**: edit `resilience4j.*` in `application.yml`; the `name` ("bio"|"match"|"social") ties the annotation to the yml instance. Error recording: yml `record-exceptions` for bio; the `CircuitBreakerConfigCustomizer` beans for match/social. There's no `resilience4j.retry.*` section — `@Retry` was removed because `@CircuitBreaker(fallbackMethod=...)` would have suppressed it anyway (Retry is the outer aspect; CB's fallback catches the exception before Retry sees it).
- **Adding a new client**: add the lib in `downstream-client-playground`, publish it, add a `libs` entry, an `api` dep here, a CB decorator + profile-gated config under `client/`, and (if OpenAPI) extend `CircuitBreakerErrorClassifier` for its `ApiException` type. Also extend `XxxStubs` + `XxxResponses` in testFixtures and `Scenarios` for the new client.
- **Awaits in ITs**: use `await(...).untilAsserted(() -> STUBS.<lastService>.verifyCalled(n))` to wait for actual processing completion. CB state assertions are not a processing-completion proxy (CB starts CLOSED, so a CLOSED assertion is trivially true).
- **CB sliding-window math crosses phases**: phase-1's success contributes to phase-2's window. Plan `verifyCalled(n)` numbers accordingly, or `circuitBreakerRegistry.circuitBreaker(name).reset()` between phases.
- **Removing the async + single-thread profile complication**: an alternative IT design publishes exactly the minimum number of messages needed to trigger each CB state transition (3 fails in phase 2, 2 probes in phase 3). With that, single vs virtual-thread executor produce the same call counts, and the profile override + nested `SingleThreadAsyncConfig` are unnecessary. Worth considering if you want the IT to exercise the prod executor too.

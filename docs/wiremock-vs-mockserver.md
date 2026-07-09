# WireMock vs MockServer — for ITs and end-to-end testing

Scope: mocking the three downstream HTTP services the `downstream-router` calls — **bio**
(`POST /process`, low-level ES `RestClient`), **match** (`POST /match`), **social** (`POST /social`),
both as OpenAPI okhttp-gson clients. Two distinct jobs, with different winners:

1. **Integration tests (ITs)** — a mock running for the duration of a JVM test, in this repo.
2. **End-to-end / deployed mock** — a long-lived mock service in a dev/test environment that QEs and
   CI drive over HTTP.

## TL;DR

| Layer | Recommendation | One-line reason |
|---|---|---|
| **ITs** | **WireMock** | Embeds in the test JVM cleanly; one stub DSL shared with the deployed mock. MockServer can't embed here and needs Docker. |
| **E2E / deployed mock** | **Either — lean WireMock** | Both deploy as a container with a baseline + live override API. WireMock keeps a single dialect across ITs *and* e2e; MockServer wins only if you want OpenAPI-spec-driven stubs. |

The deciding factor in *our* setup is **reuse + embedding**, not raw features: we already have a
WireMock-based `testFixtures` stub layer, and WireMock runs in-process where MockServer does not.

---

## Part 1 — Integration tests (this repo)

The WireMock ITs (`StandardDownstreamProcessorCircuitBreakerIT`, `RateLimitThrottlingIT`) embed a
`WireMockServer` in a static field. We built MockServer twins (`*MockServerIT`) to compare.

### WireMock (in-process `WireMockServer`)

**Advantages**
- **Embeds in the test JVM, zero infra.** `new WireMockServer(options().dynamicPort())` in a static
  field; no Docker, fast startup. Tests run anywhere (laptop, CI) with nothing extra.
- **One stubbing dialect end-to-end.** The `stubs/*` + `Scenarios` DSL (`post(urlEqualTo("/social"))
  .willReturn(...)`, `verify(...)`) is the *same* API you point at a deployed WireMock via
  `new WireMock(host, port)` — local ITs and post-deploy e2e share code.
- **Rich request journal with timestamps.** `LoggedRequest.getLoggedDate()` lets the rate-limit IT
  measure the steady-state window retrospectively, after all messages land.
- **Stateful scenarios, response templating, fault injection** all built in.

**Disadvantages**
- **`wiremock-standalone` is a shaded uber-jar** that bundles (among others) an older
  `com.networknt:json-schema-validator` whose resource bundle lacks the `crossEdits` key. On a shared
  classpath with MockServer's client this *shadows* the real bundle and crashes MockServer — we had to
  declare `com.networknt:json-schema-validator:1.0.76` **before** wiremock-standalone so the correct
  bundle wins. (Pure-WireMock setups don't hit this.)
- Bundled/relocated internals can occasionally collide with app deps (the price of the fat jar).

### MockServer (in-process `ClientAndServer`) — **does not work in this stack**

We tried to embed it the same way WireMock embeds. It can't, in this Spring Boot 3.3 JVM:

- **SLF4J binding clash.** The shaded `mockserver-netty` jar carries an *unrelocated* `org.slf4j` API
  **and** a JUL provider service file, which wins over Spring Boot's Logback. Spring's
  `LogbackLoggingSystem` then aborts context load: *"LoggerFactory is not a Logback LoggerContext."*
  `-Dslf4j.provider=…` can't fix it (two `org.slf4j` copies in one jar). Non-shaded `mockserver-netty`
  avoids the SLF4J clash but then…
- **`crossEdits` schema-validation crash.** When the MockServer client (de)serializes an expectation,
  `JsonSchemaValidator` → `com.networknt.schema.ValidatorTypeCode.<clinit>` reads an i18n key
  (`crossEdits`) that the resolved bundle lacks → `MissingResourceException`, JVM dies. This bites
  **both** the embedded server and the client.

**Net:** MockServer in-process inside Spring Boot here is not viable without deep dependency surgery.

### MockServer via Testcontainers (what the `*MockServerIT` tests actually do)

The working approach: run MockServer **out-of-process** in a `MockServerContainer`; keep only
`mockserver-client-java` in the test JVM.

**Advantages**
- **Sidesteps every embedding conflict** — the server has its own intact classpath in the container.
- **More faithful to production** — tests talk to a real MockServer over HTTP, exactly like the
  deployed e2e mock.

**Disadvantages**
- **Requires Docker** for the test run (the WireMock ITs need nothing). CI runners must provide a
  daemon; bleeding-edge daemons need `DOCKER_API_VERSION` forwarded (the `integrationTest` task does
  this when the env var is set).
- **Separate stub dialect.** MockServer's client (`request()/respond()`, `MatchType`,
  `VerificationTimes`) is not WireMock-compatible — it needed a *parallel* `it/mockserver/*` fixtures
  layer, not reuse.
- **No per-request timestamps.** `retrieveRecordedRequests` returns matched requests without receive
  times, so the rate-limit twin stamps the window **test-side** (poll the call count as it crosses
  `WARMUP` and `WARMUP+N`) instead of reading a journal. Slightly less precise; fine under the 0.8
  floor tolerance.
- Container start adds a few seconds per test class vs. WireMock's in-process millisecond start.

### IT verdict

**WireMock.** It embeds, it's fast, it needs no Docker, and its stub DSL is shared with the deployed
mock. MockServer ITs are useful as an *evaluation* (they pass via Testcontainers), but as the default
IT mock they add a Docker dependency, a second stub dialect, and lost journal precision for no
behavioural gain.

---

## Part 2 — End-to-end / deployed mock

Here the mock is a standing service in a dev/test environment, fronting bio/match/social on one host
(path-disambiguated), with a baked happy-path baseline and an HTTP API for QEs/CI to flip responses
on the fly. Both tools do this well; the differences are in ergonomics.

| Concern | WireMock | MockServer |
|---|---|---|
| Container image | `wiremock/wiremock` | `mockserver/mockserver` |
| Default port | 8080 | **1080** |
| Live control API | `POST /__admin/mappings` | `PUT /mockserver/expectation` |
| Baseline at rest | mappings files baked in image | init JSON via `MOCKSERVER_INITIALIZATION_JSON_PATH` (+ `WATCH` to hot-reload) |
| **Override semantics** | add a stub; **most-recent / lower-priority wins** (baseline at priority 10, override at default 5 wins) | add an expectation; **higher priority wins**, and equal priority = **insertion order (baseline added first wins)** → overrides **must** set a higher priority |
| **Reset to baseline** | `POST /__admin/mappings/reset` **reloads the baked files** → baseline back automatically | `PUT /mockserver/reset` clears to **empty**; baseline only via file-watch/re-init |
| Request verification | `GET /__admin/requests` journal (with timestamps) | `PUT /mockserver/retrieve` / `/verify` |
| Health check for an ALB | built-in `GET /__admin/health` | none — bake a `GET /health → 200` expectation |
| OpenAPI-driven stubs | community tooling, not native | **native**: generate expectations from an OpenAPI/Swagger spec |
| Stateful sequences | scenarios | expectation `times` + priority |

**Where MockServer is genuinely better:** native OpenAPI-spec mocking. Since `match` and `social` are
OpenAPI okhttp-gson clients, you could point MockServer at their specs and get spec-conformant mocks
for free — WireMock has no first-class equivalent.

**Where WireMock is better for *us*:**
- **One dialect across ITs and e2e.** The same `BioStubs`/`MatchStubs`/`SocialStubs` (with a one-line
  change to wrap a remote `WireMock` client) drive both the in-process ITs and the deployed server.
  MockServer forces a second dialect.
- **Cleaner "snap back to green."** `reset` reloads the baked baseline; MockServer's reset empties and
  relies on file-watch/re-init.
- **More intuitive override model** — newest stub wins without having to manage priority numbers.

### E2E verdict

**Either works; lean WireMock** to keep a single mock dialect across both testing layers. Choose
MockServer only if **OpenAPI-spec-driven mocking** is a hard requirement — and if you do, accept that
the ITs and the deployed mock then use different stub APIs.

---

## Decision summary

- **ITs → WireMock** (embeds, no Docker, shared DSL). Keep the MockServer twins as a parity reference,
  not the default.
- **E2E deployed mock → WireMock** for one-dialect consistency, unless **OpenAPI-spec mocking** tips
  you to MockServer.
- **If you adopt MockServer anywhere**, budget for: Docker in the IT pipeline, a separate stub
  dialect, the `crossEdits`/SLF4J embedding constraints (always run it out-of-process), port 1080,
  higher-priority overrides, and a baked health endpoint.

## Appendix — concrete artifacts

In this repo:
- WireMock ITs: `src/test/integration/.../it/{StandardDownstreamProcessorCircuitBreakerIT,RateLimitThrottlingIT}.java`
- WireMock fixtures: `src/testFixtures/.../it/{stubs,fixtures,Scenarios}`

> The repo previously also carried MockServer IT twins (`*MockServerIT` + `it/mockserver/*` fixtures,
> run out-of-process via Testcontainers) as a parity evaluation. They were removed — the ITs now use
> WireMock only. The comparison below is retained as the rationale for that choice.

Deployed-mock artifacts (live outside this repo):
- Deployed WireMock: its own repo — [`simplej-tech/downstream-api-mock`](https://github.com/simplej-tech/downstream-api-mock) (Dockerfile, baked mappings, reusable Terraform module + a dev environment, Bruno collection, QE runbook)
- Deployed MockServer (minimal): `deploy/mockserver/minimal/` (Dockerfile + init JSON, a playground-root scratch artifact)

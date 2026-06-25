# WireMock vs MockServer — for integration tests and end-to-end testing

A practical comparison for teams choosing an HTTP mock to stand in for downstream services. There are
**two distinct jobs**, and they don't have the same winner:

1. **Integration tests (ITs)** — a mock that lives for the duration of a JVM test, exercising your
   service-under-test against faked downstream HTTP responses.
2. **End-to-end / deployed mock** — a long-lived mock *service* in a dev/test environment that people
   (QEs) and CI drive over HTTP, flipping responses on the fly after deployment.

## TL;DR

| Layer | Recommendation | One-line reason |
|---|---|---|
| **ITs** (JVM tests) | **WireMock** | Embeds in the test JVM cleanly; if you also deploy a mock, the same stub DSL drives both. MockServer is hard to embed in a modern app JVM and effectively needs Docker. |
| **E2E / deployed mock** | **Either — lean WireMock** | Both deploy as a container with a baked baseline + a live override API. WireMock lets you keep one stub dialect across ITs *and* e2e; MockServer's standout reason to choose it is native OpenAPI-spec-driven stubbing. |

The usual deciding factors aren't raw feature counts — they're **embeddability** (can it run in your
test JVM?) and **reuse** (can one stub dialect serve both ITs and the deployed mock?).

---

## Part 1 — Integration tests

The common pattern is to start an in-process mock server in a static field, point the
service-under-test's downstream base URLs at it, stub responses, exercise the flow, and verify the
recorded calls.

### WireMock (in-process server)

**Advantages**
- **Embeds in the test JVM, zero extra infrastructure.** Start it on a dynamic port in a static
  field; no Docker, millisecond startup. Tests run anywhere (laptops, CI) with nothing extra.
- **One stubbing dialect end-to-end.** WireMock's Java DSL (`stubFor(post(...).willReturn(...))`,
  `verify(...)`) can target an in-process server *or* a remote one (`new WireMock(host, port)`). The
  same stub/verification code drives both your ITs and a deployed WireMock — no second dialect.
- **Rich request journal with timestamps.** Recorded requests carry receive times, so a
  throughput/latency assertion can reconstruct timing after the fact from the journal.
- **Stateful scenarios, response templating, and fault/latency injection** are built in.

**Disadvantages / gotchas**
- The **`wiremock-standalone` fat jar bundles (shaded) third-party libraries**. On a classpath that
  also contains those libraries at other versions, the bundled resources can *shadow* the real ones
  and break the other library. (We saw exactly this when WireMock-standalone and a MockServer client
  shared a classpath — see the cross-tool note below.) The non-standalone WireMock artifact pulls
  normal transitive deps instead, trading the shadowing risk for ordinary version-alignment work.
- General fat-jar caveat: relocated internals can occasionally collide with application dependencies.

### MockServer (in-process)

Embedding MockServer the same way (its all-in-one server type in a static field) is frequently **not
viable inside a modern framework JVM** (e.g. a current Spring Boot app). Two conflicts show up:

- **Logging-binding clash.** MockServer's all-in-one jar can ship an SLF4J provider (routing to JUL)
  that competes with the application's logging backend (e.g. Logback). The framework's logging-system
  initialization then fails because the bound logger factory isn't the one it expects. Forcing the
  SLF4J provider via a system property does not reliably fix it when two copies of the SLF4J API are
  present in one jar.
- **JSON-schema-validation crash.** When the MockServer client (de)serializes an expectation, its
  internal JSON-schema validator initializes a validation library that reads i18n message keys from a
  resource bundle. A version mismatch between that library's classes and the resolved message bundle
  throws a `MissingResourceException` and can take down the JVM. This affects both the embedded server
  and the client.

**Net:** plan to run MockServer **out-of-process**, not embedded.

### MockServer via a container (the workable IT approach)

Run MockServer in a container (e.g. via a container-orchestration test library), and keep only the
**lightweight MockServer client** in the test JVM.

**Advantages**
- **Sidesteps the embedding conflicts** — the server runs with its own intact classpath in the
  container, isolated from the application's dependencies.
- **More production-faithful** — the test talks to a real MockServer over HTTP, exactly like a
  deployed mock would be used.

**Disadvantages**
- **Requires a container runtime (Docker)** for the test run. In-process WireMock needs nothing. CI
  runners must provide a daemon, and very new daemons may require pinning the client API version.
- **A second stub dialect.** MockServer's client API (`request()/respond()`, its matchers and
  verification types) is not WireMock-compatible, so you maintain a separate stubbing layer from any
  WireMock-based tests.
- **No per-request timestamps** from the recorded-request API, so timing-sensitive assertions must be
  stamped test-side (e.g. poll the call count as it crosses thresholds) rather than read from a
  journal. Slightly less precise.
- Container startup adds seconds per test class vs. in-process millisecond startup.

### IT verdict

**WireMock**, unless you have a specific reason not to: it embeds, it's fast, it needs no container,
and it can share one stub dialect with a deployed mock. MockServer ITs are perfectly workable via a
container, but they add a Docker dependency, a second dialect, and reduced timing precision.

---

## Part 2 — End-to-end / deployed mock

Here the mock is a standing service that fronts one or more downstream endpoints (path-disambiguated
on a single host), with a baked happy-path baseline and an HTTP API so QEs/CI can change responses on
the fly without redeploying. Both tools handle this well; the differences are ergonomic.

| Concern | WireMock | MockServer |
|---|---|---|
| Container image | `wiremock/wiremock` | `mockserver/mockserver` |
| Default port | 8080 | **1080** |
| Live control API | `POST /__admin/mappings` | `PUT /mockserver/expectation` |
| Baseline at rest | mappings files baked into the image | initialization JSON via an env var (with an option to hot-reload on file change) |
| **Override semantics** | add a stub; **most-recently-added / lower-priority-number wins** (so a baseline at a low priority is overridden by a default-priority stub) | add an expectation; **higher priority wins**, and for equal priority the **first added wins** → overrides **must** set a higher priority than the baseline |
| **Reset to baseline** | a mappings-reset **reloads the baked files**, so the baseline returns automatically | reset clears expectations to **empty**; the baseline only returns via file-watch reload or re-initialization |
| Request verification | journal endpoint, **with timestamps** | retrieve/verify endpoints |
| Built-in health endpoint | yes (`GET /__admin/health`) | no — bake a `GET /health → 200` expectation for load-balancer checks |
| OpenAPI-driven stubs | community tooling, not first-class | **native**: generate expectations directly from an OpenAPI/Swagger spec |
| Stateful sequences | scenarios | expectation `times` + priority |

**Where MockServer is genuinely better:** native **OpenAPI-spec mocking**. If your downstreams have
OpenAPI/Swagger specs, MockServer can produce spec-conformant mocks from them with no hand-written
stubs — WireMock has no first-class equivalent.

**Where WireMock tends to win:**
- **One dialect across ITs and the deployed mock** — the same stub/verify code (pointed at a remote
  server) drives both. MockServer forces a separate dialect from WireMock-based ITs.
- **Cleaner "snap back to green"** — reset reloads the baked baseline; MockServer's reset empties and
  relies on file-watch/re-init.
- **More intuitive override model** — newest stub wins, no priority bookkeeping.

### E2E verdict

**Either works; lean WireMock** to keep a single mock dialect across both testing layers. Choose
MockServer when **OpenAPI-spec-driven mocking** is a hard requirement — accepting that your ITs and
deployed mock then speak different stub APIs.

---

## Cross-tool gotcha: don't put both clients on one classpath casually

If a single test module ends up with **both** the WireMock standalone jar and the MockServer client
(e.g. while evaluating a migration), watch for **shaded-dependency shadowing**: WireMock-standalone
bundles its own copy of libraries MockServer also uses, and whichever resource is found first on the
classpath wins. The symptom is a confusing failure in *one* tool caused by the *other's* bundled
resource. The fix is usually to control classpath ordering (declare the authoritative dependency
first) or to isolate the two tools into separate test source sets.

## Decision summary

- **ITs → WireMock** (embeds, no Docker, shareable DSL). Use MockServer-in-a-container only if you
  specifically need MockServer there.
- **E2E deployed mock → WireMock** for one-dialect consistency, unless **OpenAPI-spec mocking** tips
  you to MockServer.
- **If you adopt MockServer anywhere, budget for:** a container runtime in the test pipeline; a
  separate stub dialect; running it **out-of-process** (don't embed it in the app JVM); port 1080;
  higher-priority overrides; an explicitly baked health endpoint; and timing assertions stamped
  test-side rather than read from a journal.

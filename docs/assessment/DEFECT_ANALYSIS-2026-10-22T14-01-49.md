# Code & Defect Analysis: veredium (veredium-core, veredium-restclient, veredium-webclient)

1. Identification of the codebase
   - **Repository:** `https://github.com/Inqudium/veredium.git`
   - **Commit hash:** `1b72625602728c0ed578289986fc99e512a7c073` (full)
   - **Reference (branch/tag):** `refs/heads/main` / tag: keiner (`git describe` liefert nur den Hash); Arbeitsbaum sauber
2. Scope of the analysis
   - **Included:** `./veredium-core/src/main/kotlin/`, `./veredium-restclient/src/main/kotlin/`, `./veredium-webclient/src/main/kotlin/` (Produktionscode, 38 Dateien); **Testcode ist Teil der Analyse**: `./veredium-*/src/test/kotlin/`, `./veredium-*/src/test/java/` (Jazzer-Targets) und `./veredium-*/src/test/resources/`. Schwerpunkt ist der seit dem Vorbericht neue Code: die wählbaren HTTP-Engines aus Commit `1b72625602728c0ed578289986fc99e512a7c073` (`*EngineTransport*.kt`, `*Engine.kt`, `WebClientEngineChecks.kt`, die geänderten Factories) und die Behebungen aus `d011e40` (Vorbericht-Befunde, hier nachgeprüft). Ergänzend `./pom.xml`, `./veredium-*/pom.xml`, die Modul-Doku unter `./veredium-*/docs/` und `./CONTRIBUTING.md` als Konventionsquelle.
   - **Excluded:** generierte Artefakte (`./**/target/`), das Build-only-Modul `./veredium-coverage/` (kein Code), die MkDocs-Site (`./docs/` außer `./docs/adr/` und `./docs/assessment/`), `./.github/`
3. Analysis environment & tools
   - **Target environment:** Java 21 (Bytecode-Ziel), Build-JDK 24+ (lokal Oracle JDK 26.0.1); Kotlin 2.4.x; Spring Boot 4.1 / spring-web 7.0.9; Resilience4j 2.4.0; Engines: Apache HttpClient 5.6.4 / HttpCore 5.4.3, Jetty Client 12.1.12, Reactor Netty 1.3.7 (reactor-pool 1.2.7), `java.net.http`; legatium 1.2.1
   - **Build system:** Maven 3.9.15, Multi-Modul-Reaktor
   - **Analysis tools used:** manuelles Review (Claude Code, Opus 5.5) des Produktionscodes; zwei Sub-Agenten für die Testsuiten von `veredium-restclient` und `veredium-webclient`, deren Befunde einzeln gegengelesen wurden; `grep`; Abgleich mit den Quellen von httpclient5/httpcore5, reactor-netty-core, reactor-pool, jetty-client und spring-web aus dem lokalen Maven-Repository; ein `mvn -q -o clean verify` (grün, 573 Tests) samt JaCoCo-Aggregatbericht. Vorhanden sind ktlint, CodeQL (CI), JaCoCo und Jazzer; nicht vorhanden sind detekt, SpotBugs/Error Prone, ArchUnit und Sonar.
4. Placement & Output
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/veredium` (absoluter Bezugspunkt; alle relativen Pfade beziehen sich darauf)
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-22T14-01-49.md` (relativ zum workdir; Präfix + ISO-8601-Zeitstempel in UTC wie beim Vorbericht). Der Vorbericht `./docs/assessment/DEFECT_ANALYSIS-2026-10-21T20-14-02.md` bleibt unverändert.
   - **Scope root (relative to the workdir):** `./` (Modulwurzeln `./veredium-core/`, `./veredium-restclient/`, `./veredium-webclient/`)
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>`; das gemeinsame Präfix `src/main/kotlin/eu/inqudium/veredium/<modul>/` bzw. `src/test/kotlin/eu/inqudium/veredium/<modul>/` wird in den Befunden **ausgeschrieben**

---

## 1. Executive Summary

Der Kern (blockierender Decorator, Leiter-Ableitung, Breaker-Verdicts) ist weiterhin solide. Von den 25 Befunden des
Vorberichts sind 24 gegen den aktuellen Code als behoben bestätigt. Offen ist nur Nr. 16, das legatium betrifft
(legatium#88). **Es gibt keinen Critical- und keinen High-Befund.**

Das Risiko liegt fast vollständig in der neuen Engine-Auswahl. Das ist viel Code: rund 1,7 k Zeilen Produktionscode
für vier Engines je Stack. Er ist sorgfältig gebaut, ruht aber an mehreren Stellen auf Annahmen über das Verhalten
von Drittbibliotheken, und eine davon ist falsch:

- **Apache verwirft den Rest eines zu großen Bodys nicht, es liest ihn zu Ende** (Befund 1). Betroffen ist die
  Default-Engine des blockierenden Stacks: Der Worker liest beim Schließen den ganzen Rest von der Leitung. Ist
  der Rest groß oder langsam, läuft der TimeLimiter ab. Der Aufrufer bekommt dann eine `TimeoutException`, die der
  Breaker als Fehler zählt, statt der `ResponseBodyLimitExceededException`, die er bewusst nicht zählt. Die KDocs
  der Jetty- und Reactor-Netty-Transports nehmen ausdrücklich das Gegenteil an.
- **`pool.max-idle-time: 0` bedeutet je Engine etwas anderes** (Befund 2). Bei Reactor Netty schaltet der Wert
  Keep-Alive faktisch ab, bei Jetty heißt er „nie schließen“. Die Bind-Validierung lässt die 0 zu.
- **Der Shutdown der RestClient-Factory ist pro Engine nicht abgeschottet** (Befund 3). Wirft ein Engine-Shutdown,
  bleiben die folgenden Engines offen, und die Exception verlässt den `@PreDestroy`. Der WebClient-Zwilling ist
  hier abgeschottet, und seine KDoc lehnt genau das `block(Duration)` ab, das der RestClient-Reactor-Transport
  verwendet.

Die Testsuite deckt für jede Engine den Happy Path, das Status-Mapping, den Response-Timeout, das Abbrechen eines
Exchanges und die Isolation gegen Boots globalen Schalter ab. Die übrigen Zusagen der Startup-Zeilen („applies
max-connections …“) sind dagegen nur auf der Default-Engine oder gar nicht belegt. Für die Isolation stützen sich
die Tests teils auf die eigene Buchführung der Factory statt auf beobachtbares Verhalten.

**Test-Urteil (kompakt):**

- **Verlässlichkeit:** Für Decorator, Verdicts, Leiter und Bounded Reader ist die Suite ein echtes Netz. Die
  Vorbericht-Lücken sind geschlossen (Margin-Sweep, reaktiver Operator-Order, Trickle-Streams, Jazzer), und es gibt
  kein `Thread.sleep` und keine Mocks. Für die Engines erzeugt sie teilweise falsche Sicherheit:
  - Pool- und Lebenszyklus-Knöpfe sind auf den Nicht-Default-Engines unbelegt (Befund 4).
  - Der Shutdown mit laufenden Exchanges ist nur auf Apache belegt (Befund 5).
  - Isolations-Assertions lesen die `connectors`-Map der Factory (Befund 6).
  - Strukturell ist alles deterministisch testbar, Uhren sind injizierbar.
- **Pyramide:** Der Core hat eine gesunde, schnelle Unit-Basis (195 Tests, ≈4 s). Die Client-Module sind
  integrationslastig (199 bzw. 179 Tests, ≈25 s je Modul, echtes Loopback-HTTP über WireMock bzw. `ServerSocket`,
  über `@EnumSource` mit vier Engines multipliziert). Für Transport-Verdrahtung ist das angemessen. Die
  Aggregat-Coverage ist hoch (Instruction 95,9 %, Line 97,3 %, Branch 86,1 %). Die Branch-Lücken sitzen genau in
  den Engine-Transports (50–70 %).
- **Gravierendste Lücken:**
  - Ein großer oder tröpfelnder Body über dem Limit wird auf keiner Engine getestet. Deshalb blieb Befund 1
    unentdeckt (Befund 8).
  - `max-connections`, `pending-acquire-timeout`, `connect-timeout` und die Pool-Lebensdauer sind auf
    Jetty, Reactor Netty, JDK und dem reaktiven Apache ungetestet (Befund 4).
  - Drain, Deadline-Ablauf und unterbrochener Shutdown sind nur auf Apache belegt (Befund 5).
  - Isolations-Assertions prüfen die eigene Buchführung der Factory (Befund 6).
  - Response-Timeouts auf geteilten Engine-Clients werden nur mit einem Endpoint geprüft (Befund 7).

## 2. Scope & Methodology

- **Analysiert:** alle drei Module, Produktions- **und** Testcode. **Testcode war im Scope**, als eigener
  Analysegegenstand (Mutations-Gedankenexperiment) und als Risikoindikator. Den Produktionscode habe ich
  vollständig selbst gelesen, mit Schwerpunkt auf dem Delta `0faa7dc..1b72625` (25 Dateien, +2,9 k Zeilen). Die
  Testsuiten der zwei Client-Module haben zwei Sub-Agenten gelesen. Ihre Befunde wurden an den genannten Zeilen
  stichprobenartig nachgeprüft und nach Phase-3-Kriterien neu gewichtet; ein Agenten-Befund (Jetty-Queue) wurde
  nach Prüfung der Jetty-Quellen von Medium auf Low herabgestuft (Befund 12).
- **Erkannter Stack:** Spring Boot 4.1 als Bibliothek, ohne Controller und ohne `src/main/resources`.
  - Blockierend: Spring `RestClient` über vier wählbare `ClientHttpRequestFactory`s (Apache 5 classic per
    Default, JDK, Jetty 12, Reactor Netty).
  - Reaktiv: WebFlux-`WebClient` über vier `ClientHttpConnector`s (Reactor Netty per Default, JDK, Jetty, Apache
    5 async).
  - Virtual Threads: als Worker des Decorators, als Executor des JDK-Clients und als Body-Hand-off des
    Reactor-Netty-Transports.
  - Coroutines gibt es nur auf Aufruferseite. Persistenz und Messaging gibt es nicht.
  - Einstiegspunkte: `@PreDestroy` in den Factories und im Decorator, `SmartInitializingSingleton`
    (Startup-Guard), `@EventListener(ApplicationReadyEvent)` (Leiter-Logger), `BeanRegistrar` (Customizer).
- **Statische Analyse:** ktlint (Stil) und CodeQL (Sicherheit). Einen Defekt-Linter gibt es nicht.
- **Lokale Verifikation:** `mvn -q -o clean verify` ist grün (core 195, restclient 199, webclient 179 Tests, 0
  Fehler).
  - Die Behauptungen über Drittbibliotheken sind an deren Quellen nachgelesen:
    - httpclient5 `ResponseEntityProxy.streamClosed` und httpcore5 `ContentLengthInputStream.close` /
      `ChunkedInputStream.close`
    - reactor-netty `PooledConnectionProvider.newPoolInternal` (Eviction-Prädikat)
    - reactor-pool `PoolConfig.generateMaxLifeTimeMs`
    - jetty-client `HttpDestination.send` / `enqueue`
    - spring-web `JettyClientHttpRequest` / `JettyClientHttpRequestFactory` / `HttpComponentsClientHttpResponse.close`
  - **Reproduktionstests wurden nicht geschrieben**, weil die Freigabe laut Prompt aussteht. Befunde 1 und 2 sind
    mit Ausführungsskizzen am Bibliotheksquellcode belegt.
- **Vorbericht:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-21T20-14-02.md`, Befund für Befund gegengeprüft.
  - Produktion: 3 (`recordExceptions()`/`ignoreExceptions()` geleert, `overridden`-Diagnose), 4 (WARN
    `warnIfBulkheadsOvercommitThePool`), 5 (`EmptyResponseBodyException` auf beiden Stacks), 6
    (`ClientCallLog.withMdc`), 12, 13 (`@Throws`), 14 (Margin- und Transport-Grenzen) und 15 (Bounded Reader ohne
    Zwischenkopien) sind behoben.
  - Tests: 1, 2, 7, 8, 9, 10, 11, 17, 18, 19, 20, 21, 22, 23, 24 und 25 sind behoben bzw. nachgezogen.
  - 16 bleibt offen (hier Befund 16).
- **Blinde Flecken:**
  - Es gab keine Messung unter Last.
  - legatium wurde nur über Doku und Tests beurteilt.
  - Das Verhalten der Engines bei HTTPS/TLS liegt außerhalb des Scopes: hinter dem Sidecar ist alles Loopback-HTTP.
  - Die Zählungen der Testmuster stammen von den Sub-Agenten, per `grep`, mit den dort genannten Ausdrücken.

## 3. Statistics

| Severity | Anzahl |
|---|---|
| 🔴 Critical | 0 |
| 🟠 High | 0 |
| 🟡 Medium | 8 |
| 🟢 Low | 16 |
| **Summe** | **24** |
| Systemische Muster | 4 |

Kategorien: test quality 13, resources 4, robustness 3, correctness 2, maintainability 2.

## Bearbeitungsstand (2026-10-22, nach der Analyse)

23 von 24 Befunden sind behoben (abgehakt), auf Branch `fix/defect-analysis-2026-10-22` über Commit
`1b72625602728c0ed578289986fc99e512a7c073`, noch nicht committet. Ein `mvn -o clean verify` über alle Module ist
grün: 640 Tests (vorher 573: core 196, restclient 233, webclient 210 + 1 im eigenen JDK-Flag-Lauf), 0 Fehler,
ktlint sauber, Dokka mit `failOnWarning` ohne Warnung. Die neuen Engine-Testklassen liefen je dreimal
hintereinander grün. Die Aggregat-Coverage steigt auf 96,2 % Instruction / 87,2 % Branch / 97,7 % Line. Die
Änderungen stehen in `./CHANGELOG.md` unter „Fixed“ und „Changed“.

Belegt per Gegenprobe:

- **1** — Mit dem alten Apache-Request-Factory braucht der neue Tröpfel-Test 30,3 s und endet am TimeLimiter
  (`TimeoutException`) statt mit `ResponseBodyLimitExceededException`; mit dem Fix unter 2 s.
- **6** — Entfernt man `.requestFactory(requestFactory)` am Klon, werden 4 von 5 Isolations-Tests rot (vorher
  blieb der erste Fall grün).

Offen:

- **16** — Liegt in legatium (Inqudium/legatium#88) und ist hier nicht behebbar. Die neuen Abbruchtypen
  (`InterruptedIOException` des Reactor-Hand-offs, `AsynchronousCloseException` von Jetty) sind dort mit
  aufzunehmen.

Bei der Behebung zusätzlich gefunden und behoben:

- **Die JDK-Engine des RestClients drainiert ebenfalls.** Spring's `JdkClientHttpResponse.close()` liest den Rest
  mit `StreamUtils.drain` bis zur Read-Deadline (im Test 8,4 s). Sie nutzt jetzt denselben Abbruch-Decorator wie
  Apache und Jetty (`AbortingRequest`/`AbortingResponse` in `EngineTransport.kt`).
- **Jettys `stop()` blockiert auf einem unterbrochenen Shutdown-Thread** länger als 2 s (RestClient). Der Stop
  läuft in diesem Fall auf einem eigenen Thread zu Ende, wie Reactor Nettys Disposal.
- **Der reaktive Apache-Shutdown hing mit laufendem Exchange:** `close(IMMEDIATE)` schloss die Verbindung, ohne
  den Callback abzuschließen, und der Aufrufer wartete bis zum TimeLimiter. Laufende Exchanges werden jetzt beim
  Release abgebrochen (WARN mit Anzahl).
- **Reactor Netty (WebClient) teilt keine Verbindungen zwischen Backends:** ein Sub-Pool pro Remote-Adresse und
  Client-Konfiguration, und jedes Backend hat einen eigenen `HttpClient`. `max-connections` gilt dort also pro
  Backend; Doku und Startup-Zeile sind korrigiert, und `connectionPools()` meldet dem Guard einen Pool pro
  Backend. Der Reactor-Netty-Transport des RestClients teilt einen `HttpClient` und damit einen Pool; der neue
  `max-connections`-Test belegt das.
- **Der reaktive Apache wendet `pending-acquire-timeout-ms` nicht an:** Der asynchrone Pool prüft die Deadline
  eines wartenden Lease nur beim nächsten Lease/Release. Die Startup-Zeile führt die Einstellung jetzt unter
  „does NOT apply“; das klassische Apache im RestClient ist nicht betroffen (sein Test ist grün).

Abweichend von der Fix-Strategie:

- **12** — `requireJettyQueue` lehnt `pending-acquire-max-count: 0` weiter ab. Jetty hält jeden Request in der
  Queue, bis er auf einer Verbindung ist, kann „niemand wartet“ also nicht ausdrücken.
- **13** — Statt die Prüfung in die Factories zu verlagern, meldet jede Factory ihre Pools über die neue
  Core-Schnittstelle `MeshConnectionPools`. Der Guard prüft je Pool und überspringt Pools ohne Grenze (JDK); ohne
  Factory-Bean bleibt die alte Ein-Pool-Prüfung.

## 4. File-Ranking (Phase 1)

**Produktionscode**

| Datei | Score | Begründung |
|---|---|---|
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt` | 5 | Body-Hand-off über Virtual Threads, Interrupt-Übersetzung, eigener Meter-Registrar, Dispose mit `block` |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/JettyEngineTransport.kt` | 5 | ThreadLocal-Capture des Jetty-Requests, Abort-Decorator, Drain |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt` | 5 | Vier Engines, Apache-Future-Capture mit Race, Release-Pfade |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt` | 5 | Hot Path; Exchange, Bounded Read, async Fortsetzung |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecorator.kt` | 5 | Thread-Hand-off, Interrupt/Cancel (unverändert gut) |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt` | 4 | Engine-Auswahl, eager Erzeugung, Shutdown über alle Engines |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt` | 4 | lazy Engines, Drain, JVM-weiter Hook |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ApacheEngineTransport.kt`, `JdkEngineTransport.kt`, `EngineTransport.kt` | 4 | Pool, Evictor, Drain-Schleife, Host-Konflikt |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClient.kt` | 4 | Reaktive Kette, `onErrorDropped`-Hook, MDC-Snapshot |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceCustomizers.kt`, `MeshResilienceStartupGuard.kt`, `BreakerVerdicts.kt` | 4 | Prädikat-Komposition, Fail-fast |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/ResolvedTimeouts.kt` | 4 | Leiter-Ableitung (jetzt gut abgesichert) |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReader.kt` | 4 | Peer-kontrollierte Grenze (fuzzed) |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineChecks.kt` | 3 | Fail-fast-Prüfungen, Startup-Text |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/client/ClientCallLog.kt`, `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/MeshProperties.kt` | 3 | Level-Politik, MDC-Tausch, Bind-Guards |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/RestClientProperties.kt`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientProperties.kt` | 2 | Bind-Guards (Befund 2) |
| `./veredium-*/src/main/kotlin/**/*Engine.kt`, `*Verdicts.kt`, `*Configuration.kt`, `*ObservationReport.kt`, `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/{BaseAdapter,Veredium}.kt`, `validation/*`, `observation/*`, `client/{ResponseBodyMetrics,EmptyResponseBodyException}.kt` | 1–2 | Enums, deklarativ, Einzeiler |

**Testcode** (Gewicht als Sicherheitsnachweis)

| Datei | Score | Begründung |
|---|---|---|
| `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt` | 5 | Einziger Nachweis der drei Nicht-Default-Engines; Lücken in Befunden 4, 5, 8, 19, 20, 21 |
| `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt` | 5 | Dito reaktiv; Befunde 4, 5, 7, 14, 24 |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecoratorTest.kt` | 5 | Echte Konkurrenz über Latches; keine neuen Befunde |
| `./veredium-*/src/test/kotlin/**/*EngineIsolationTest.kt` | 4 | Isolation gegen Boot; Beweis über eigene Buchführung (Befund 6) |
| `./veredium-*/src/test/kotlin/**/Resilient*IntegrationTest.kt` | 4 | Breite Real-HTTP-Abdeckung, Shutdown-Drain nur auf Apache bzw. Reactor Netty |
| `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ReactiveResilienceDecoratorTest.kt`, `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/mesh/MeshPropertiesDerivationTest.kt` | 4 | Vorbericht-Lücken geschlossen |
| `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineConfigTest.kt`, `JdkEngineRestrictedHostHeaderTest.kt`, `./veredium-*/src/test/kotlin/**/*PropertiesTest.kt` | 3 | Fail-fast und Bindung; Befunde 18, 22 |
| `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReaderTest.kt`, `./veredium-*/src/test/java/**/*FuzzTest.java` | 3 | Trickle-Streams, Jazzer-Invarianten |
| übrige Core-Unit-Tests, `*LoggingLevelContractTest.kt`, `*ObservationReportTest.kt` | 2 | Pinnen Konfiguration bzw. Startup-Text |

## 5. Findings

### 🔴 Critical

Keine.

### 🟠 High

Keine.

### 🟡 Medium

- [x] 1. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt:457` — bis `:461`, mit `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReader.kt:53-56`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/JettyEngineTransport.kt:55`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt:65-66`] {Medium} {Confidence: high (Mechanik) / medium (Häufigkeit)} {correctness / resources}
  - Ein Erfolgs-Body über `max-in-memory-size` wird auf der Apache-Engine, dem Default, beim Schließen **zu Ende
    gelesen**, nicht verworfen.
    - `BoundedBodyReader` hört nach Limit+1 Byte auf, und `exchange(..., close = true)` schließt danach die
      Antwort. Spring ruft `HttpComponentsClientHttpResponse.close()` → `EntityUtils.consume` auf.
    - Apaches `ResponseEntityProxy.streamClosed` schließt den Stream mit dem Kommentar „this assumes that closing
      the stream will consume the remainder“. `ContentLengthInputStream.close()` bzw. `ChunkedInputStream.close()`
      lesen dann den Rest in einer Schleife (httpcore5 5.4.3).
    - Die KDocs der beiden anderen Transports begründen ihre Decorators ausdrücklich mit „the moment Apache would
      discard its connection“. Die Annahme ist falsch.
    - Ausführungsskizze: Ein Backend liefert 200 mit 50 MB, tröpfelnd oder hinter einem langsamen Upstream.
      1. Der Worker hat nach 256 KiB genug, liest beim Close aber weiter, mit Permit und Verbindung.
      2. Dauert der Rest länger als das TimeLimiter-Budget, erhält der Aufrufer `TimeoutException`. Der Breaker
         zählt einen Fehler.
      3. Die eigentlich zutreffende `ResponseBodyLimitExceededException` wäre ignoriert worden
         (`RestClientVerdicts.clientSideFailure`). Ein dauerhaft zu großer Endpoint öffnet so den Breaker des
         Backends.
      4. Erst der Interrupt des TimeLimiters schließt den Socket.
    - Triggering condition: nur Edge Case. Der übergroße Body muss groß oder langsam genug sein, damit der Rest das
      Budget sprengt; sonst bleibt nur verschwendete Lesezeit.
    - Impact: falsches Breaker-Verdict (eine client-seitige Ablehnung erscheint als Backend-Ausfall). Worker,
      Bulkhead-Permit und Verbindung bleiben bis zum Budget-Ende belegt.
  - Fix strategy: Auch für Apache einen Request-Decorator wie bei Jetty/Reactor Netty vorsehen, der eine nicht zu
    Ende gelesene Antwort abbricht statt drainiert (Apache kennt dafür den Abort des klassischen Requests bzw.
    `CloseMode.IMMEDIATE` auf der Verbindung). Die drei KDocs korrigieren und den Fall mit einem tröpfelnden
    Übergröße-Body je Engine testen (Befund 8).

- [x] 2. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/RestClientProperties.kt:115` — mit `:131`; `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientProperties.kt:98`, `:113`; Anwendung in `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt:86`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/JettyEngineTransport.kt:69`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ApacheEngineTransport.kt:108`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:160`, `:280`, `:398`] {Medium} {Confidence: high} {robustness / config}
  - `pool.max-idle-time` wird nur gegen negative Werte geprüft; `0` passiert die Validierung. Die Engines lesen die
    0 aber gegensätzlich:
    - **Reactor Netty** (beide Stacks): Das Eviction-Prädikat ist `maxIdleTime != -1 && meta.idleTime() >=
      maxIdleTime` (`PooledConnectionProvider.java:668-669`). Bei 0 trifft es auf **jede** freie Verbindung zu,
      sie wird beim nächsten Acquire verworfen. Damit gibt es kein Keep-Alive, also eine neue TCP-Verbindung pro
      Aufruf.
    - **Jetty** (beide Stacks): `idleTimeout = 0` heißt „kein Idle-Timeout“, freie Verbindungen werden nie
      geschlossen.
    - **Apache** (beide Stacks): `closeIdle(0)` schließt bei jedem Sweep alle freien Verbindungen.
    - Für die Nachbarknöpfe ist 0 ausdrücklich „aus/unbegrenzt“ (`max-life-time`, `evict-in-background`). Ein
      Operator, der analog „kein Idle-Limit“ meint, schaltet auf der Default-Engine des reaktiven Stacks die
      Wiederverwendung ab. Die Startup-Zeile meldet „applies pool.max-idle-time=PT0S“.
    - Triggering condition: nur bei explizit konfigurierter 0
    - Impact: Verbindungsaufbau pro Call und TIME_WAIT-Stau auf Loopback unter Last; auf Jetty das Gegenteil
      (Verbindungen, die der Sidecar längst geschlossen hat, bleiben im Pool). Die Wirkung hängt von der Engine
      ab, ohne dass jemand es merkt.
  - Fix strategy: Die 0 bei der Bindung ablehnen (`max-idle-time` ≥ 1 ms) oder eine einheitliche Bedeutung
    festlegen und je Engine umsetzen. Die Wahl in der Property-KDoc nennen.

- [x] 3. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt:356` — bis `:360`, mit `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt:160-167`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/JettyEngineTransport.kt:154`] {Medium} {Confidence: high (Mechanik) / medium (Häufigkeit)} {resources}
  - `shutdown()` ruft `transports.values.forEach { it.shutdown(...) }` ohne try/catch pro Engine auf.
    - Jettys `HttpClient.stop()` deklariert `throws Exception`.
    - Der Reactor-Netty-Transport ruft `provider.disposeLater().block(DISPOSE_WAIT)` und
      `loops.disposeLater(Duration.ZERO, DISPOSE_WAIT).block(DISPOSE_WAIT)` auf. Das zweite `block` hat dieselben
      5 s wie Nettys eigenes Graceful-Timeout, und ein Ablauf wirft `IllegalStateException`. Danach laufen
      `offload.shutdownNow()` und die Loop-Disposal nicht mehr.
    - Eine Exception verlässt damit den `@PreDestroy`, was die KDoc ausdrücklich ausschließt („never as an
      exception out of a destroy callback“), und die Engines danach in Enum-Reihenfolge werden nicht geschlossen.
    - Der WebClient-Zwilling isoliert jede Engine (`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:108-116`).
      Seine Factory-KDoc begründet ausführlich, warum `block(Duration)` dort vermieden wird
      (`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:305-312`).
    - Triggering condition: ein Fehler beim Jetty-Stop oder ein langsamer Netty-Loop-Shutdown, also selten
    - Impact: Leak von Pool, Event-Loops und Threads beim Context-Close (relevant für Tests, Refresh und
      Hot-Redeploy) sowie ein Stacktrace statt der Ergebniszeile. Jettys `QueuedThreadPool` hat
      Nicht-Daemon-Threads und kann eine JVM am Beenden hindern.
  - Fix strategy: Jeden Engine-Shutdown einzeln absichern und ein Fehlschlagen als WARN-Zeile melden, wie im
    Zwilling. Im Reactor-Transport den Ablauf des Disposals als Ergebnis behandeln (`timeout` + `onErrorResume`
    bzw. Fallback auf `dispose()`), bevor `offload` beendet wird.

- [x] 4. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt` — ganze Datei; `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt` — ganze Datei; Zusagen in `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineChecks.kt:176-211` und den `applied`-Listen der RestClient-Transports] {Medium} {Confidence: high} {test quality / false confidence}
  - Je Engine sind nur der Response-Timeout, das Buffer-Limit, der Host-Header, gzip (reaktiv) und der Abbruch belegt.
    - **Nirgends außer auf der Apache-Engine des RestClients** getestet sind `max-connections`,
      `pending-acquire-timeout`, `max-life-time` und `max-idle-time`.
    - `connect-timeout` hat auf keiner Engine einen Test.
    - Mutationen wie `DuplexConnectionPool(destination, 64)`, ein entferntes `.maxConnections`/`.pendingAcquireTimeout`
      bei Reactor Netty oder `setMaxConnPerRoute`/`setConnectionRequestTimeout` beim reaktiven Apache bleiben grün.
    - Die Startup-Zeilen-Tests prüfen nur Text.
    - Triggering condition: Regression in der Pool-Verdrahtung einer Nicht-Default-Engine
    - Impact: Ein Pool kann still auf dem Engine-Default laufen (z. B. unbegrenzt oder ohne Acquire-Timeout),
      während die Startup-Zeile „applies“ meldet. Die Bulkhead-Invariante aus Vorbericht-Befund 4 stützt sich
      genau auf diese Grenze.
  - Fix strategy: Einen über `@EnumSource` parametrisierten Test mit `max-connections: 1` schreiben: einen Exchange
    halten und prüfen, dass der zweite engine-spezifisch begrenzt wird, wie die Startup-Zeile es zusagt. Dazu einen
    Connect-Timeout-Fall gegen eine nicht antwortende Adresse.

- [x] 5. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:259` — bis `:281`, gegen `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/JettyEngineTransport.kt:146-155`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt:152-168`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/JdkEngineTransport.kt:107-130`; reaktiv `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:238-251`, `:415-431`] {Medium} {Confidence: high} {test quality / coverage gap}
  - Der Shutdown je Engine wird nur an einer leerlaufenden Factory geprüft (`isClosed`). Drain, Deadline-Ablauf und
    unterbrochener Shutdown (`ResilientRestClientIntegrationTest`, `leapingClock`) laufen nur auf Apache.
    - Reaktiv ist der WARN „did not drain“ nur für Reactor Netty belegt.
    - Die Zeitheuristik des Apache-Releases, die auf einer ausdrücklichen `Assumption:` ruht (`:424-427`), und der
      `shutdownNow`-Fallback des JDK-Clients laufen nie.
    - Mutationen, die grün bleiben: `drainOrReport` aus Jetty oder Reactor entfernen, `awaitTermination` beim JDK
      entfernen, den Interrupt-Zweig von Reactor löschen.
    - Triggering condition: Regression im Shutdown einer Nicht-Default-Engine
    - Impact: Ein Graceful Shutdown, der laufende Aufrufe abschneidet oder hängt, bliebe für drei von vier Engines
      unbemerkt (siehe auch Befund 3).
  - Fix strategy: Die vorhandenen Drain-, Deadline- und Interrupt-Shutdown-Tests über die Engines
    parametrisieren. Für den JDK-Client, der keinen Lease-Zähler hat, auf das Ergebnis prüfen (der Call gelingt
    bzw. die WARN-Zeile erscheint). Reaktiv den Release mit kurzer Release-Zeit gegen einen hängenden Exchange
    testen.

- [x] 6. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIsolationTest.kt:121` — und `:164`, gegen `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:220-227`; ebenso `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIsolationTest.kt:100-103`] {Medium} {Confidence: high} {test quality / false confidence}
  - Die Isolations-Tests belegen „das Modul nutzt seine Engine, nicht Boots Schalter“ über `connectorOf(...)` bzw.
    `engineTransports.keys`. Beides ist die eigene Buchführung der Factory: Die Map wird in `:220-221` gefüllt,
    **vor** und unabhängig von `.clientConnector(connector)` in `:227`.
    - Fällt `.clientConnector(...)`/`.requestFactory(...)` am Klon weg oder wird es bedingt, behält der Klon Boots
      Engine, und der Test bleibt grün.
    - Ein Draht-Merkmal sichert nichts zusätzlich: Der User-Agent ist veredium-eigen und auf allen Engines
      gleich.
    - Die RestClient-Seite prüft im zweiten Fall immerhin den Typ von `requestFactoryOf` (`:136`), im ersten nicht.
    - Triggering condition: Regression in der Klon-Verdrahtung
    - Impact: Boots globaler Schalter könnte unbemerkt Engine, Pool und Timeouts der Mesh-Clients bestimmen. Genau
      das soll die Engine-Auswahl laut ihrer KDoc verhindern.
  - Fix strategy: Die tatsächlich genutzte Engine beobachten, über ein engine-spezifisches Merkmal auf dem Draht
    oder den Namen des I/O-Threads, oder den Connector vom gebauten Client ablesen statt aus der Map.

- [x] 7. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:271` — bis `:297`; Gegenstück `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt` (Response-Timeout-Fall)] {Medium} {Confidence: high} {test quality / coverage gap}
  - JDK, Jetty und Apache teilen **einen** Client je Engine und tragen den Response-Timeout pro Connector bzw. pro
    Request-Factory (`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:232`,
    `:295`, `:401-409`).
    - Jeder Timeout-Fall nutzt genau einen Endpoint, also einen Timeout-Wert.
    - Eine Mutation, die die `RequestConfig` des ersten Endpoints cacht oder das Maximum über alle Endpoints nimmt,
      bleibt grün.
    - Triggering condition: Regression bei mehreren Backends auf einer Engine
    - Impact: Ein Backend läuft mit dem Timeout eines anderen. Die Transport-Sprosse der Leiter kann dann über
      ihrem eigenen TimeLimiter liegen, und Timeouts erscheinen als TimeLimiter-Abbruch statt als
      Transport-Timeout (anderes Breaker- und Log-Bild).
  - Fix strategy: Zwei Backends auf derselben Engine mit verschiedenen Ankern anlegen; jedes muss an seinem eigenen
    Timeout scheitern.

- [x] 8. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:232` — bis `:255`, gegen `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/JettyEngineTransport.kt:228-231` (`AbortingResponse.close`) und `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt:245-250`] {Medium} {Confidence: high} {test quality / false confidence}
  - Der Übergröße-Test nutzt einen 2-KB-Body, der in einem Stück ankommt. Das Drainieren ist sofort fertig, und der
    Jetty-Request ist schon abgeschlossen. Ob die Engine den Rest verwirft oder liest, beeinflusst das Ergebnis also
    nicht.
    - Das Entfernen von `if (!endOfBody) jettyRequest.abort(...)` oder `body?.closeUnlessAtEnd()` bleibt grün.
    - Auf Apache wäre ein solcher Test rot geworden (Befund 1).
    - Triggering condition: großer oder tröpfelnder Body über dem Limit
    - Impact: Die Zusage „released instead of drained“ ist für alle Engines unbelegt; der echte Defekt auf der
      Default-Engine blieb dadurch verborgen.
  - Fix strategy: Ein tröpfelnder Body deutlich über dem Limit (`withChunkedDribbleDelay`) je Engine. Nach der
    `ResponseBodyLimitExceededException` prüfen, dass die Ausnahme vor dem Ende des Tröpfelns kommt und
    `connectionsInUse() == 0` gilt.

### 🟢 Low

- [x] 9. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt:165` — bis `:176`] {Low} {Confidence: medium} {resources}
  - Die Transports werden eager in Enum-Reihenfolge erzeugt. Wirft ein späterer Konstruktor (Jettys `start()`, eine
    fehlende Klasse jenseits der einen Marker-Klasse von `RestClientEngine.isPresent`), werden die schon gebauten
    nie geschlossen; die Bean erreicht `@PreDestroy` nicht. Betroffen sind der Apache-Evictor und gegebenenfalls ein
    gestarteter Jetty-Client mit Nicht-Daemon-Threads, der die JVM nach einem gescheiterten Start am Beenden hindern
    kann.
  - Fix strategy: Transports in einer Schleife bauen, die bei einem Fehler die bereits erzeugten schließt und dann
    weiterwirft.

- [x] 10. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:108` — mit `:238-251`, `:415-431` und `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:169-171`] {Low} {Confidence: high} {robustness}
  - Der reaktive Release wartet je Engine nacheinander, jede mit dem vollen `poolDisposeTimeout` (JDK
    `awaitTermination`, Apache `awaitShutdown`, Reactor-Drain). Damit kann `@PreDestroy` bis zu dreimal so lange
    dauern. Der RestClient-Zwilling teilt eine gemeinsame Deadline.
  - Außerdem ruft `IdleConnectionEvictor.shutdown()` nur `interrupt()` auf, ohne Join; `isReleased()` (`:433`) kann
    kurz `false` melden.
  - Fix strategy: Eine gemeinsame Deadline wie im RestClient einführen und den Evictor nach dem Shutdown begrenzt
    abwarten.

- [x] 11. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:208` — mit `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:91`, `:102`] {Low} {Confidence: high} {resources}
  - Die Engines des WebClients entstehen lazy beim ersten `create()`, und `release()` erfasst nur bereits
    gestartete. Ein `create()` nach `shutdown()` für eine bis dahin unbenutzte Engine baut einen neuen Transport
    (Threads, I/O-Reaktor), den niemand mehr freigibt. Im RestClient läuft derselbe Aufruf dagegen auf einen bereits
    geschlossenen Pool.
  - Fix strategy: Ein „released“-Flag einführen, das spätere `create()`/`connectorFor` mit klarer Meldung ablehnt
    (beide Factories).

- [x] 12. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:282` — mit `:304-308`] {Low} {Confidence: high} {robustness}
  - `pool.pending-acquire-max-count` wird bei Jetty zur Queue-Kapazität pro Destination. Jetty stellt aber **jeden**
    Request zuerst in diese Queue, auch den, der gleich eine Verbindung bekommt (`HttpDestination.send` →
    `enqueue` → `queue.offer`, jetty-client 12.1.12).
    - Bei einem Burst auf einen kalten Pool weist Jetty schon ab `pendingAcquireMaxCount` gleichzeitigen Requests
      ab (Default 100). Reactor Netty nähme bei denselben Werten 100 + 100 an.
    - Praktisch begrenzen die Bulkheads die Gleichzeitigkeit vorher (Summe ≤ `max-connections`, sonst warnt der
      Guard). Deshalb nur Low; ein Sub-Agent hatte Medium vorgeschlagen.
  - Fix strategy: Die Kapazität auf `maxConnections + pendingAcquireMaxCount` abbilden (unbegrenzt bleibt
    unbegrenzt) oder die abweichende Semantik in der Startup-Zeile nennen.

- [x] 13. [`./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceStartupGuard.kt:206`] {Low} {Confidence: high} {robustness / diagnostics}
  - `warnIfBulkheadsOvercommitThePool` vergleicht die Bulkhead-Summe **aller** Mesh-Endpoints mit einem
    `max-connections`. Mit der Engine-Auswahl hat aber jede Engine ihren eigenen Pool, und die JDK-Engine hat gar
    keine Grenze. Werden Backends auf zwei Engines verteilt, warnt der Guard fälschlich; für reine JDK-Backends ist
    die Warnung gegenstandslos.
  - Fix strategy: Die Summe je Engine bilden (die Zuordnung kennt die Client-Factory, nicht der Core-Guard) oder den
    Check in die Factories verlagern.

- [x] 14. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClient.kt:273` — mit `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:364-392`] {Low} {Confidence: medium} {maintainability / test quality}
  - Der lokale `onErrorDropped`-Hook gilt für die ganze Kette und stuft **jeden** verworfenen Fehler auf DEBUG
    herab, nicht nur das erwartete Nachspiel eines Abbruchs. Ein echter Operatorfehler würde damit ebenfalls leise.
  - Der Test, der laut Assumption-Kommentar den Hook-Schlüssel pinnt, prüft nur die Abwesenheit einer ERROR-Zeile.
    Er prüft direkt nach dem Verbindungsabbau, während die Engine ihren Fehler asynchron meldet, und kann deshalb
    grün sein, bevor etwas gefallen ist. Die DEBUG-Zeile „ended after cancellation“ prüft kein Test. Der Appender
    wird außerdem nicht im `finally` abgehängt.
  - Fix strategy: Im Hook nur Fehler nach einem Cancel herabstufen und andere weiter an Reactors Default geben. Im
    Test begrenzt auf die DEBUG-Zeile des Backends warten, dann die ERROR-Abwesenheit prüfen.

- [x] 15. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ApacheEngineTransport.kt:122` — mit `:207-217`] {Low} {Confidence: medium} {resources / metrics}
  - Die `ConnPoolMeters`-Gauges bleiben nach `connectionManager.close` in der Host-Registry; der Reactor-Transport
    deregistriert seine (`deRegisterMetrics`). Bei einer Registry, die die Factory überlebt (Context-Refresh, zweite
    Factory), lesen die Gauges einen geschlossenen Pool weiter, und eine Neuregistrierung mit gleichen Tags liefert
    die alte Gauge.
  - Fix strategy: Die gebundenen Meter merken und im Shutdown entfernen.

- [ ] 16. [`./veredium-restclient/docs/timeouts-bulkheads-threads.md` — Klassifikationstabelle; aus dem Vorbericht übernommen, gegen legatium 1.2.1 nachgeprüft] {Low} {Confidence: high} {maintainability / logging}
  - Ein abgebrochener Exchange (TimeLimiter-Ablauf, unterbrochener Aufrufer: `Closed by interrupt`) erscheint im
    Exchange-Strom weiter als ERROR `adapter_outcome=failure`. Der Pool-Lease-Timeout ist seit 1.2.1 korrekt
    `timeout`. Mit den neuen Engines kommen weitere Abbruch-Exceptions hinzu (`InterruptedIOException` des
    Reactor-Hand-offs, `AsynchronousCloseException` von Jetty), deren Klassifikation nicht geprüft ist.
  - Fix strategy: weiter über Inqudium/legatium#88; die neuen Abbruchtypen dort mit aufnehmen.

- [x] 17. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:485` — bis `:505` (`AbortableExchange`)] {Low} {Confidence: high} {test quality / concurrency}
  - Die Race „Abort vor Attach“ (Cancel kommt, bevor Apache seinen `Future` geliefert hat) hat keinen Unit-Test.
    Im End-to-End-Test ist der `Future` stets lange vor dem Abort angehängt. Die Implementierung (volatile Flag +
    `AtomicReference`) ist korrekt, aber eine Regression bliebe unbemerkt und hielte Exchange und Pool-Slot bis zum
    Response-Timeout.
  - Fix strategy: Ein Unit-Test mit einem Stub-`Future` für beide Reihenfolgen.

- [x] 18. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineConfigTest.kt:140` — bis `:160`, gegen `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineChecks.kt:43`] {Low} {Confidence: high} {test quality}
  - `FilteredClassLoader("org.eclipse.jetty")` versteckt beide geforderten Klassen; eine Mutation von `all {}` zu
    `any {}` bleibt grün. Der wahrscheinlichste reale Fehler ist ein Host mit `jetty-client`, aber ohne
    `jetty-reactive-httpclient` (bzw. ohne `httpcore5-reactive`).
  - Fix strategy: Nur `org.eclipse.jetty.reactive` bzw. `org.apache.hc.core5.reactive` verstecken.

- [x] 19. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:286` — bis `:320`] {Low} {Confidence: medium} {test quality}
  - Der Test „Abbruch beim Warten auf den Head“ bestätigt nicht, dass der Request das Backend erreicht hat (anders
    als die Interrupt- und Cancel-Tests mit `inFlight`-Latch, `:377-394`). Läuft das 300-ms-Budget schon während
    Connect oder Engine-Start einer frisch gebauten Engine ab, ist der Test grün, prüft aber den Connect-Pfad.
  - Fix strategy: Denselben Request-Listener-Latch ergänzen und prüfen, dass er vor dem Timeout ausgelöst hat.

- [x] 20. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:85` — bis `:106`, mit den Delays um `:208`, `:299`, `:341`, `:376`, `:414`] {Low} {Confidence: medium} {test quality / flakiness}
  - Ein WireMock-Server für die ganze Klasse. Etwa 16 abgebrochene Exchanges mit 10-s-Server-Delay schlafen
    weiter auf Container-Threads, auch nach dem Client-Abbruch; `resetAll()` beendet sie nicht. Auf einem
    langsamen Runner kann das Thread-Pool-Engpässe erzeugen und die `elapsed < 2 s`-Assertions späterer Fälle
    reißen.
  - Fix strategy: `asynchronousResponseEnabled(true)` bzw. mehr Container-Threads, oder eine WireMock-Instanz pro
    Nested-Klasse.

- [x] 21. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:686` — bis `:690`] {Low} {Confidence: medium} {test quality / isolation}
  - Der Check „keine globalen `HttpResources`“ liest den privaten statischen Zustand von Reactor Netty und alle
    Threadnamen der JVM. Ein früherer Test im selben Surefire-Fork, der die globalen Ressourcen berührt, macht ihn
    rot, ohne dass ein Defekt vorliegt. Umgekehrt können noch terminierende Loops eines früheren Tests das
    `anyMatch` erfüllen.
  - Fix strategy: Den Zustand vor dem Aufruf erfassen und auf die Differenz prüfen.

- [x] 22. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientPropertiesTest.kt:202`] {Low} {Confidence: high} {test quality}
  - Der `bind()`-Helfer fällt mit `orElseGet { RestClientProperties() }` auf die Defaults zurück. Der Fall `apache`
    in „should bind every engine by its YAML value“ wäre deshalb auch bei kaputter Bindung grün.
  - Fix strategy: `orElseThrow` wie im Referenzdatei-Fall verwenden.

- [x] 23. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:501` — bis `:525`] {Low} {Confidence: high} {test quality}
  - Die Zusage „ein Client pro Engine, geteilt von allen Backends“ wird über die Keys von `startedEngines()`
    geprüft. Die Map kann für eine Engine aber gar nicht zwei Einträge haben. Eine Mutation, die pro
    `connectorFor` einen eigenen JDK- oder Apache-Client baut, bliebe grün; nur der letzte würde freigegeben.
  - Fix strategy: Prüfen, dass die Connectors zweier Backends dieselbe Client-Instanz tragen.

- [x] 24. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:290` — bis `:296`] {Low} {Confidence: medium} {test quality / flakiness}
  - Das Response-Timeout-Fenster wird mit der Wanduhr gemessen, und die 400 ms obere Toleranz enthalten auch das
    Laden der Klassen und den Codec-Aufbau beim ersten Exchange einer Engine.
  - Fix strategy: Vorher einen Warm-up-Call machen oder ab dem von WireMock protokollierten Eingang des Requests
    messen.

## 6. Systemic Patterns

1. **Die Startup-Zeilen sagen pro Engine mehr zu, als die Tests belegen.** Je Engine meldet die Zeile 5–8
   „applies“-Knöpfe, auf dem Draht geprüft sind davon 2–4 (Response-Timeout, Limit, Host, gzip). Zählbasis: die
   `applied`-Listen der vier RestClient-Transports und `WebClientEngineChecks.coverageOf`, gegen die
   `@EnumSource`-Fälle der beiden `*EngineIntegrationTest.kt`. Das sind 7 Engine-Kombinationen außerhalb der Defaults
   mit zusammen ≈30 unbelegten Zusagen (Zählung beim manuellen Abgleich). Repräsentativ: Befunde 4, 5, 7, 8.
2. **Annahmen über das Verhalten von Drittbibliotheken stehen in Prosa-KDoc, nicht als geprüfte `Assumption:`.**
   Die Annahmen, die sich als falsch oder engine-abhängig erwiesen, sind „Apache verwirft eine nicht gelesene
   Antwort“ und „0 = aus“. Zählbasis: `grep -rn "Assumption:" */src/main` liefert 4 Treffer, gegenüber ≈10
   Prosa-Annahmen über Engine-Interna in den Transport-KDocs (Cancellation-Absätze, Mapping-Tabellen), gezählt beim
   Lesen. Die CONTRIBUTING-Konvention, mit der `grep "Assumption:"` als Checkliste für Dependency-Bumps dienen soll,
   greift für die Engines also kaum. Repräsentativ: Befunde 1, 2.
3. **Die beiden Zwillingsmodule lösen den Lebenszyklus der Engines verschieden.** RestClient baut eager, WebClient
   lazy; RestClient teilt eine Deadline, WebClient wartet nacheinander; WebClient isoliert Fehler pro Engine,
   RestClient nicht; Jetty drainiert im RestClient und wird im WebClient sofort gestoppt; Reactor Netty disponiert
   hier mit `block(Duration)` und dort mit `timeout`. Zählbasis: 5 Unterschiede, gezählt beim Abgleich von
   `ResilientRestClientFactory`/`EngineTransport` mit `ResilientWebClientFactory`/`EngineTransports`. Jeder
   Unterschied ist für sich vertretbar, zusammen ergeben sie zwei Fehlerbilder für eine Aufgabe. Repräsentativ:
   Befunde 3, 9, 10, 11.
4. **Tests prüfen die Buchführung der Factory statt beobachtbares Verhalten.** `connectorOf`, `requestFactoryOf`,
   `engineTransports.keys` und `startedEngines().keys` dienen als Beweis für die Engine-Zuordnung und das Teilen der
   Clients. Zählbasis: `grep -rn "connectorOf\|requestFactoryOf\|engineTransports\|startedEngines" */src/test`
   liefert 23 Treffer in 4 Dateien. Repräsentativ: Befunde 6, 23.

Statistik-Block: **4 systemische Muster**.

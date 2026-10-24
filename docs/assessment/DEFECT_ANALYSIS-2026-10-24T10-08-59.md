# Code & Defect Analysis: veredium (veredium-core, veredium-restclient, veredium-webclient, veredium-examples, veredium-test-support)

1. Identification of the codebase
   - **Repository:** `https://github.com/Inqudium/veredium.git`
   - **Commit hash:** `31fb21b1e5d54314c761f7d8a605533c0ccaff90` (full)
   - **Reference (branch/tag):** `refs/heads/main` / tag: keiner; Arbeitsbaum sauber
2. Scope of the analysis
   - **Included:**
     - Produktionscode: `./veredium-core/src/main/kotlin/`, `./veredium-restclient/src/main/kotlin/`, `./veredium-webclient/src/main/kotlin/`, `./veredium-examples/src/main/kotlin/` und `./veredium-test-support/src/main/kotlin/` (die geteilten Testhelfer; ein Fehler dort trifft beide Client-Suiten).
     - **Testcode ist Teil der Analyse:** `./veredium-*/src/test/kotlin/`, `./veredium-*/src/test/java/` (Jazzer-Targets) und `./veredium-*/src/test/resources/`.
     - Schwerpunkt ist das Delta seit dem Vorbericht, `6d423d61cdacb0be8d4b466483b69378484e9a70..31fb21b1e5d54314c761f7d8a605533c0ccaff90` (47 Commits, 146 Dateien unter `src/`, +10,3 k / −7,3 k Zeilen). Es umfasst `EngineSet`/`EngineDrain`/`MeshClientPlan` im Core, ADR-0003 (beide Stacks in einem Kontext), die Auto-Konfigurationen, die neuen Module `veredium-examples` und `veredium-test-support`, legatium 1.3.0 und die Reactor-Netty-Hand-off-Korrektur.
     - Ergänzend gelesen: `./docs/engine-behaviour.md`, `./docs/adr/`, `./CONTRIBUTING.md`, `./CHANGELOG.md` und die Quellen von spring-web 7.0.9 (`JdkClientHttpRequest`, `ReactorClientHttpRequest`/`-Response`) aus dem lokalen Maven-Repository.
   - **Excluded:** generierte Artefakte (`./**/target/`), das Build-only-Modul `./veredium-coverage/` (kein Code), `./.github/` sowie die MkDocs-Site außer den oben genannten Seiten.
3. Analysis environment & tools
   - **Target environment:** Java 21 (Bytecode-Ziel), Build-JDK 24+ (lokal Oracle JDK 26.0.1); Kotlin 2.4.21; Spring Boot 4.1.1 / spring-web 7.0.9; Resilience4j 2.4.0; Apache HttpClient 5.6, Jetty Client 12.1, Reactor Netty 1.3; legatium 1.3.0
   - **Build system:** Maven 3.9.15, Multi-Modul-Reaktor
   - **Analysis tools used:**
     - Manuelles Review (Claude Code, Opus 5.5) des gesamten Produktionscodes des Deltas und aller Score-4/5-Dateien.
     - Drei Sub-Agenten für die Testsuiten (restclient; webclient; core + examples + test-support). Ihre Befunde wurden an den genannten Zeilen gegengelesen. Zwei Befunde wurden von den Agenten mit Wegwerf-Proben im Scratchpad gegen `target/classes` reproduziert (Befund 1 und Befund 9); im Repository wurde nichts geändert.
     - `mvn -q -o verify` (siehe Abschnitt 2), dazu `grep`.
     - Vorhandene statische Analyse: ktlint, CodeQL (CI), JaCoCo, Jazzer, eigene CI-Prüfungen (Kommentar- und Rationale-Form). Nicht vorhanden: detekt, SpotBugs/Error Prone, ArchUnit, Sonar.
4. Placement & Output
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/veredium` (absoluter Bezugspunkt; alle relativen Pfade beziehen sich darauf)
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-24T10-08-59.md` (relativ zum workdir; Präfix + ISO-8601-Zeitstempel in UTC wie bei den Vorberichten). Die Vorberichte unter `./docs/assessment/` bleiben unverändert.
   - **Scope root (relative to the workdir):** `./` (Modulwurzeln `./veredium-core/`, `./veredium-restclient/`, `./veredium-webclient/`, `./veredium-examples/`, `./veredium-test-support/`)
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>`. Die gemeinsamen Präfixe werden ausgeschrieben: `src/main/kotlin/eu/inqudium/veredium/<modul>/` bzw. `src/test/kotlin/eu/inqudium/veredium/<modul>/`.

---

## 1. Executive Summary

Der Code ist weiterhin in gutem Zustand. Alle 19 Befunde des Vorberichts sind abgehakt, und keiner ist im
aktuellen Code zurückgekehrt. **Es gibt keinen Critical- und keinen High-Befund.**

Der große Umbau seit dem Vorbericht ist im Produktionscode sauber gelungen: `EngineSet` (gemeinsamer Lebenszyklus
beider Stacks), `MeshClientPlan`, die Auto-Konfigurationen und die Eigentümerprüfung nach ADR-0003.

Der eine neue Produktionsbefund von Gewicht betrifft den **Release der reaktiven Reactor-Netty-Engine auf einem
unterbrochenen Shutdown-Thread** (Befund 1). `Mono.block()` wirft dort sofort. Die Event-Loops werden dann nie
freigegeben, laufende Aufrufe enden erst an ihrem Response-Timeout, und statt der einheitlichen Drain-Zeile erscheint
ein Stacktrace. Der Testfall, der das zeigen würde, lässt genau diese Engine aus. Das ist reproduziert.

Die übrigen Medium-Befunde sind Lücken in der Beweiskraft der Tests:

- **Shutdown:** Die Shutdown-Tests prüfen die Engine-Seite (geschlossen, WARN), aber nicht, wie ein laufender Aufruf
  endet. Die CI-Hang-Korrektur der blockierenden Reactor-Netty-Engine ließe sich deshalb unbemerkt zurückbauen
  (Befund 2).
- **Auto-Konfiguration:** Kein Test schickt einen Aufruf durch die Factory, die die Auto-Konfiguration baut. Alle
  Kontexte bauen sie per `withBean` selbst (Befund 3).
- **Reaktive Validierung:** Deserialisierung und Validierung nach der Dekoration sind im reaktiven Stack ungetestet;
  der blockierende Zwilling testet sie (Befund 4).
- **Beispiele:** Der Abbruch-Test des Async-Beispiels ist aus dem falschen Grund grün (Befund 5).
- **Body-Abbruch:** Der Body-Phasen-Abbruch hängt am Timing von WireMocks Dribble (Befund 6).

**Test-Urteil (kompakt):**

- **Verlässlichkeit als Sicherheitsnetz:** hoch für Leiter, Startup-Guard, Eigentümerprüfung, Verdicts,
  Customizer, den blockierenden Decorator und die Engine-Transporte.
  - Es gibt kein `Thread.sleep`, keine Mocks, injizierte Uhren für die Fristzweige und Raw-Socket-Backends für die
    riskanten Abbruchpfade. Die meisten Rationale-Blöcke sind mit Mutationen im Kopf geschrieben.
  - Falsche Sicherheit entsteht an den Rändern: bei der Verdrahtung (Befund 3), auf der Aufruferseite des Shutdowns
    (Befunde 1, 2) und bei asymmetrisch abgedeckten Zwillingen (Befund 4).
- **Nutzung & Abdeckung (Pyramide):** bewusst kopflastig. Die Engine-Zusagen sind nur gegen echte Engines
  beweisbar. Eine dünne, gute Unit-Schicht trägt die reine Logik (Leiter, Decorator, Verdicts, `BoundedBodyReader`,
  `AbortableExchange`), dazu Jazzer im Regressionsmodus. Die neuen Core-Bausteine `EngineDrain`, `MeshClientPlan`
  und die Sperre von `EngineSet` haben keine eigenen Core-Tests (Befund 12).
- **Gravierendste Lücken:**
  - Reactor-Netty-Release bei unterbrochenem Shutdown (reaktiv): echter Fehler, vom Test ausgespart (Befund 1).
  - Hand-off und `shutdownNow` der blockierenden Reactor-Netty-Engine an keinen Test gebunden (Befund 2).
  - Produktionsweg der Auto-Konfiguration nie mit einem Aufruf ausgeübt (Befund 3).
  - Reaktive Deserialisierung/Validierung gegenüber dem Breaker ungetestet (Befund 4).

## 2. Scope & methodology

- **Analysiert:** der gesamte Produktionscode der fünf Module mit Code, mit Tiefe auf dem Delta und den
  Score-4/5-Dateien (Abschnitt 4). Testcode war im Umfang: alle drei Client-/Core-Suiten, die Beispiel-Tests und die
  geteilten Helfer.
- **Nicht analysiert:** `./veredium-coverage/` (kein Code), `./.github/`, generierte Artefakte.
- **Erkannter Stack:**
  - Spring Boot 4.1.1.
  - Ein blockierender Stack (Spring `RestClient` auf virtuellen Worker-Threads des `BlockingResilienceDecorator`)
    und ein reaktiver Stack (WebClient/Reactor), je mit vier wählbaren Engines (Apache, JDK, Jetty, Reactor Netty).
    Die Engine-Wahl ist gewollte Funktionalität (ADR-0004) und wird hier nicht in Frage gestellt.
  - Keine Coroutines, keine Persistenz, kein Messaging.
  - Kotlin 2.4 auf Java 21, Build mit JDK 24+.
- **Statische Analyse:** ktlint, CodeQL, JaCoCo (Aggregat in `veredium-coverage`), Jazzer, projekteigene CI-Checks.
  Ohne detekt/SpotBugs/ArchUnit ist die Tiefe manuell.
- **Lokale Verifikation:**
  - `mvn -q -o verify` lief auf dem analysierten Commit (Ergebnis siehe unten).
  - Befund 1 wurde vom webclient-Sub-Agenten mit einer Wegwerf-Probe gegen `target/classes` reproduziert:
    - Der Release-Aufruf auf einem unterbrochenen Thread warf nach etwa 10 ms.
    - `loops.isDisposed` war 3 s nach der Drain-Schranke noch `false`.
    - Der gehaltene Aufruf endete erst an seinem Response-Timeout von 10,4 s.
  - Befund 9 wurde mit einer Probe auf `BoundedBodyReader` reproduziert: Limit 4, deklarierte Länge 4, Body 8
    Bytes; dem Stream werden 5 Bytes entnommen.
  - Weitere Reproduktions-Tests wurden nicht geschrieben; der Prompt verlangt dafür eine Freigabe.
  - **Build-Ergebnis:** `mvn -q -o verify` grün (Exit 0), ohne Fehler und Fehlschläge. Laut den Surefire-Berichten liefen 737 Tests: core 210, restclient 260, webclient 241, examples 22, test-support 4. Die Befunde 1, 2, 5 und 6 bestehen also bei grüner Suite, wie es falsche Sicherheit erwarten lässt.
- **Grenzen:**
  - Befund 7 (Rennen beim Kopf-Hand-off) beruht auf dem Lesen von spring-web 7.0.9 und dem Verhalten von Reactor
    Netty bei einem nie abonnierten Inbound. Er ist nicht reproduziert; Konfidenz niedrig.
  - Das Verhalten der Engines unter echter Last wurde nicht gemessen.

## 3. Statistics

| Severity | Anzahl |
|---|---|
| 🔴 Critical | 0 |
| 🟠 High | 0 |
| 🟡 Medium | 6 |
| 🟢 Low | 14 |
| **Summe** | **20** |

Davon betreffen 4 Befunde Produktionscode (1, 7, 8, 11), 1 Befund die Dokumentation (20) und 15 Befunde den
Testcode. **Systemische Muster: 4** (Abschnitt 6).

## 4. File-ranking table

Produktionscode (Score = Wahrscheinlichkeit eines Defekts):

| Datei | Score | Begründung |
|---|---|---|
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt` | 5 | Vier Engines, Drain/Abbruch/Shutdown, Lock-freie Zustände (`AbortableExchange`, In-flight-Set), Reactor-Netty-Disposal |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt` | 5 | Hand-off der Kopf- und Body-Wartezeit auf virtuelle Threads, Interrupt-Semantik, Disposal |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecorator.kt` | 5 | Thread-Hand-off, Budget, Interrupt, asynchroner Abbruch, Permit-Freigabe |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt` | 4 | Engine-Lebenszyklus, blockierendes Warten auf die Pool-Disposal |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt` | 4 | Hot Path, Async-Abbruch, Statusabbildung, Bounded Read |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/EngineTransport.kt` | 4 | `AbortingRequest`/`AbortingResponse`: Abbruch vor dem Drain des Rests |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ApacheEngineTransport.kt` | 4 | Geteilter Pool, Evictor, Abbruch über `ThreadLocal`-Fang |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/JettyEngineTransport.kt` | 4 | Abbruch, Interrupt-Flag-Wiederherstellung |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/engine/EngineSet.kt` | 4 | Neu: Lock-Lebenszyklus, Release-Fehlerpfade |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/engine/EngineDrain.kt` | 4 | Neu: Fristarithmetik, Interrupt-Ausgang |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceStartupGuard.kt` | 4 | Viele Zweige, Pool-Dimensionierung |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceCustomizers.kt` | 4 | Prädikat-Komposition gegen Resilience4j-Interna, `BeanRegistrar` |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshOwnershipCheck.kt` | 3 | Neu: Bean-Namen-Parsing, `BeanFactoryPostProcessor` |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/MeshProperties.kt` / `ResolvedTimeouts.kt` | 3 | Ableitung und Guards; gut getestet |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt` | 3 | Engine-Prüfungen, Cache, Shutdown |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/JdkEngineTransport.kt` | 3 | Host-Header-Konflikt, Abbruch über Body-Close |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReader.kt` | 3 | Grenzarithmetik; gefuzzt |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/client/MeshClientPlan.kt` | 3 | Neu: Drain-Fenster, Endpunktauflösung |
| `./veredium-examples/src/main/kotlin/eu/inqudium/veredium/examples/async/AsyncCustomerAdapter.kt` | 3 | Abbruch-Weitergabe an abgeleitete Futures |
| `./veredium-examples/src/main/kotlin/eu/inqudium/veredium/examples/decorator/*.kt` | 2 | Natives HttpClient-4-Beispiel |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/engine/EngineSelection.kt`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineChecks.kt` | 2 | Startprüfungen, Texte |
| `*AutoConfiguration.kt`, `*Properties.kt`, `CallerMdc.kt`, `*Verdicts.kt`, `*ObservationReport.kt` | 1–2 | Verdrahtung, Bindung, kleine Helfer |

Testcode (Score = Gewicht als Sicherheitsnachweis):

| Datei | Score | Begründung |
|---|---|---|
| `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt` | 5 | Vier Engines × Abbruch, Timeouts, Pool, Drain, Shutdown (Befunde 2, 6, 10) |
| `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt` | 5 | Einziger Echt-HTTP-Nachweis der reaktiven Engines (Befunde 1, 18) |
| `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt` | 5 | Pipeline je Aufruf (Befund 16) |
| `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientIntegrationTest.kt` | 5 | Pipeline je Aufruf (Befunde 4, 16) |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecoratorTest.kt` | 5 | Hand-off, Budget, Interrupt (Befund 13) |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/mesh/MeshPropertiesDerivationTest.kt` | 5 | Die Leiter, sehr stark |
| `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ReactiveResilienceDecoratorTest.kt` | 5 | Verschachtelung über ihre Folgen gepinnt; kein Befund |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceStartupGuardTest.kt`, `MeshOwnershipCheckTest.kt`, `MeshResilienceContextTest.kt`, `MeshResilienceCustomizersTest.kt` | 4 | Alle Zweige, echte Kontexte; kein Befund |
| `./veredium-*/src/test/kotlin/.../LegatiumExchangeLoggingIntegrationTest.kt` (beide) | 4 | Ergebnisvokabular je Engine (Befunde 3, 16) |
| `./veredium-*/src/test/kotlin/.../*VerdictsTest.kt`, `*EngineIsolationTest.kt`, `*BuilderWiringTest.kt` | 4 | Verdicts deterministisch; Isolation auf dem Draht (Befund 3) |
| `./veredium-test-support/src/main/kotlin/eu/inqudium/veredium/testsupport/Backends.kt` | 4 | Draht-Beweise beider Suiten, selbst ungetestet (Befund 15) |
| `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ReactorNettyHandOffTest.kt` | 3 | Richtig für den Helfer, nicht an die Engine gebunden (Befund 2) |
| `./veredium-restclient/src/test/java/eu/inqudium/veredium/restclient/BoundedBodyReaderFuzzTest.java` | 3 | Eine Invariante falsch kalibriert (Befund 9) |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/engine/EngineSetTest.kt` | 3 | Fehlerpfade gut; Sperre ungetestet (Befund 12) |
| `./veredium-examples/src/test/kotlin/eu/inqudium/veredium/examples/**` | 2–3 | Smoke, Routing, Abbruch (Befunde 5, 14, 16) |
| `*PropertiesTest.kt`, `*ContractTest.kt`, `*ObservationReportTest.kt`, `WebClientMetricsTest.kt` | 1–2 | Bindung, statische Pins (Befunde 17, 19) |

## 5. Findings

### 🔴 Critical

Keine.

### 🟠 High

Keine.

### 🟡 Medium

- [x] 1. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:281` — mit `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:174-180`; Testlücke `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:163`, `:995`] {Medium} {Confidence: high} {resources / test quality}
  - Symptom → Ursache:
    - Der Release der Reactor-Netty-Engine wartet mit `drain(...).block()` auf die Pool-Disposal.
    - Auf einem Thread mit gesetztem Interrupt-Flag wirft Reactors `block()` sofort eine `ReactiveException` um die `InterruptedException`.
    - `ReactorNettyEngineTransport.shutdown` erreicht die Zeitplanung der Loop-Disposal (`:175-180`) dann nie.
    - `EngineSet` fängt die Ausnahme und schreibt „failed to release its resources" mit Stacktrace, statt der einheitlichen Zeile „did not drain because the shutdown thread was interrupted".
    - Reproduziert: Die Loops waren 3 s nach der Drain-Schranke noch nicht freigegeben, und der gehaltene Aufruf endete erst an seinem Response-Timeout.
    - Der Testfall „interrupted shutdown" läuft über `drainingEngines()` (JDK, Jetty, Apache). Diese Liste wurde für den Fristfall gebaut und ohne Begründung wiederverwendet; Reactor Netty fehlt darin.
    - `./docs/engine-behaviour.md` §12 sagt, der Fall sei auf jeder Engine gepinnt.
    - Auslösebedingung: Ein Container schließt den Kontext auf einem unterbrochenen Thread, während ein Reactor-Netty-Aufruf läuft oder Verbindungen im Pool sind.
    - Auswirkung (Leak):
      - Die Event-Loop-Threads (Daemons) überleben den Kontext. Bei einem Refresh oder in der nächsten Testklasse derselben JVM sammeln sie sich an.
      - Laufende Aufrufe enden erst an ihrem Response-Timeout.
      - Die Ursache ist im Log nur als Stacktrace sichtbar.
  - Fix-Strategie:
    - Das Warten auf die Disposal so behandeln, wie `EngineDrain` einen Interrupt behandelt: Disposal anstoßen, nicht blockieren, Flag stehen lassen.
    - Die Loop-Disposal in einem `finally` einplanen.
    - Den Interrupt-Testfall auf alle vier Engines ausdehnen.

- [x] 2. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:833` — und `:867`; `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ReactorNettyHandOffTest.kt:32-121`; gegen `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt:128`, `:187`] {Medium} {Confidence: high} {test quality / false confidence}
  - Symptom → Ursache:
    - Der Fristfall (`:799`) und der Fall mit unterbrochenem Shutdown (`:838`) prüfen die Engine-Seite: kein Ausnahmefehler, eine WARN-Zeile, Engine geschlossen.
    - Den laufenden Aufruf holen beide mit `inFlight.get(10, SECONDS)` ab und verwerfen das Ergebnis. Da das Backend nach 3 s ohnehin antwortet, besteht auch ein Aufruf, der nie abgebrochen wurde.
    - `ReactorNettyHandOffTest` ruft nur den statischen Helfer `handOff` mit einem eigenen Executor auf. Kein Test bindet den Kopf-Hand-off (`:128`) oder das `offload.shutdownNow()` (`:187`) an die Engine.
    - Mutationen, die grün bleiben: den Hand-off um `delegate.execute()` entfernen; `shutdownNow` entfernen.
    - `./docs/engine-behaviour.md:662` nennt den Fall „pinned by `ReactorNettyHandOffTest`".
    - Auslösebedingung: immer; die Suite kann die Regression nicht sehen.
    - Auswirkung: Der auf CI beobachtete Hang kann unbemerkt zurückkehren. Nach einem Shutdown über die Frist hinaus hält ein Aufruf dann Worker und Permit bis zum TimeLimiter-Budget.
  - Fix-Strategie: In beiden Shutdown-Fällen verlangen, dass der laufende Aufruf kurz nach der Rückkehr von `shutdown()` scheitert (`IOException`/`ResourceAccessException` in der Kette) und nicht erst nach den 3 s des Backends. Für Reactor Netty zusätzlich prüfen, dass der Offload-Executor beendet ist.

- [x] 3. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactoryBuilderWiringTest.kt:239` — sowie `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIsolationTest.kt:262`, `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/LegatiumExchangeLoggingIntegrationTest.kt:875`, `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIsolationTest.kt:287`; gegen `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/VerediumWebClientAutoConfiguration.kt:63-84`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/VerediumRestClientAutoConfiguration.kt:321-340`] {Medium} {Confidence: high} {test quality / false confidence (wiring)}
  - Symptom → Ursache:
    - Seit dem Umbau auf Auto-Konfigurationen entsteht die Factory im Host über die `@Bean`-Methode `resilient*ClientFactory(...)`.
    - Die Tests, die Aufrufe machen (Host-Customizer-Header, Boots Observation, legatiums Exchange-Event, Engine-Isolation) und der Test „fehlender Builder lässt den Start scheitern", bauen sie dagegen per `withBean(...)` über den Konstruktor.
    - `MeshResilienceShippedConfigTest` bootet die Auto-Konfiguration, macht aber keinen Aufruf.
    - Mutationen, die grün bleiben: in der `@Bean`-Methode `WebClient.builder()` statt des injizierten Builders übergeben, den Builder aus einem `ObjectProvider` mit Fallback nehmen, `meterRegistry`/`validatorFactory`/`resourceLoader` auf `null` setzen.
    - Auslösebedingung: eine Änderung an der Auto-Konfiguration.
    - Auswirkung: Im Host verlieren alle Clients still legatiums Exchange-Event, die Host-Customizer und den Client-Span. Genau davor zu schützen, behauptet die KDoc des Wiring-Tests.
  - Fix-Strategie: Einen Kontext je Stack über die echte Auto-Konfiguration laden, der einen Aufruf macht und Header, Meter und Exchange-Event prüft, plus einen Kontext ohne Builder-Bean.

- [x] 4. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClient.kt:259-265` — Testlücke in `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientIntegrationTest.kt`] {Medium} {Confidence: high} {test quality / coverage gap}
  - Symptom → Ursache:
    - Deserialisierung und Validierung laufen bewusst NACH der Dekoration, damit ein Vertragsfehler den Breaker nicht öffnet.
    - Keine reaktive Testklasse übergibt eine `validatorFactory`; keine prüft fail-on-violation, die Report-only-Zeile oder einen kaputten 200-Body.
    - Der 204-Fall kann die Lage der `map` nicht unterscheiden, weil ein leerer Body sie nie erreicht.
    - Der blockierende Zwilling testet alle drei Fälle (`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt:979` ff., `:1303`).
    - Mutation, die grün bleibt: `readValue`/`validate` in `rawMono` vor `resilience.decorate` verschieben. Parse-Fehler und Vertragsverletzungen öffnen dann den Breaker gegen ein gesundes Backend; die Referenzkonfiguration hat die Validierung an.
    - Auslösebedingung: eine Umstellung der Pipeline.
    - Auswirkung: falsch offene Breaker.
  - Fix-Strategie: Die drei Fälle des blockierenden Zwillings reaktiv nachziehen, jeweils mit der Prüfung „Breaker zählt keine Fehler".

- [x] 5. [`./veredium-examples/src/test/kotlin/eu/inqudium/veredium/examples/async/AsyncExampleTest.kt:61` — Abbruch `:74`; Stubs `./veredium-examples/src/test/kotlin/eu/inqudium/veredium/examples/Sidecar.kt:37-48`; gegen `./veredium-examples/src/main/kotlin/eu/inqudium/veredium/examples/async/AsyncCustomerAdapter.kt:261-277`] {Medium} {Confidence: high} {test quality / false confidence}
  - Symptom → Ursache:
    - Der Test soll zeigen, dass das `cancel(true)` des Aufrufers auf dem kombinierten Future den laufenden Kundenaufruf abbricht.
    - Für die Bestellsuche zu `c-slow` gibt es aber keinen Stub; WireMock antwortet 404.
    - `thenCombine` endet daraufhin exceptionally, und `cancelling(customer, orders)` bricht den Kundenaufruf ohnehin ab.
    - Die erwartete Zeile „Cancelled request to backend 'customers'" erscheint deshalb auch, wenn der Abbruch des Aufrufers wirkungslos wäre.
    - Auslösebedingung: immer; der Test bliebe auch ohne Zeile 74 grün.
    - Auswirkung: Das Beispiel, das Hosts kopieren, beweist seinen Abbruchpfad nicht. Die Abbruch-Mechanik des Clients selbst ist im restclient-Modul getestet.
  - Fix-Strategie: Die Bestellsuche für `c-slow` ebenfalls lange verzögert stubben, sodass nur der Abbruch des Aufrufers den Aufruf beenden kann.

- [x] 6. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:501` — bis `:540`] {Medium} {Confidence: medium} {test quality / conditional false green}
  - Symptom → Ursache:
    - Der einzige Test, der die Rückgabe von Permit und Verbindung nach einem Interrupt WÄHREND des Body-Lesens prüft (`HandOffInputStream`, `AbortingResponse`), nutzt WireMocks `withChunkedDribbleDelay`.
    - Davor warnt `./docs/engine-behaviour.md` selbst („Test infrastructure"). Gemessen: Der Kopf kommt nach etwa 1,13 s, das Budget ist 1,5 s, und der Test prüft nicht, dass der Kopf angekommen ist.
    - Der legatium-Body-Fall beweist die Body-Phase, prüft aber nur das Ergebnis, nicht die Freigabe der Verbindung.
    - Auslösebedingung: ein belasteter Runner, auf dem der Kopf nach mehr als etwa 1,5 s ankommt. Der Fall testet dann stillschweigend die Kopf-Wartezeit.
    - Auswirkung: Eine Regression im Body-Abbruch (gehaltene Verbindung oder Lesethread auf Reactor Netty oder Jetty) bleibt auf solchen Läufen grün.
  - Fix-Strategie: Auf `DrippingBackend` umstellen (Kopf und erster Chunk sofort) oder die Ankunft des Kopfes vor Ablauf des Budgets verlangen.

### 🟢 Low

- [x] 7. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt:385-390`] {Low} {Confidence: low} {resources / concurrency}
  - Ein Interrupt, der den Worker trifft, nachdem die Kopf-Aufgabe ihre Antwort schon erzeugt hat, aber bevor `pending.get()` zurückkehrt, ruft `cancel(true)` auf einer erledigten Aufgabe auf (No-op).
  - Die `ClientHttpResponse` wird dann nie geschlossen. Spring abonniert den Inbound erst in `getBody()`, deshalb bleibt die Verbindung im Pool gehalten.
  - Auslösebedingung: nur nebenläufig, ein Zeitfenster im Mikrosekundenbereich. Am ehesten, wenn ein Aufrufer einen Geschwister-Aufruf abbricht (wie `AsyncCustomerAdapter.overview`).
  - Auswirkung: ein geleaster Slot je Treffer; der Shutdown wartet bis zur Frist darauf.
  - Fix-Strategie: Im Interrupt-Zweig eine schon erzeugte Antwort abholen und schließen, statt sie zu verwerfen.

- [x] 8. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:306-316` — mit `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:174`] {Low} {Confidence: high} {maintainability / logging}
  - Die Drain-Zeile von Reactor Netty nennt als „within" den RESTBETRAG der geteilten Frist, die anderen Engines nennen das Drain-Fenster. Nach einer vorangehenden Engine steht dort etwa „within PT0.3S" statt des Fensters.
  - Ist die Frist schon verbraucht (Restbetrag 0), läuft `timeout(Duration.ZERO)` sofort ab: Es kommt eine WARN-Zeile auch für einen leeren Pool, und das Warten auf die Disposal wird abgebrochen.
  - Die Zahl der laufenden Exchanges fehlt („the exchanges"), obwohl der Pool Metriken hat.
  - Fix-Strategie: Das Fenster melden wie die anderen Engines, bei verbrauchter Frist nicht warten, und die Zahl aus den Pool-Metriken nehmen.

- [x] 9. [`./veredium-restclient/src/test/java/eu/inqudium/veredium/restclient/BoundedBodyReaderFuzzTest.java:87`] {Low} {Confidence: high} {test quality / red without defect}
  - Invariante (3), „readTruncated nimmt höchstens `limit` Bytes", widerspricht dem gewollten Probe-Read in `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReader.kt:100`.
  - Bei deklarierter Länge = Limit und längerem Body (lügender Peer) werden `limit + 1` Bytes entnommen. Reproduziert.
  - Auslösebedingung: nur im nächtlichen `JAZZER_FUZZ=1`. Jazzers Vergleichs-Hooks steuern auf `contentLength == limit` zu.
  - Auswirkung: kein Produktionsrisiko, aber ein falscher Fuzz-Fund.
  - Fix-Strategie: In diesem Fall `limit + 1` zulassen und die Eingabe als Seed aufnehmen.

- [x] 10. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:229` — und `:367`; `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt:739`] {Low} {Confidence: medium} {test quality / coverage gap}
  - Alle Response-Timeout-Fälle verzögern den Kopf (`withFixedDelay`). Die in `./docs/engine-behaviour.md` zugesagte Body-Phasen-Sprosse ist ungetestet: JDK/Jetty über Kopf und Body, Reactor Netty „bis die Antwort vollständig ist", Apache je Lesevorgang.
  - Ein Upgrade, das eine Sprosse zur reinen Kopf-Grenze macht, bleibt unbemerkt; der TimeLimiter greift später und der Breaker sieht einen `TimeoutException`.
  - Fix-Strategie: Ein Fall je Engine mit einem Raw-Socket-Backend, das Kopf und ersten Chunk sendet und dann schweigt.

- [x] 11. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt:458`] {Low} {Confidence: medium} {robustness / coverage gap}
  - `status.isError` schickt 1xx/3xx auf den Erfolgspfad. Da alle Engines keine Redirects folgen, wird ein 3xx des Sidecars als 2xx-Body dekodiert oder scheitert als `EmptyResponseBodyException`, und der Breaker zählt einen Erfolg.
  - Kein Test legt fest, was ein 3xx ergeben soll.
  - Fix-Strategie: Den Vertrag festlegen (vermutlich eine Fehlerabbildung) und ihn mit einem 302-Fall je Engine pinnen.

- [x] 12. [`./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/engine/EngineSet.kt:71`, `:81` — Test `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/engine/EngineSetTest.kt:111`; `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/engine/EngineDrain.kt:26-76`; `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/client/MeshClientPlan.kt:58`] {Low} {Confidence: high} {test quality / coverage gap}
  - Drei neue Core-Bausteine haben keinen eigenen Core-Test ihrer riskanten Teile:
    - Die Sperre von `EngineSet` ist nur einfädig getestet; ihr Entfernen bleibt grün, und die zugesagte Idempotenz ist ungeprüft.
    - `EngineDrain` (überlaufsichere Fristprüfung, Klemmung von `remaining()`, Wortlaut ohne Zählung) wird nur indirekt über Echtzeit-Integrationstests geprüft.
    - `MeshClientPlan.drainWindow` wird nur mit einem Endpunkt geprüft; `max` vs. `min`/`first` und der Fallback ohne Endpunkte sind ununterscheidbar.
  - Fix-Strategie:
    - Ein Nebenläufigkeitstest von `EngineSet` mit einem an einem Latch gehaltenen `use {}`.
    - Ein `EngineDrainTest` mit Fake-Uhr und vorab gesetztem Interrupt.
    - Ein `MeshClientPlan`-Test mit zwei Endpunkten und einem ohne Endpunkte.

- [x] 13. [`./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecoratorTest.kt:685` — und `:681`] {Low} {Confidence: medium} {test quality}
  - `:685` pinnt den `newIncompleteFuture`-Override (`./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecorator.kt:345`). Der JDK liefert dort ohnehin eine einfache `CompletableFuture`, deshalb macht keine plausible Mutation den Test rot.
  - `:681` liest „der Breaker zählte nichts", bevor der Wartende sein Ergebnis sicher aufgezeichnet hat. Das Entfernen von `InterruptedException` aus `SELF_INFLICTED_FAILURES` wird so nur mit hoher Wahrscheinlichkeit erkannt, nicht deterministisch.
  - Fix-Strategie: `:685` als Dokumentation des JDK-Verhaltens kennzeichnen oder samt Override entfernen; in `:681` auf das Breaker-Ereignis warten.

- [x] 14. [`./veredium-examples/src/test/kotlin/eu/inqudium/veredium/examples/decorator/DecoratorExampleTest.kt:112` — und `:145`; gegen `./veredium-examples/src/main/kotlin/eu/inqudium/veredium/examples/decorator/DecoratorConfiguration.kt:72-75`] {Low} {Confidence: medium} {test quality / flakiness, coverage gap}
  - `:112` verlangt, dass der Socket-Timeout von 700 ms vor dem TimeLimiter von 1 s greift. Eine GC-Pause oder CI-Last von mehr als etwa 300 ms dreht die Reihenfolge um, und der Test wird rot ohne Defekt.
  - Der Hauptzweck des Breaker-Customizers im Beispiel, `SELF_INFLICTED_FAILURES` zu ignorieren, ist in keinem Beispiel-Test gepinnt; nur `UnknownCustomerException` ist es.
  - Fix-Strategie: mehr Abstand im Testprofil; im Abbruch-Test zusätzlich „Breaker zählt keine Fehler" prüfen.

- [x] 15. [`./veredium-test-support/src/main/kotlin/eu/inqudium/veredium/testsupport/Backends.kt:41-62`, `:133`, `:148`] {Low} {Confidence: medium} {test quality / helper semantics}
  - `DrippingBackend.connectionDropped` hat zwei blinde Flecken:
    - Es zählt nicht herunter, wenn der Client vor dem vollständigen Kopf schließt.
    - Es zählt bei jeder `IOException` herunter, auch beim eigenen `close()` des Backends.
  - `SocketBackend.close()` kann einen angenommenen, noch nicht registrierten Socket verpassen.
  - Die Raw-Socket-Backends, auf denen die heikelsten Beweise beider Suiten beruhen, haben keinen Selbsttest.
  - Auslösebedingung: seltene Scheduling-Rennen. Ein Beispiel ist der Abbruch-Test in `./veredium-examples/src/test/kotlin/eu/inqudium/veredium/examples/decorator/DecoratorExampleTest.kt:163-169`, der auf `connections == 1` statt auf den empfangenen Kopf gattert.
  - Fix-Strategie: die Latch-Semantik schärfen, das Registrierungsrennen schließen und Selbsttests in `./veredium-test-support/src/test/` ergänzen.

- [x] 16. [Schwache Zusicherungen, gesammelt — siehe Muster P2 in Abschnitt 6] {Low} {Confidence: high} {test quality / weak assertion}
  - `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/LegatiumExchangeLoggingIntegrationTest.kt:194` und `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/LegatiumExchangeLoggingIntegrationTest.kt:191` (auch `:256`, `:288`, `:383`, `:410`): Der Name verspricht „exactly one event", geprüft wird `awaitEvents().last()`.
  - `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt:954`: „from the call's own clock" prüft nur die Form `\d+ms`; die injizierbare Uhr des Clients setzt kein Test.
  - `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt:1583`: Der Evictor-Fall prüft nicht vorab, dass die Verbindung im Pool lag.
  - `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientIntegrationTest.kt:529-549`: Der Fall „keine Meter-Registry" prüft eine Registry, die die Factory nie sieht (Tautologie). Ein Fallback auf `Metrics.globalRegistry` bliebe grün.
  - `./veredium-examples/src/test/kotlin/eu/inqudium/veredium/examples/blocking/BlockingExampleTest.kt:60`, `./veredium-examples/src/test/kotlin/eu/inqudium/veredium/examples/reactive/ReactiveExampleTest.kt:66`: Die Budgets werden nur auf `isPositive` geprüft, obwohl die Werte aus `application.yml` bekannt sind. Der 404-Stub (`Sidecar.kt:37`) hat keinen Host-Matcher.
  - Fix-Strategie: genaue Werte bzw. `single()` nach einem Abschluss-Zaun verlangen; die Tautologie gegen `Metrics.globalRegistry` richten.

- [x] 17. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/LoggingLevelContractTest.kt:23-24` — und `:135`; `./veredium-webclient/src/test/resources/application.yml:20`] {Low} {Confidence: high} {test quality / misplaced evidence}
  - Der Vertragstest prüft die `logging.level`- und `adapter-logging`-Blöcke von `application-test.yml`, die kein Kontext lädt. Die Referenzkonfiguration, die Hosts kopieren, ist nicht gepinnt.
  - Die beiden Dateien widersprechen sich beim Body-Logging (`on-failure` vs. `always`).
  - Fix-Strategie: den Vertrag gegen die Referenzkonfiguration laufen lassen.

- [x] 18. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:762-769`; `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ApacheEngineTransportTest.kt:23`, `:67-75`] {Low} {Confidence: medium} {test quality / flakiness}
  - „Loops noch nicht freigegeben" wird nach einer Drain-Schranke von 50 ms gelesen; ein Stillstand über 50 ms macht den Fall rot.
  - `ApacheEngineTransportTest` nutzt Logbacks nicht threadsicheren `ListAppender` auf einem Transport mit echten I/O-Workern. Ein später `ClosedSelectorException`-Bericht ergibt dann `[DEBUG, DEBUG, ERROR]` statt `[DEBUG, ERROR]`.
  - Laut `./CONTRIBUTING.md` gehört hier die threadsichere Erfassung aus `veredium-test-support` hin.
  - Fix-Strategie: eine längere Verzögerung oder virtuelle Zeit; `CapturingAppender` nutzen und erst nach dem Ende der Engine-Threads leeren.

- [x] 19. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:156` — und `:167`; Test nur `WebClientMetricsTest.kt`] {Low} {Confidence: medium} {test quality / coverage gap (metric cardinality)}
  - Nur die reine Funktion `uriTagger` ist getestet. Dass Reactor Netty den Tagger bekommt und mit Pfad statt voller URI aufruft, ist ungeprüft; `.metrics(true) { it }` bliebe grün.
  - Fix-Strategie: ein Reactor-Netty-Aufruf, der den `uri`-Tag gleich der Vorlage verlangt.

- [x] 20. [`./docs/engine-behaviour.md:662` — und `:687`, §12; `./CHANGELOG.md:246`] {Low} {Confidence: high} {maintainability / documentation drift}
  - `:662` nennt die Hand-off-Korrektur „pinned by `ReactorNettyHandOffTest`"; siehe Befund 2.
  - `:687` nennt `BlackholeBackend`, das es nicht mehr gibt; der blockierende Connect-Fall nutzt `UnansweredConnectBackend`.
  - §12 sagt, der unterbrochene Shutdown sei auf jeder Engine gepinnt; siehe Befund 1.
  - `./CHANGELOG.md:246` sagt noch, ein Host mit beiden Modulen schließe eine Auto-Konfiguration aus. Seit ADR-0003 laufen beide Stacks in einem Kontext.
  - Fix-Strategie: die Stellen nachziehen, sobald die Befunde 1 und 2 behoben sind.

## 6. Systemic patterns

- **P1 — Shutdown-Tests prüfen die Engine-Seite, nicht die Aufruferseite.** Etwa 6 Fälle (gezählt über die
  Shutdown-, Frist- und Interrupt-Fälle in `./veredium-*/src/test/kotlin/**/*EngineIntegrationTest.kt`) prüfen
  „geschlossen" und die WARN-Zeile, aber nicht, wie und wann der laufende Aufruf endet.
  - Folgen: Befund 2 (Hand-off ungebunden), Befund 1 (Lücke beim reaktiven Interrupt) und die fehlende
    End-to-End-Prüfung, dass reaktive Reactor-Netty-Aufrufe mit den Loops enden.
  - Vertreter: `RestClientEngineIntegrationTest.kt:833`, `:867`; `WebClientEngineIntegrationTest.kt:995`.
- **P2 — Testnamen versprechen mehr, als die Zusicherung prüft.** Etwa 10 Stellen (grep nach `.last()` auf
  `awaitEvents`, `isPositive` auf Budgets, Formprüfungen per Regex; Liste in Befund 16): „exactly one", „own clock",
  „same budgets", „no registry".
  - Einzeln je Low; zusammen schwächen sie die Rationale-Blöcke, auf die das Projekt baut.
- **P3 — Testkontexte bauen die Factory selbst statt über die Produktions-Auto-Konfiguration.** 4 Kontexte, die
  Aufrufe machen (`grep -rn 'withBean(Resilient' veredium-*/src/test`), auf beiden Stacks.
  - Der Produktionsweg (`@Bean`-Methode) wird nie mit einem Aufruf ausgeübt (Befund 3).
- **P4 — Asymmetrische Abdeckung der Zwillinge.** Ein Stack hat den Fall, der andere nicht:
  - reaktive Validierung und Parse-Fehler (Befund 4);
  - Reactor Netty im Interrupt-Fall (Befund 1);
  - die Body-Phasen-Sprosse (Befund 10).
  - Basis: Vergleich der Fall-Listen der Zwillingsklassen; etwa 3 Fallgruppen.

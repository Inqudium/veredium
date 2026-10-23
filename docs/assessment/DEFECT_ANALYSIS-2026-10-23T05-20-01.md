# Code & Defect Analysis: veredium (veredium-core, veredium-restclient, veredium-webclient)

1. Identification of the codebase
   - **Repository:** `https://github.com/Inqudium/veredium.git`
   - **Commit hash:** `6d423d61cdacb0be8d4b466483b69378484e9a70` (full)
   - **Reference (branch/tag):** `refs/heads/main` / tag: keiner (`git describe` liefert nur den Hash); Arbeitsbaum sauber
2. Scope of the analysis
   - **Included:** `./veredium-core/src/main/kotlin/`, `./veredium-restclient/src/main/kotlin/`, `./veredium-webclient/src/main/kotlin/` (Produktionscode, 39 Dateien). **Testcode ist Teil der Analyse**: `./veredium-*/src/test/kotlin/`, `./veredium-*/src/test/java/` (Jazzer-Targets) und `./veredium-*/src/test/resources/`.
     - Schwerpunkt ist das Delta seit dem Vorbericht, `1b72625602728c0ed578289986fc99e512a7c073..6d423d61cdacb0be8d4b466483b69378484e9a70`: die Behebungen aus `c399b38` sowie `5addc1e`, `ca7955a` und `9823825` (27 Dateien unter `src/`, +2,6 k / −0,3 k Zeilen).
     - Ergänzend gelesen: `./docs/engine-behaviour.md` (dokumentierter Soll-Zustand je Engine), `./CONTRIBUTING.md` (Konventionen) und `./docs/adr/`.
   - **Excluded:** generierte Artefakte (`./**/target/`), das Build-only-Modul `./veredium-coverage/` (kein Code), die MkDocs-Site außer `./docs/engine-behaviour.md`, `./docs/adr/` und `./docs/assessment/`, sowie `./.github/`
3. Analysis environment & tools
   - **Target environment:** Java 21 (Bytecode-Ziel), Build-JDK 24+ (lokal Oracle JDK 26.0.1); Kotlin 2.4.21; Spring Boot 4.1.1 / spring-web 7.0; Resilience4j 2.4.0; Apache HttpClient 5.6.4 / HttpCore 5.4.3, Jetty Client 12.1, Reactor Netty 1.3; legatium 1.2.1
   - **Build system:** Maven 3.9.15, Multi-Modul-Reaktor
   - **Analysis tools used:**
     - Manuelles Review (Claude Code, Opus 5.5) des gesamten Produktionscodes des Deltas und der Score-4/5-Dateien.
     - Zwei Sub-Agenten für die Testsuiten von `veredium-restclient` und `veredium-webclient`. Ihre Befunde wurden an den genannten Zeilen gegengelesen; einer wurde per Einzel-Testlauf widerlegt (siehe Abschnitt 2).
     - Quellen von httpcore5 5.4.3 (`SingleCoreIOReactor`, `AbstractHttp1StreamDuplexer`, `ClientHttp1StreamDuplexer`) aus dem lokalen Maven-Repository.
     - `mvn -q -o clean verify` (grün) mit JaCoCo-Aggregat sowie ein JaCoCo-Lauf eines Einzeltests; dazu `grep`.
     - Vorhandene statische Analyse: ktlint, CodeQL (CI), JaCoCo, Jazzer. Nicht vorhanden sind detekt, SpotBugs/Error Prone, ArchUnit und Sonar.
4. Placement & Output
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/veredium` (absoluter Bezugspunkt; alle relativen Pfade beziehen sich darauf)
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-23T05-20-01.md` (relativ zum workdir; Präfix + ISO-8601-Zeitstempel in UTC wie bei den Vorberichten). Die Vorberichte `./docs/assessment/DEFECT_ANALYSIS-2026-10-21T20-14-02.md` und `./docs/assessment/DEFECT_ANALYSIS-2026-10-22T14-01-49.md` bleiben unverändert.
   - **Scope root (relative to the workdir):** `./` (Modulwurzeln `./veredium-core/`, `./veredium-restclient/`, `./veredium-webclient/`)
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>`. Die gemeinsamen Präfixe werden ausgeschrieben: `src/main/kotlin/eu/inqudium/veredium/<modul>/` bzw. `src/test/kotlin/eu/inqudium/veredium/<modul>/`.

---

## 1. Executive Summary

Der Code ist in gutem Zustand. Von den 24 Befunden des Vorberichts sind die 23 abgehakten gegen den aktuellen Code
bestätigt, und zwar im Produktionscode wie in den Tests. Offen bleibt nur Nr. 16, der legatium betrifft (legatium
1.2.1 unverändert).

**Es gibt keinen Critical- und keinen High-Befund.** Der Kern (blockierender Decorator, Leiter, Verdicts) ist
unverändert und weiterhin solide.

Der einzige neue Produktionsbefund von Gewicht betrifft den **Release der reaktiven Apache-Engine** (Befund 1). Er
ruft sofort `initiateShutdown()` auf, statt erst wie Jetty auf die laufenden Exchanges zu warten. Ob Apaches
Graceful-Shutdown diese Exchanges zu Ende laufen lässt, ist laut dem eigenen Test „von Lauf zu Lauf verschieden“.
Läuft er vor der Frist aus, bricht `abortInFlight()` Aufrufe ab, die noch Zeit gehabt hätten. Das widerspricht dem
dokumentierten Soll („every engine first lets its in-flight exchanges finish“, `./docs/engine-behaviour.md` §12).

Alles Übrige sind Testlücken:

- **Webclient-Engines:** Hier fehlt, was der Vorbericht für die blockierenden Engines nachgezogen hat: ein tröpfelnder
  Übergröße-Body (Befund 2) und Tests der Pool-Verdrahtung über `max-connections` hinaus (Befund 3).
- **Zeitzusagen der Leiter:** Viele sind nur nach oben begrenzt (Befunde 4, 5). Eine Mutation, die der
  Transport-Sprosse den falschen Wert gibt, bleibt dadurch grün.

**Test-Urteil (kompakt):**

- **Verlässlichkeit als Sicherheitsnetz:** hoch für Decorator, Verdicts, Leiter, Bounded Reader, Isolation und
  Shutdown des blockierenden Stacks.
  - Die Vorberichts-Fixes halten der Mutationsprüfung stand. Die Isolation wird jetzt auf dem Draht bzw. am Pool
    beobachtet, Drain, Frist und Interrupt laufen pro Engine, und `DrippingBackend` belegt den Abbruch statt Drain.
  - Es gibt kein `Thread.sleep`, keine Mocks und eine injizierbare Uhr. Die riskante Logik ist strukturell
    deterministisch prüfbar.
  - Falsche Sicherheit entsteht dort, wo Zeitfenster nur eine obere Schranke haben und wo reaktive Engine-Zusagen
    nur auf dem schnellen Pfad geprüft werden.
- **Pyramide:** Der Core hat eine schnelle Unit-Basis (196 Tests). Die Client-Module sind kopflastig, mit 233 bzw.
  215 Tests, davon rund zwei Drittel der Zeilen Real-HTTP-Integration über WireMock oder Raw Sockets, je Engine
  parametrisiert. Für Transport-Verdrahtung ist das angemessen. Die Aggregat-Coverage beträgt Instruction 96,0 %,
  Branch 86,5 % und Line 97,5 %; die Branch-Lücken sitzen weiter in den Engine-Transports.
- **Gravierendste Lücken:**
  - Der reaktive Apache-Release drainiert nicht nachweislich, und reaktiv gibt es den Test „in-flight finishes“ nur
    für Jetty (Befund 1).
  - Der Body-Phasen-Abbruch auf dem Webclient ist ungetestet; es gibt dort keinen einzigen tröpfelnden Body (Befund 2).
  - Die Pool-Knöpfe des Webclients sind nur als Property-Werte geprüft, nicht als Engine-Verdrahtung (Befund 3).
  - Response- und Connect-Timeout sind nur nach oben begrenzt (Befunde 4, 5).
  - Ein Transportfehler mitten im Body ist auf keiner Engine getestet. Der `ExecutionException`-Zweig des
    Reactor-Netty-Hand-offs ist nie gelaufen (Befund 6).

## 2. Scope & Methodology

- **Analysiert:** alle drei Module, Produktions- und Testcode. **Testcode war im Scope**, als eigener
  Analysegegenstand (Mutations-Gedankenexperiment) und als Risikoindikator.
  - Den Produktionscode des Deltas und alle Score-4/5-Dateien habe ich vollständig selbst gelesen. Der unveränderte
    Core-Code (Leiter, Verdicts, Customizer) wurde nur gegen das Delta abgeglichen; die beiden Vorberichte haben ihn
    zuvor vollständig geprüft.
  - Die zwei Client-Testsuiten haben Sub-Agenten gelesen. Ihre Befunde wurden an den Zeilen gegengeprüft und nach
    Phase-3-Kriterien neu gewichtet.
- **Widerlegter Agenten-Befund:** Der Restclient-Agent meldete als **High**, der Fall „should release the worker
  promptly when the budget expires while the body is still arriving“
  (`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:439`)
  erreiche die Body-Phase nie, weil WireMocks Gzip-Handler den Head zurückhalte.
  - Ein Lauf nur dieses Tests mit frischer JaCoCo-Datei zeigt, dass `HandOffInputStream.read` betreten wird und der
    erste Read nie normal zurückkehrt (`ReactorNettyEngineTransport.kt:306` ausgeführt, `:308` nicht). Danach läuft
    der Interrupt-Zweig `:309-314`.
  - Der Head kommt also an, und der Test prüft auf Reactor Netty tatsächlich den Body-Read. Der Befund ist verworfen.
- **Erkannter Stack:** Spring Boot 4.1 als Bibliothek, ohne Controller und ohne `src/main/resources`.
  - Blockierend: Spring `RestClient` über vier `ClientHttpRequestFactory`s (Apache 5 classic als Default, JDK, Jetty
    12, Reactor Netty).
  - Reaktiv: WebFlux-`WebClient` über vier `ClientHttpConnector`s (Reactor Netty als Default, JDK, Jetty, Apache 5
    async).
  - Virtual Threads dienen als Worker des Decorators, als JDK-Executor und als Body-Hand-off. Coroutines gibt es nur
    auf Aufruferseite. Persistenz und Messaging gibt es nicht.
  - Einstiegspunkte: `@PreDestroy` (Factories, Decorator), `SmartInitializingSingleton` (Startup-Guard),
    `@EventListener` (Leiter-Logger), `BeanRegistrar` (Customizer).
- **Lokale Verifikation:** `mvn -q -o clean verify` ist grün (core 196, restclient 233, webclient 215 Tests; 0
  Fehler, 0 übersprungen).
  - Für die Gegenprobe des Agenten-Befunds wurden `veredium-core` und `veredium-restclient` per `mvn -o install
    -DskipTests` ins lokale Maven-Repository installiert (SNAPSHOT, Nebenwirkung außerhalb des Repos). Danach lief
    ein einzelner Test mit JaCoCo.
  - `./veredium-restclient/target/` enthält deshalb jetzt die Coverage nur dieses Einzellaufs. Für Coverage-Zahlen
    `mvn clean verify` erneut ausführen.
  - **Reproduktionstests wurden nicht geschrieben**: Die Freigabe steht laut Prompt aus.
- **Vorberichte:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-22T14-01-49.md` wurde Befund für Befund
  gegengeprüft.
  - Produktion: 1 (`AbortingRequest` für Apache und JDK, `EngineTransport.kt:226-290`), 2 (`max-idle-time` > 0 in
    beiden Properties), 3 (`shutdownAll`), 9 (`createAll`), 10 (gemeinsame Frist im Webclient), 11 (`released`-Flag),
    12 (`jettyQueueCapacity`), 13 (`MeshConnectionPools`), 14 (Hook nur nach Cancel) und 15 (`boundPoolMeters`) sind
    behoben.
  - Tests: 4, 5, 6, 7, 8, 17, 18, 19, 20, 21, 22, 23 und 24 sind wirksam nachgezogen. 4 und 5 gelten im Webclient
    nur teilweise (hier Befunde 3 und 9).
  - 16 bleibt offen (hier Befund 19).
- **Blinde Flecken:**
  - Es gab keine Messung unter Last.
  - Das Verhalten von Apaches Graceful-Reactor-Shutdown mit einem HTTP/1-Exchange im Flug wurde nur am Quellcode
    beurteilt, nicht reproduziert (Befund 1).
  - legatium wurde nur über Doku und Tests beurteilt.
  - TLS liegt außerhalb des Scopes.

## 3. Statistics

| Severity | Anzahl |
|---|---|
| 🔴 Critical | 0 |
| 🟠 High | 0 |
| 🟡 Medium | 6 |
| 🟢 Low | 13 |
| **Summe** | **19** |
| Systemische Muster | 3 |

Kategorien: test quality 16, resources 1, robustness 1, maintainability 1.

## Bearbeitungsstand (2026-10-23, nach der Analyse)

Befund 1 ist behoben, auf Branch `fix/reactive-apache-drain` über Commit
`6d423d61cdacb0be8d4b466483b69378484e9a70`. Ein `mvn -o clean verify` über alle Module ist grün: 651 Tests
(core 196, restclient 233, webclient 221 + 1 im eigenen JDK-Flag-Lauf; mit dem restclient-Zwillingsfall restclient 237), 0 Fehler, ktlint sauber. Die Änderungen
stehen in `./CHANGELOG.md` unter „Fixed“.

**Die Gegenprobe hat Befund 1 präzisiert:**

- **Nicht bestätigt:** Ein Exchange, der beim Release schon auf einer Verbindung läuft, wird auf Apache nicht
  abgeschnitten. Der neue, über alle Engines parametrisierte Fall „should let an in-flight exchange finish before
  closing the engine“ war vor dem Fix 8 von 8 Mal grün. Falsch waren hier die `Assumption:` im Code und
  `./docs/engine-behaviour.md`, die das Gegenteil behaupteten.
- **Bestätigt:** Ein Aufruf, der beim Release noch auf eine Verbindung **wartet** (`max-connections` erreicht),
  scheiterte deterministisch (3 von 3).
  - Apache: `IOReactorShutdownException` nach ≈0,6 s bei 5 s Frist. Ein Reactor im `SHUTTING_DOWN` öffnet keine
    Verbindung mehr.
  - Reactor Netty: `PoolShutdownException` sofort, weil `disposeLater()` ohne `disposeTimeout` wartende Acquires
    abweist.
  - Der neue Fall „should let a call still waiting for a connection finish before closing the engine“ läuft auf
    allen vier Engines. Vor dem Fix war er auf Apache und Reactor Netty rot, danach je dreimal grün.

**Fix:**

- Apache: `ApacheTransport.release()` wartet zuerst wie Jetty auf das leere `inFlight`-Set (bis zur Frist oder zum
  Interrupt) und fährt erst dann den Reactor herunter.
- Reactor Netty: Der Pool wird mit `disposeTimeout` = Drain-Grenze der Factory gebaut. Seine Disposal bedient so die
  wartenden Acquires und endet erst, wenn alle Verbindungen zurück sind.

**Folgeänderungen:**

- Der Fristfall erlaubt für Apache keine zweite WARN-Zeile mehr, weil das Ergebnis nicht mehr schwankt.
- Der Test, der pinnte, dass Reactor Netty beim Shutdown nicht wartet
  (`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientIntegrationTest.kt`),
  prüft jetzt das Warten.
- KDocs, `./docs/engine-behaviour.md` §12 und `./veredium-webclient/docs/webclient-config-guide.md` sind angepasst.

**Folgen und Restrisiko:**

- Mit `disposeTimeout` loggt Reactor Netty einen Pool, der über die Drain-Grenze hinaus nicht leer wird, selbst
  mit ERROR („didn't shut down gracefully“). Vorher hat die Factory ihn schon per WARN gemeldet.
- Die blockierenden Engines (`veredium-restclient`) sind nicht betroffen: Der Zwillingsfall „should let a call
  still waiting for a connection finish before closing the engine“ ist auf allen vier Engines grün, je dreimal.
  Gegenprobe: Ohne `drainOrReport` im Reactor-Netty-Transport wird er rot. Die Übergabe einer zurückgegebenen
  Verbindung an den wartenden Acquire hält die Zahl der vergebenen Verbindungen über null.

### Befunde 2–19 (2026-10-23, Branch `fix/defect-analysis-2026-10-23`)

18 von 19 Befunden sind behoben. Ein `mvn -o clean verify` über alle Module ist grün: 690 Tests (core 196,
restclient 249, webclient 245), 0 Fehler, ktlint sauber. Jeder neue oder verschärfte Test wurde mit der Mutation
gegengeprüft, die der Befund als „bleibt grün“ nannte; jede wird jetzt rot. Die Änderungen stehen in
`./CHANGELOG.md` unter „Fixed“.

Neue Defekte, die die Tests aufgedeckt haben:

- **2** — Der neue Fall mit tröpfelndem Body fand einen Defekt auf **Jetty** (reaktiv): Ein abgebrochener Body
  behielt seine Verbindung, solange der Peer sendet. Jetzt bricht `AbortJettyOnBodyCancel` den nativen Request ab.
  Der **JDK-Client** kann einen Exchange nach dem Head gar nicht abbrechen (`HttpBodySubscriberWrapper.onCancel`
  meldet nur ab). Der Fall pinnt das, und Startup-Zeile wie `./docs/engine-behaviour.md` nennen es. Apaches
  `AbortOnBodyCancel` ist auf beiden Body-Pfaden (Codec-Abbruch, TimeLimiter) nicht nötig: Ohne ihn bleiben beide
  Fälle grün.
- **5** — Der blockierende Connect-Timeout-Fall lief auf Apache, Jetty und Reactor Netty in ein **refused connect**.
  `localhost` löste auf `::1` auf, das Blackhole lauscht auf `127.0.0.1`. Der Fall war grün, ohne dass der
  Connect-Timeout lief. Er zielt jetzt auf die Adresse und verlangt die engine-eigene Timeout-Exception.
- **6** — Der neue Cut-off-Fall fand einen Defekt auf **Reactor Netty** (blockierend): Ein mitten im Body
  abgebrochener Peer kam als ungeprüfte `ReactiveException` an, nicht als `ResourceAccessException`. Springs
  `SubscriberInputStream` wirft ungeprüft. Der Body-Read packt die `IOException` jetzt aus, wie Spring es für den
  Head tut.

Abweichend von der Fix-Strategie:

- **3** — Statt die Werte zurückzulesen: Apache und Reactor Netty geben Idle- und Lebensdauer nicht heraus.
  Gepinnt wird deshalb auf dem Draht, mit einem Backend, das seine Verbindungen zählt.
- **9** — Die WARN-Zeilen der JDK- und Apache-Releases nannten einen Interrupt nicht als Grund. Sie tun es jetzt,
  sonst wäre der parametrisierte Interrupt-Fall nicht formulierbar gewesen.
- **10** — Statt eines deterministischen Interleavings: `AbortableExchange` ist jetzt ein einziger atomarer
  Zustand. Die Reihenfolge, die der Test nicht fangen konnte, gibt es nicht mehr.
- **13** — Ein Listener pro Testklasse verteilt an pro Test registrierte Latches und wird vor jedem Test geleert.
  Das betrifft 16 Listener in 6 Klassen, beide Module.
- **17** — Die Fenster sind so breit, wie die Unterscheidung der beiden Timeouts zulässt; gemessen wird weiter ab
  dem Aufruf.

Offen:

- **19** — legatium#88 ist geschlossen (`580578e` auf legatiums `main`): Ein abgebrochener Aufruf wird am
  Interrupt-Flag des Workers erkannt, zusätzlich an einer `InterruptedException` in der Ursachenkette. Das deckt
  auch die `InterruptedIOException` des Hand-offs ab. Ein Release mit dem Fix gibt es noch nicht (zuletzt 1.2.1
  vom 2026-10-19). Zu tun bleibt das Anheben von `legatium.version`, sobald 1.2.2 erscheint.

Aufgefallen, nicht Teil der Befunde: Auf `main` warnt der Kotlin-Compiler in `BlockingResilienceDecoratorTest`
(5×) und `RestClientVerdictsTest` (3×) „Expression is unused“, in `WebClientEngineTransports` über Jettys
deprecated `transport`. Die Warnung in `ReactorNettyHandOffTest` aus PR #12 ist behoben.

## 4. File-Ranking (Phase 1)

**Produktionscode**

| Datei | Score | Begründung |
|---|---|---|
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt` | 5 | Vier Engines, Apache-Future-Capture, Release-Pfade mit Reactor-Status-Heuristik (Befund 1) |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt` | 5 | Body-Hand-off über Virtual Threads, Interrupt-Übersetzung, eigener Meter-Registrar, Disposal |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/EngineTransport.kt` | 5 | Neu: `AbortingRequest`/`AbortingResponse` für drei Engines, `shutdownAll`, `createAll` |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt` | 5 | Hot Path; Exchange, Bounded Read, async Fortsetzung |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecorator.kt` | 5 | Thread-Hand-off, Interrupt/Cancel (unverändert, keine Befunde) |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ApacheEngineTransport.kt`, `JettyEngineTransport.kt`, `JdkEngineTransport.kt` | 4 | ThreadLocal-Capture, Abort-Decorator, Drain |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt` | 4 | Engine-Auswahl, Lebenszyklus, Frist |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClient.kt` | 4 | Reaktive Kette, `onErrorDropped`-Hook, MDC-Snapshot |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReader.kt` | 4 | Peer-kontrollierte Grenze (gefuzzt); Wechselwirkung mit `AbortingResponse` (Befund 18) |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceStartupGuard.kt`, `MeshResilienceCustomizers.kt`, `BreakerVerdicts.kt`, `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/ResolvedTimeouts.kt` | 4 | Prädikat-Komposition, Pool-Prüfung je Engine, Leiter |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ReactiveResilienceDecorator.kt` | 3 | Operator-Reihenfolge (gepinnt) |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineChecks.kt`, `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/client/ClientCallLog.kt`, `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/MeshProperties.kt` | 3 | Fail-fast, Startup-Text, Level-Politik |
| `./veredium-*/src/main/kotlin/**/*Properties.kt` | 2 | Bind-Guards |
| `./veredium-*/src/main/kotlin/**/*Engine.kt`, `*Verdicts.kt`, `*Configuration.kt`, `*ObservationReport.kt`, `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/{BaseAdapter,Veredium}.kt`, `mesh/{MeshConnectionPools,MeshTimeoutConfig}.kt`, `validation/*`, `observation/*`, `client/{ResponseBodyMetrics,EmptyResponseBodyException}.kt` | 1–2 | Enums, deklarativ, Einzeiler |

**Testcode** (Gewicht als Sicherheitsnachweis)

| Datei | Score | Begründung |
|---|---|---|
| `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt` | 5 | Einziger Nachweis der Nicht-Default-Engines; Befunde 4, 5, 6, 11, 12 |
| `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt` | 5 | Dito reaktiv; Befunde 1, 2, 3, 5, 8, 9 |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecoratorTest.kt` | 5 | Echte Konkurrenz über Latches; keine Befunde |
| `./veredium-*/src/test/kotlin/**/Resilient*IntegrationTest.kt` | 4 | Breite Real-HTTP-Abdeckung; Befunde 7, 14, 16 |
| `./veredium-*/src/test/kotlin/**/*EngineIsolationTest.kt` | 4 | Isolation jetzt auf dem Draht belegt; keine Befunde |
| `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/AbortableExchangeTest.kt`, `EngineTransportsTest.kt`, `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/EngineTransportLifecycleTest.kt` | 3 | Neue Unit-Tests der Lebenszyklus-Bausteine; Befund 10 |
| `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReaderTest.kt`, `./veredium-*/src/test/java/**/*FuzzTest.java` | 3 | Trickle-Streams, Jazzer-Invarianten |
| `./veredium-*/src/test/kotlin/**/*PropertiesTest.kt`, `WebClientEngineConfigTest.kt` | 3 | Bindung, Fail-fast; tragen allein die Webclient-Pool-Knöpfe (Befund 3) |
| übrige Core-Unit-Tests, `*LoggingLevelContractTest.kt`, `*ObservationReportTest.kt` | 2 | Pinnen Konfiguration bzw. Startup-Text |

## 5. Findings

### 🔴 Critical

Keine.

### 🟠 High

Keine.

### 🟡 Medium

- [x] 1. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:578` — bis `:596`; Test `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:776-780`, `:815-820`; Soll `./docs/engine-behaviour.md` §12] {Medium} {Confidence: medium} {resources / robustness}
  - Der reaktive Apache-Release wartet nicht auf laufende Exchanges, bevor er den I/O-Reactor herunterfährt.
    - `release()` ruft sofort `client.initiateShutdown()` und dann `awaitShutdown(rest der Frist)` auf. Danach
      bricht `abortInFlight()` jeden Exchange ab, der noch im Set steht.
    - Jetty dagegen pollt erst `exchangesInFlight()` bis zur Frist (`:384-386`), und der JDK-Client wartet per
      `shutdown` + `awaitTermination`.
    - Ob Apaches Graceful-Shutdown einen HTTP/1-Exchange zu Ende laufen lässt, ist nicht geklärt:
      - httpcore5 5.4.3 schließt eine Verbindung im `GRACEFUL_SHUTDOWN` erst, wenn `inputIdle() && outputIdle()`
        gilt (`AbstractHttp1StreamDuplexer.java:395`, `ClientHttp1StreamDuplexer.java:300`). Das spricht fürs
        Warten.
      - Die eigene `Assumption:` bei `:588-590` und `./docs/engine-behaviour.md` behaupten dagegen, der
        Graceful-Shutdown schließe die Verbindungen laufender Exchanges.
      - Der Release-Test sagt ausdrücklich, ob der Shutdown „vor der Frist mit noch offener Verbindung endet,
        variiert von Lauf zu Lauf“, und akzeptiert deshalb beide WARN-Zeilen.
    - Ausführungsskizze:
      1. Der Context schließt, während ein Aufruf mit 2 s Restlaufzeit läuft, bei 10 s Frist.
      2. Endet `awaitShutdown` vorzeitig, meldet `abortInFlight()` den Abbruch, und der Aufrufer erhält eine
         Cancellation, obwohl der Backend-Call innerhalb der Frist fertig geworden wäre.
      3. Die WARN-Zeile „shut down with 1 exchange(s) still in flight“ ist dann der einzige Hinweis.
    - Einen reaktiven Test „should let an in-flight exchange finish“ gibt es nur für Jetty (`:837`). Für Apache, JDK
      und Reactor Netty ist das Soll aus §12 unbelegt.
    - Triggering condition: nur beim Shutdown mit laufenden Aufrufen auf der Nicht-Default-Engine `apache`. Ob es
      eintritt, ist nicht deterministisch.
    - Impact: Ein Graceful Shutdown (Rolling Deploy) schneidet Aufrufe ab, die das Soll zu Ende laufen ließe.
      Dokumentation und Assumption widersprechen sich.
  - Fix strategy: Vor `initiateShutdown()` wie bei Jetty auf das leere `inFlight`-Set bis zur Frist warten, dann
    erst den Reactor stoppen. Den Fall „in-flight finishes before the deadline“ über alle reaktiven Engines
    parametrisieren und die Assumption an die geprüfte Quelle anpassen.

- [x] 2. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:673` — und `:679-684` (`AbortOnBodyCancel`), gegen `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:349-372`] {Medium} {Confidence: high} {test quality / false confidence}
  - Die KDoc sagt „Both phases are covered“ (`:646-648`), doch der Abbruch in der Body-Phase ist auf keiner
    reaktiven Engine getestet.
    - Der Codec-Limit-Test schickt 4 KB in einem Stück. Wenn der Codec abbricht, ist der Exchange schon beendet.
    - Der TimeLimiter-Abbruchfall bricht vor dem Head ab.
    - `grep -rn "withChunkedDribbleDelay\|DrippingBackend()" veredium-webclient/src/test` liefert 0 Treffer.
  - Mutation: `.map { AbortOnBodyCancel(...) }` entfernen, oder ihr `doOnCancel(exchange::abort)`. Alles bleibt grün.
  - Das ist genau die Testform, die im Vorbericht (Befund 8) den echten Drain-Defekt des blockierenden Apache
    verborgen hat.
    - Triggering condition: Regression im Body-Abbruch, oder eine Engine, die einen abgelehnten Body zu Ende liest
    - Impact: Ein Übergröße- oder Timeout-Body hält Verbindung und Pool-Slot bis zum Response-Timeout. Die Zusage
      „cancellation aborts“ ist für die Body-Phase unbelegt.
  - Fix strategy: Den `DrippingBackend` des Restclients übernehmen. Je Engine einen tröpfelnden Body weit über
    `maxInMemorySize` schicken und prüfen, dass `DataBufferLimitException` vor dem Ende des Tröpfelns kommt und das
    Backend den Verbindungsabbau sieht.

- [x] 3. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:209` — `:211-213` (Reactor Netty), `:354`, `:356`, `:362` (Jetty), `:509`, `:542-545` (Apache); Startup-Zusagen `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineChecks.kt:147-215`] {Medium} {Confidence: high} {test quality / false confidence}
  - Auf dem Webclient sind nur `max-connections`, Timeouts, Host und gzip je Engine auf dem Draht belegt. Ungetestet
    als Engine-Verdrahtung sind:
    - `pool.max-idle-time`, `max-life-time`, `evict-in-background` und `pending-acquire-max-count`
    - Jettys `idleTimeout`, `maxRequestsQueuedPerDestination` und `maxDuration`
    - Apaches `setTimeToLive` und der Evictor (JaCoCo: der Evictor-Zweig `:543`/`:568` ist nur teilweise
      ausgeführt)
  - Diese Werte prüfen nur `WebClientPropertiesTest` (Bindung) und `jettyQueueCapacity` als reine Funktion
    (`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineConfigTest.kt:206-220`).
  - Mutation: `.maxIdleTime(...)`, `idleTimeout = ...`, `maxRequestsQueuedPerDestination = ...` oder
    `setTimeToLive(...)` löschen. Die Suite bleibt grün, und die Startup-Zeile meldet weiter „applies“.
  - Der Restclient hat diese Lücke mit dem Vorbericht-Fix 4 geschlossen; der reaktive Zwilling nicht.
    - Triggering condition: Regression in der Pool-Verdrahtung einer reaktiven Engine
    - Impact: Ein Pool läuft still mit Engine-Defaults (zum Beispiel Jetty-Queue 1024 statt der konfigurierten
      Grenze, oder ohne TTL), während das Log das Gegenteil sagt.
  - Fix strategy: Den Wert vom gebauten Engine-Objekt zurücklesen (billig). Wo das nicht geht, je Knopf einen
    Verhaltensfall schreiben, zum Beispiel dass eine Idle-Verbindung nach kurzer `max-idle-time` ersetzt wird.

- [x] 4. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:240` — und `:355`] {Medium} {Confidence: high} {test quality / false confidence}
  - Die Transport-Sprosse der Leiter ist nur nach oben begrenzt.
    - Fall `stalled` (Response-Timeout 700 ms): Er prüft nur `elapsed < 2,5 s`.
    - Fall `quick`/`patient`: Er prüft `quick < 1,5 s` und `patient ≥ 1,5 s`.
  - Mutation: Den Engines den Anker statt der abgeleiteten Sprosse geben, also `individualRequestTimeoutMs` statt
    `endpoint.responseTimeout` (`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ApacheEngineTransport.kt:183`,
    `JdkEngineTransport.kt:85`, `JettyEngineTransport.kt:77`, `ReactorNettyEngineTransport.kt:111`).
    - `stalled` scheitert dann nach ≈300 ms, `quick` ebenfalls.
    - `patient` scheitert nach ≈1,5 s, was die Untergrenze genau erfüllt.
    - Alles bleibt grün.
    - Triggering condition: Regression in der Übergabe der Leiter an eine Engine
    - Impact: Der Client gibt vor dem Sidecar auf. Der Aufrufer sieht ein Transport-Timeout statt der 504 des
      Sidecars, mit anderem Breaker- und Log-Bild. Die Leiter („transport > anchor“) ist dann verletzt, ohne dass ein
      Test es merkt.
  - Fix strategy: Zusätzlich `elapsed ≥ responseTimeout − wenige ms` prüfen. Eine Untergrenze ist auf einem
    belasteten Rechner sicher, weil ein Timeout nicht früher feuert.

- [x] 5. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:627` — bis `:628`; `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:1010`, `:1024-1025`] {Medium} {Confidence: medium} {test quality / false confidence}
  - Die Connect-Timeout-Tests beider Module akzeptieren jeden schnellen Fehler.
    - Restclient: `isNotNull().isNotInstanceOf(TimeoutException)` + `elapsed < 2 s`
    - Webclient: `WebClientRequestException` + `elapsed < 2 s`
  - Ein sofortiges „connection refused“ macht beide grün, ohne den Connect-Timeout zu prüfen. Das passiert etwa, wenn
    `localhost` auf `::1` auflöst, das Blackhole aber auf `getLoopbackAddress()` lauscht.
  - Ein Connect-Timeout von 1 s statt 100 ms erfüllt die 2-s-Schranke ebenfalls.
  - Der Webclient-Fall überspringt sich per `assumeTrue(backend.saturated)` still auf Plattformen, die den Connect
    ablehnen statt verwerfen.
    - Triggering condition: Plattform- oder Resolver-Abhängigkeit, oder eine Regression im Connect-Timeout
    - Impact: Der Vorbericht-Fix 4 („connect-timeout auf jeder Engine belegt“) belegt weniger, als er sagt.
  - Fix strategy: Die engine-spezifische Connect-Timeout-Ursache in der Cause-Chain verlangen, plus
    `elapsed ≥ connect-timeout`. Ein übersprungener Fall soll in CI auffallen (Zählung oder Fehler bei `CI=true`).

- [x] 6. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt:315` — bis `:321`; Testlücke in `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/`] {Medium} {Confidence: high} {test quality / coverage gap}
  - Ein Transportfehler **mitten im Body** (Peer schließt oder setzt die Verbindung zurück, Response-Timeout zwischen
    zwei Chunks) ist auf keiner Engine des Restclients getestet.
    - `grep -rn "Fault\.\|withFault\|CONNECTION_RESET" veredium-restclient/src/test` liefert 0 Treffer.
    - JaCoCo zeigt, dass der `ExecutionException`-Zweig des `HandOffInputStream` nie gelaufen ist.
  - Gerade dieser Zweig entscheidet die Fehlerform. Er packt die `IOException` des virtuellen Readers aus, die
    Spring dann zur `ResourceAccessException` macht.
  - Mutation: `throw e` statt `throw cause`. Dann erreicht eine geprüfte `ExecutionException` an Springs
    IOException-Behandlung vorbei den Breaker, und das ändert Verdict und legatium-Klassifikation. Die Mutation
    bleibt grün.
    - Triggering condition: Peer bricht mitten im Body ab, auf der Engine `reactor-netty`
    - Impact: Falsche Fehlerform bzw. falsches Breaker-Verdict auf einem Score-5-Pfad, unbemerkt
  - Fix strategy: Je Engine einen Raw-Socket-Backend-Fall (wie `DrippingBackend`), der nach dem Head einen Teil des
    Bodys schreibt und die Verbindung schließt. Erwartet wird `ResourceAccessException` mit `IOException`-Ursache und
    ein vom Breaker gezählter Fehler.

### 🟢 Low

- [x] 7. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt:1131`] {Low} {Confidence: low} {test quality / flakiness}
  - `shutdown.get(1, SECONDS)` umfasst den Start eines Virtual Threads und das Schließen des Apache-Pools. Der
    Engine-Zwilling erlaubt dafür 2 s (`PROMPTLY`, `RestClientEngineIntegrationTest.kt:733`).
  - Fenster dieser Art musste Commit `9823825` bereits für CI aufweiten. Ein Ausfall wurde nicht beobachtet.
  - Fix strategy: Auf `PROMPTLY` angleichen. Eine Mutation, die den Drain hot spinnen lässt, fällt auch dann noch
    auf (Fenster ≥ 2,4 s, Backend antwortet nach 5 s).

- [x] 8. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:824` — bis `:825`, gegen `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:606`, `:619-625`] {Low} {Confidence: medium} {test quality}
  - `close(IMMEDIATE)` wartet nicht auf die Worker. Eine `ClosedSelectorException` kommt daher asynchron und
    womöglich erst nach der ERROR-Prüfung.
  - JaCoCo zeigt, dass weder der DEBUG- noch der ERROR-Zweig von `onReactorException` im Lauf ausgeführt wurde. Die
    Herabstufung aus `ca7955a` ist also nur auf einem langsamen Runner belegt.
  - Die Prüfung des Apache-Loggers (`:824`) ist zudem leer geworden: Seit dem eigenen Callback loggt dort nichts mehr.
  - Fix strategy: `onReactorException` direkt unit-testen (ERROR vor dem Release, DEBUG währenddessen). Im
    Integrationsfall begrenzt auf das Ende der I/O-Threads warten, bevor die ERROR-Abwesenheit geprüft wird.

- [x] 9. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:171` — mit `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:299-301`, `:310-311`, `:581-582`] {Low} {Confidence: high} {test quality / coverage gap}
  - Reactor Netty bekommt `left`, also den Rest der gemeinsamen Frist. Der Virtual-Time-Test ruft `drain()` aber mit
    dem Default `poolDisposeTimeout` auf
    (`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientIntegrationTest.kt:1127-1168`).
    Wird `left` durch `poolDisposeTimeout` ersetzt, bleibt der Test grün.
  - Die Interrupt-Zweige des JDK- und des Apache-Releases laufen nie; nur Jettys ist getestet (`:875`).
  - Fix strategy: `ReactorNettyTransport.release` mit abgelaufener Frist und einem aufzeichnenden `dispose` aufrufen.
    Den Interrupt-Fall über alle Engines parametrisieren.

- [x] 10. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/AbortableExchangeTest.kt:67` — bis `:93`, gegen `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:696-699`] {Low} {Confidence: medium} {test quality / concurrency}
  - Vertauscht man in `attach` die Reihenfolge von `future.set(started)` und `if (aborted)`, bleiben die beiden
    geordneten Fälle grün.
  - Nur das 1000-Runden-Barrier-Rennen könnte die Vertauschung fangen, und sein Fenster ist wenige Nanosekunden
    breit. Die Implementierung selbst ist korrekt (volatile + `AtomicReference`, Dekker-Muster).
  - Fix strategy: Eine deterministische Verschränkung, etwa ein `Future`-Stub, der beim `set` selbst `abort()` aufruft.

- [x] 11. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:374`] {Low} {Confidence: high} {test quality}
  - „Ein Engine für alle Backends“ wird über `engineTransports.keys` geprüft, also über die Buchführung der Factory.
  - Mutation: `JdkEngineTransport.requestFactory` baut pro Backend einen eigenen `HttpClient`. Der Test bleibt grün,
    und nur ein Client wird geschlossen.
  - Das ist der Restclient-Zwilling von Vorbericht-Befund 23, der nur reaktiv behoben wurde.
  - Fix strategy: Prüfen, dass die Request-Factories zweier Backends dieselbe Client-Instanz tragen, oder die
    Engine-Threads zählen.

- [x] 12. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientEngineIntegrationTest.kt:597`] {Low} {Confidence: high} {test quality}
  - Im `max-connections`-Fall prüft die Assertion für den zweiten Aufruf nur `failure isNotNull`. Jeder Fehler vor
    dem Senden erfüllt das; `verify(0)` begrenzt den Schaden.
  - Fix strategy: Die engine-spezifische Pool- bzw. Queue-Timeout-Ursache verlangen.

- [x] 13. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/` — 12 Aufrufe von `addMockServiceRequestListener` (`grep -rn addMockServiceRequestListener veredium-restclient/src/test`)] {Low} {Confidence: high} {test quality / isolation}
  - `wireMock.resetAll()` entfernt Request-Listener nicht, sodass sie sich pro Klassenlauf anhäufen.
  - Ein später Request auf eine wiederverwendete URL (`/never`, `/slow`) eines früheren Tests kann den Latch eines
    späteren Tests auslösen.
  - Fix strategy: Eindeutige URLs pro Test verwenden oder den Listener in `@AfterEach` entfernen.

- [x] 14. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt:1142` — bis `:1165`] {Low} {Confidence: high} {test quality}
  - Der Test verspricht einen geschlossenen Pool, prüft ihn aber nicht. Ein leeres `shutdown()` bliebe hier grün;
    nur der Engine-Test (`RestClientEngineIntegrationTest.kt:381`) fängt es.
  - Fix strategy: `engineTransports[APACHE].isClosed` assertieren.

- [x] 15. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ReactorNettyEngineTransport.kt:182` — bis `:193`, und `:226-239`] {Low} {Confidence: high} {test quality / coverage gap}
  - Der Ablaufzweig von `awaitDisposal` (WARN „not disposed within“) hat keinen Test
    (`grep -rn "not disposed within" veredium-restclient/src/test` liefert 0 Treffer).
  - Die Gauge-Lambdas werden nie gelesen (JaCoCo: `:226`, `:234-239` nicht ausgeführt). Getestet ist nur, dass die
    Gauges registriert werden, nicht ihr Wert. Eine Verwechslung wie `ACTIVE ↔ acquiredSize` bliebe grün.
  - Fix strategy: Eine Disposal injizieren, die nie endet. Eine Gauge während eines gehaltenen Exchanges lesen.

- [x] 16. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt:122`] {Low} {Confidence: high} {test quality / flakiness}
  - Ein Rest von Vorbericht-Befund 20: Dieser WireMock antwortet weiter synchron.
  - Die 30-s-Delays bei `:828` und `:1336` halten nach dem Client-Abbruch je einen Container-Thread 30 s lang belegt.
  - Fix strategy: `asynchronousResponseEnabled(true)` wie im Engine-Test.

- [x] 17. [`./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientEngineIntegrationTest.kt:343` — sowie `:472`, `:813`, `:977`] {Low} {Confidence: medium} {test quality / flakiness}
  - Die Zeitfenster laufen auf der Wanduhr; die engste obere Toleranz ist +400 ms.
  - Der Warm-up hat das Klassenladen herausgenommen. GC- oder CPU-Hunger auf einem belasteten Runner und das 50-ms-
    Select-Intervall von Apache bleiben aber im Fenster.
  - Fix strategy: Ab dem Eingang des Requests am Backend messen (Zeitstempel im Backend) oder die Obergrenze an die
    nächste Sprosse der Leiter legen.

- [x] 18. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReader.kt:87` — mit `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/EngineTransport.kt:286-289`] {Low} {Confidence: high} {resources}
  - `readTruncated` hört bei genau `limitBytes` auf, ohne einen Probe-Read nach `-1`.
  - Ein Fehler-Body, der **genau** das Limit lang ist, gilt für `AbortingResponse` deshalb als „nicht zu Ende
    gelesen“. Auf Apache, JDK und Jetty wird die Verbindung verworfen, obwohl nichts mehr kommt.
  - Das ist nur eine verschenkte Keep-Alive-Verbindung im seltenen Grenzfall. Erfolgs-Bodies sind nicht betroffen:
    `readOrNull` liest Limit+1.
  - Fix strategy: In `readTruncated` nach erreichtem Limit einen Probe-Read machen, wenn der deklarierte
    `Content-Length` dem Limit entspricht, oder den Grenzfall dokumentiert hinnehmen.

- [ ] 19. [`./veredium-restclient/docs/timeouts-bulkheads-threads.md` — Klassifikationstabelle; aus dem Vorbericht (Nr. 16) übernommen, gegen legatium 1.2.1 nachgeprüft] {Low} {Confidence: high} {maintainability / logging}
  - Ein abgebrochener Exchange erscheint im Exchange-Strom weiter als ERROR `adapter_outcome=failure`.
  - Die neuen Abbruchtypen der Engines sind dort nicht klassifiziert: `InterruptedIOException` des Reactor-Hand-offs,
    `AsynchronousCloseException` aus `AbortingResponse` und die Cancellation von Apaches `cancel()`.
  - Fix strategy: weiter über Inqudium/legatium#88, mit den genannten Typen.

## 6. Systemic Patterns

1. **Zeitzusagen nur mit oberer Schranke.**
   - Zählbasis: `grep -rnE "assertThat\((elapsed|waited|quickElapsed|patientElapsed)\)" --include=*.kt */src/test`
     liefert **16** Wanduhr-Assertions. Davon haben nur **5** eine Untergrenze (`… isGreaterThan…`). Hinzu kommen
     14 `System.nanoTime()`-Messungen im `WebClientEngineIntegrationTest.kt`.
   - Ohne Untergrenze bleibt jede Mutation grün, die einer Sprosse einen kleineren Wert gibt: Anker statt
     Transport-Timeout, Pending-Acquire statt Connect-Timeout. Ohne Puffer werden die Fenster außerdem auf
     belasteten Runnern eng.
   - Repräsentativ: Befunde 4, 5, 7, 17.
2. **Reaktive Engine-Zusagen nur auf dem schnellen Pfad.** Auf dem Restclient hat der Vorbericht diese Lücke
   geschlossen; auf dem Webclient besteht sie weiter.
   - Bodies kommen dort am Stück an, und es gibt keinen Fehler mitten im Body, kein Tröpfeln, keinen Drain-Test
     außer für Jetty und keinen Wire-Test der Pool-Knöpfe.
   - Zählbasis: `grep -rn "withChunkedDribbleDelay\|DrippingBackend()"` liefert im Restclient 2 und im Webclient 0
     Treffer. „In-flight finishes“-Tests: Restclient 4 Engines, Webclient 1 (Jetty).
   - Repräsentativ: Befunde 1, 2, 3, 6, 9.
3. **Die Buchführung der Factory als Beweis**, ein Rest des gleichnamigen Vorbericht-Musters.
   - Zählbasis: `grep -rn "connectorOf\|requestFactoryOf\|engineTransports\|startedEngines" --include=*.kt */src/test`
     liefert 29 Treffer in 4 Dateien.
   - Die meisten dienen jetzt nur noch dem Zugriff auf das Objekt, das dann auf dem Draht oder am Pool geprüft wird.
     Als alleiniger Beweis bleibt Befund 11.

Statistik-Block: **3 systemische Muster**.

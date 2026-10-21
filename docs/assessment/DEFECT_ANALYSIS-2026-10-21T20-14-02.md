# Code & Defect Analysis: veredium (veredium-core, veredium-restclient, veredium-webclient)

1. Identification of the codebase
   - **Repository:** `https://github.com/Inqudium/veredium.git`
   - **Commit hash:** `0faa7dcc20b2a74c0ba09d19102f790ae47e82be` (full)
   - **Reference (branch/tag):** `refs/heads/main` / tag: keiner (kein Tag auf dem Commit; `git describe` liefert nur den Hash)
2. Scope of the analysis
   - **Included:** `./veredium-core/src/main/kotlin/`, `./veredium-restclient/src/main/kotlin/`, `./veredium-webclient/src/main/kotlin/` (Produktionscode, 27 Dateien, ≈4,4 k Zeilen); **Testcode ist Teil der Analyse**: `./veredium-*/src/test/kotlin/` und `./veredium-*/src/test/resources/` (31 Testklassen plus Helfer, ≈11,3 k Zeilen); ergänzend `./pom.xml`, `./veredium-*/pom.xml`, `./.github/workflows/fuzz.yml` und die Modul-Dokumentation unter `./veredium-*/docs/`, soweit sie Verhalten oder Testschutz zusichert
   - **Excluded:** generierte Artefakte (`./**/target/`), die MkDocs-Site (`./docs/` außer `./docs/adr/`), `.github/scripts/`, die anderen Workflows
3. Analysis environment & tools
   - **Target environment:** Java 21 (Bytecode-Ziel, `jvmTarget`), Build-JDK 24+ (lokal Oracle JDK 26.0.1, CI Temurin 25); Kotlin 2.4.21; Spring Boot 4.1.1; Resilience4j 2.4.0; Apache HttpClient 5 / Reactor Netty; legatium 1.2.0
   - **Build system:** Maven 3.9.15, Multi-Modul-Reaktor (`veredium-core`, `veredium-restclient`, `veredium-webclient`)
   - **Analysis tools used:** manuelles Review (Claude Code, Opus 5.5); `grep`; Abgleich gegen den Resilience4j-2.4.0-Quellcode (`CircuitBreakerConfig.Builder`, `PredicateCreator`) aus dem lokalen Maven-Repository; ein `mvn -q -o clean verify` (grün, 342 Tests) mit den JaCoCo-Berichten daraus; im Projekt vorhanden sind ktlint, CodeQL (CI) und JaCoCo, nicht vorhanden sind detekt, SpotBugs/Error Prone, ArchUnit und Sonar
4. Placement & Output
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/veredium` (absoluter Bezugspunkt; alle relativen Pfade beziehen sich darauf)
   - **Report output path:** `./docs/assessment/DEFECT_ANALYSIS-2026-10-21T20-14-02.md` (relativ zum workdir; Präfix + ISO-8601-Zeitstempel; `assessment/` ist in `./mkdocs.yml` per `exclude_docs` von der Site ausgenommen)
   - **Scope root (relative to the workdir):** `./` (drei Modulwurzeln `./veredium-core/`, `./veredium-restclient/`, `./veredium-webclient/`)
   - **Path convention for finding locations:** `<path relative to the workdir>:<line>` (z. B. `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceCustomizers.kt:97`)

---

## 1. Executive Summary

veredium ist in einem guten, bewusst gebauten Zustand. Die blockierende Resilience-Pipeline
(`BlockingResilienceDecorator`) behandelt Interrupt, Cancel, MDC/Observation-Übergabe und Shutdown sorgfältig.
Die drei Low-Befunde, die der Ursprungsbericht in tool-box zu diesem Code hatte (Int-Überlauf im Bounded Reader,
heiß drehende Drain-Schleife, `RejectedExecutionException` im Breaker), sind im Port behoben; das ist gegen den
aktuellen Code geprüft. **Es gibt keinen Critical-Befund.**

Die beiden **High**-Befunde liegen im Testschutz, nicht im Produktionsverhalten:

- Der Sweep über die Timeout-Leiter läuft nur auf Default-Margins, die alle `200` sind. Vertauschte Margin-Felder
  in der Leiter (Score-5-Logik) bleiben damit unentdeckt.
- Im reaktiven Stack pinnt kein Test die Reihenfolge CircuitBreaker ⟶ TimeLimiter ⟶ Bulkhead. Ein vertauschter
  Operator würde Timeouts am Breaker vorbeileiten, und kein Test schlüge an.

Im Produktionscode sind drei Medium-Befunde am gewichtigsten:

- Die Customizer-Komposition des Record-Prädikats hält ihre dokumentierte Zusage nicht ein. Resilience4j ODER-t
  die `record-exceptions`-Klassen beim `build()` wieder dazu. Ein Operator, der z. B. `WebClientResponseException`
  einträgt, bekommt dann einen fehlschlagenden Start mit irreführender Diagnose.
- Die in Doku und Referenz-YAML zitierten Contract-Tests (`ResilienceConfigContractTest`, `MeshAliasContractTest`)
  existieren im Port nicht. Die Invariante `sum(max-concurrent-calls) <= max-connections` ist damit unbewacht.
  Pool-Erschöpfung wird dann dem Breaker eines gesunden Backends angelastet.
- Ein leerer 2xx-Body verhält sich auf den beiden Stacks verschieden: blockierend eine Jackson-Exception mit WARN,
  reaktiv ein stilles leeres `Mono`.

Daneben gibt es eine Gruppe von Determinismus- und Beweiskraft-Schulden in den Integrationstests (Default-Budget
von 1 s, Timeout-Assertions, die den falschen Grund akzeptieren, ein Race im Cancel-Test). Fuzzing ist laut
Konvention vorgeschrieben, fehlt aber.

**Test-Urteil (kompakt):**

- **Verlässlichkeit:** Für die blockierende Pipeline und die Breaker-Verdicts ist die Suite ein echtes Netz.
  Konkurrenz entsteht deterministisch über Latches, es gibt weder `Thread.sleep` noch Mocks, und die Uhr ist
  injiziert. Falsche Sicherheit gibt es punktuell: bei der Leiter-Ableitung, beim reaktiven Operator-Order und bei
  zwei Timeout-Tests, die ohne TimeLimiter grün blieben. Die riskante Logik ist strukturell deterministisch und
  isoliert testbar; wo Tests infrastrukturgebunden sind, ist das eine Wahl des Tests.
- **Pyramide:** Der Core hat eine schnelle, gesunde Unit-Basis (153 Tests, ≈2 s). Die Client-Module sind
  integrationslastig (99 bzw. 90 Tests auf WireMock), was für Transport-Wiring angemessen ist. `ClientCallLog`
  hat dagegen keinen Unit-Test und zeigt im Core 0 % (Coverage wird pro Modul gerechnet). Die Zeilen-Coverage ist
  hoch (78 / 98 / 94 %), die Branch-Coverage liegt bei 70–84 %. Die Lücken häufen sich genau an den
  Score-4/5-Kanten.
- **Gravierendste Lücken:**
  - Leiter-Sweep blind für Margin-Vertauschung; Overrides und Init-Guards ungetestet (Befunde 1, 22)
  - Reaktiver Operator-Order ungepinnt (Befund 2)
  - Komposition der Record-Liste falsch und ungetestet (Befund 3)
  - Timeout-Tests bestehen ohne TimeLimiter (Befund 7)
  - Flake-Fläche durch das 1-s-Default-Budget (Befund 8)
  - Drain-Wartezeit und Mehrfach-Reads des Bounded Readers ungetestet (Befunde 9, 10)
  - Keine Fuzz-Targets (Befund 20)

## 2. Scope & Methodology

- **Analysiert:** alle drei Module, Produktions- **und** Testcode (siehe Metadaten). **Testcode war im Scope.**
  Er wurde als eigener Analysegegenstand behandelt (Mutations-Gedankenexperiment pro risikorelevantem Test) und
  zusätzlich als Risikoindikator für den Produktionscode.
- **Erkannter Stack:**
  - Spring Boot 4.1.1; blockierend mit Spring MVC-`RestClient` über Apache HttpClient 5, reaktiv mit WebFlux-`WebClient` über Reactor Netty.
  - Virtual Threads im blockierenden Decorator (`Executors.newThreadPerTaskExecutor(Thread.ofVirtual())`); Coroutines nur auf Aufruferseite (`awaitSingle`, Test-Scope).
  - Keine Persistenz und kein Messaging.
  - Kotlin 2.4.21, Ziel Java 21; Resilience4j 2.4.0 über `resilience4j-spring-boot4`.
  - Es ist eine Bibliothek: Es gibt keine Controller und keinen `src/main/resources`, und die „shipped“ YAML ist eine Test-Ressource.
  - Einstiegspunkte: `@EventListener(ApplicationReadyEvent)` in `MeshLadderLogger`, `SmartInitializingSingleton` in `MeshResilienceStartupGuard`, `BeanRegistrar` in `MeshResilienceCustomizerRegistrar`, `@PreDestroy` in den Factories und im Decorator.
- **Statische Analyse:** ktlint (Stil), CodeQL (Sicherheit, CI). Es gibt keinen Defekt-Linter (detekt/SpotBugs), deshalb wurde der Produktionscode vollständig manuell gelesen.
- **Lokale Verifikation:** Ein `mvn -o clean verify` lief grün (Test-Teilanalyse). Behauptungen über Resilience4j-Interna sind am Quellcode 2.4.0 nachgelesen. **Reproduktionstests wurden nicht geschrieben**, weil dafür laut Prompt die Freigabe aussteht. Befund 3 ist mit einer Ausführungsskizze belegt.
- **Vorberichte:** Im workdir liegt noch keine Analyse. Der Ursprungsbericht
  `~/IdeaProjects/tool-box/rest-client/docs/assessment/DEFECT_ANALYSIS-2026-10-19T14-13-16.md` wurde Befund für
  Befund gegen den Port geprüft:
  - Seine Nummern 4, 5, 6 und 8 sind im Port behoben.
  - Nummer 2 ist durch ADR-0001 strukturell entfallen.
  - Nummer 7 lebt als Befund 5 fort, Nummer 9 als Befund 16, Nummer 3 in veränderter Form als Befunde 4 und 19, Nummer 10 ist behoben (`@Timeout` vorhanden).
  - Die Nummern 1 und 14 betreffen eine ausgelieferte `application.yml`, die veredium nicht hat.
- **Blinde Flecken:**
  - Das Verhalten von legatium 1.2.0 wurde nur über veredium-Doku und Tests beurteilt.
  - Unter echter Last wurde nichts gemessen.
  - Die Zählungen in Befund 23 stammen aus einem Skript der Test-Teilanalyse und wurden nicht einzeln nachgezählt.

## 3. Statistics

| Severity | Anzahl |
|---|---|
| 🔴 Critical | 0 |
| 🟠 High | 2 |
| 🟡 Medium | 9 |
| 🟢 Low | 14 |
| **Summe** | **25** |
| Systemische Muster | 4 |

Kategorien: test quality 15, robustness 4, correctness 2, maintainability 2, resources/performance 1, concurrency 1.

## Bearbeitungsstand (2026-10-21, nach der Analyse)

23 von 25 Befunden sind behoben (abgehakt). Stand: Arbeitsbaum auf `main` über Commit
`0faa7dcc20b2a74c0ba09d19102f790ae47e82be`, noch nicht committet. Ein `mvn -o clean verify` über alle drei Module
ist grün: 441 Tests (vorher 342), 0 Fehler, ktlint sauber. `generate-test-catalog.py` meldet für alle 441 Tests
einen vollständigen Rationale-Block. Die Änderungen sind in `./CHANGELOG.md` unter „Fixed“ zusammengefasst.

Offen:

- **16** — Teilweise erledigt durch das Upgrade auf legatium 1.2.1. Der Pool-Lease-Timeout ist jetzt WARN
  `timeout`; ein neuer Fall in `LegatiumExchangeLoggingIntegrationTest` pinnt das (gegengeprüft: mit 1.2.0
  rot). Offen bleibt der abgebrochene Exchange (TimeLimiter-Ablauf, unterbrochener Aufrufer: `Closed by
  interrupt`): er erscheint weiter als ERROR `failure`. Das kann nur legatium ändern; erfasst als
  [Inqudium/legatium#88](https://github.com/Inqudium/legatium/issues/88).
- **24** — Nur teilweise erledigt. `ClientCallLog` hat einen eigenen Unit-Test (`ClientCallLogTest`) und
  erscheint im Core nicht mehr mit 0 %. Eine aggregierte JaCoCo-Coverage (`report-aggregate`) gibt es nicht,
  denn sie bräuchte ein eigenes Aggregator-Modul. Die CI baut ohnehin aus einem frischen Checkout, verwaiste
  Klassen betreffen also nur lokale Builds ohne `clean`.

Bei der Behebung zusätzlich gefunden:

- Der Shutdown der WebClient-Factory wartet nicht auf laufende Aufrufe; sie laufen auf ihrer Verbindung zu Ende.
  KDoc korrigiert und das Verhalten per Test gepinnt; ein Graceful Shutdown wäre eine Designänderung.
- Die Wartezeit des reaktiven Bulkheads (`max-wait-duration > 0`) läuft außerhalb des TimeLimiter-Budgets.
  Dokumentiert und gepinnt; ausgeliefert ist der Wert 0.
- Die Buffer-Limit-Zeile trug weder `adapter_name` noch `adapter_url_template`; behoben.

## 4. File-Ranking (Phase 1)

**Produktionscode**

| Datei | Score | Begründung |
|---|---|---|
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecorator.kt` | 5 | Thread-Hand-off, Interrupt/Cancel, async Waiter, MDC/Observation über Threads, Shutdown |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt` | 5 | Hot Path; Exchange, Bounded Read, Fehlerabbildung, Cancel-Through-Future, async Fortsetzung |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceCustomizers.kt` | 5 | Prädikat-Komposition gegen R4j-Builder-Interna; bestimmt jede Breaker-Entscheidung |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/ResolvedTimeouts.kt` | 5 | Ableitung aller Timeouts; Fehler wirken in jedem Aufruf |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt` | 4 | Pool-Lebenszyklus, Evictor, Drain im Shutdown, `computeIfAbsent` |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClient.kt` | 4 | Reaktive Kette, `defer`/`contextCapture`, Signal-Hooks |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ReactiveResilienceDecorator.kt` | 4 | Operator-Reihenfolge = Semantik |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/BreakerVerdicts.kt` | 4 | Cause-Walk, Benign-4xx-Regel |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceStartupGuard.kt` | 4 | Fail-fast-Logik, Probes |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReader.kt` | 4 | Peer-kontrollierte Eingabe, Grenzarithmetik |
| `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt` | 3 | Pool, Shutdown mit `block()`, JVM-weiter Hook, URI-Tagging |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/MeshProperties.kt` | 3 | Bind-Validierung, Triplet-Parsing, lazy Cache |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/client/ClientCallLog.kt` | 3 | Level-Politik, Validierung, Builder-Logging |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/client/ResponseBodyMetrics.kt` | 2 | Reine Funktion, getestet gegen JDK-Encoder |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/validation/ResponseValidation.kt` | 2 | Wenig Logik |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/observation/HttpObservationReport.kt` | 2 | Startup-Text |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/RestClientProperties.kt`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientProperties.kt` | 2 | Bind-Guards |
| `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/RestClientVerdicts.kt`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientVerdicts.kt` | 2 | Deklarativ |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/MeshTimeoutConfig.kt`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/RestClientConfiguration.kt`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientConfiguration.kt` | 1 | Bean-Verdrahtung |
| `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/BaseAdapter.kt`, `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/Veredium.kt`, `./veredium-*/src/main/kotlin/**/*ObservationReport.kt` | 1 | Interface, Konstanten, Einzeiler |

**Testcode** (Gewicht als Sicherheitsnachweis)

| Datei | Score | Begründung |
|---|---|---|
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecoratorTest.kt` | 5 | Echte Konkurrenz über Latches, Interrupt- und Cancel-Pfade, `isSameAs`; Schwächen in Befunden 11, 17 |
| `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/RestClientVerdictsTest.kt` | 5 | Alle Self-inflicted-Fälle durch den echten Decorator, mit Gegenproben |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceStartupGuardTest.kt` | 5 | Jede Problemklasse isoliert, exakte Zählung |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceContextTest.kt`, `ClientSideFailurePolicyTest.kt`, `HttpExchangeErrorPolicyTest.kt`, `MeshResilienceCustomizersTest.kt` | 4 | Echte R4j-Auto-Konfiguration; die Record-Liste fehlt (Befund 3) |
| `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientVerdictsTest.kt` | 4 | Verdicts gut; Operator-Order ungepinnt (Befund 2) |
| `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt`, `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientIntegrationTest.kt` | 4 | Breite Real-HTTP-Abdeckung; Befunde 7, 8, 9, 25 |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/mesh/MeshPropertiesDerivationTest.kt` | 3 (gewichtet 5) | Spiegel-Orakel auf gleichen Margins (Befund 1) |
| `./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReaderTest.kt` | 3 | Nur Single-Read (Befund 10) |
| `./veredium-*/src/test/kotlin/**/LegatiumExchangeLoggingIntegrationTest.kt` | 3 | Contract zur Schwesterbibliothek; Appender unscoped (Befund 18) |
| `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/WebClientMetricsTest.kt`, `*FactoryBuilderWiringTest.kt`, `*PropertiesTest.kt`, `*MeshResilienceShippedConfigTest.kt` | 3 | Solide, schmaler Fokus; „shipped“ ist eine Test-Ressource (Befund 19) |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/{observation,validation,client}/*Test.kt` | 3 | Gute Unit-Tests |
| `./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/VerediumTest.kt`, `MeshApplicationYmlBindingTest.kt`, `MeshLadderLoggerTest.kt`, `*LoggingLevelContractTest.kt`, `*ObservationReportTest.kt` | 2 | Pinnen überwiegend Testdaten bzw. Einzeiler |

## 5. Findings

### 🔴 Critical

Keine.

### 🟠 High

- [x] 1. [`./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/mesh/MeshPropertiesDerivationTest.kt:155` — bis `:206`, gegen `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/MeshProperties.kt:180-184`] {High} {Confidence: high} {test quality / false confidence}
  - Der 40-Fälle-Sweep läuft ausschließlich auf den Default-Margins. Die sind alle `200` (global,
    response-timeout, p99, slow-call-backoff), und das Orakel rechnet dieselbe Formel im Test nach.
    - Die Mutation `responseTimeout = sidecarGlobalMs + margins.p99MarginMs` statt `responseTimeoutMarginMs`
      (`./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/ResolvedTimeouts.kt:65`) bleibt grün.
    - Ebenso `p99MarginMs` statt `slowCallBackoffMs` in `:79`.
    - Nur `MeshLadderLoggerTest` setzt eine Margin abweichend (`globalMarginMs = 1000`). `Margins(` kommt in
      den Tests genau einmal vor.
    - Triggering condition: jeder Host, der eine Margin tunt
    - Impact: falsche responseTimeout- oder slowCall-Werte in Produktion. Der Startup-Guard vergleicht gegen
      dieselbe Ableitung und kann das nicht fangen.
  - Fix strategy: Sweep-Fälle mit paarweise verschiedenen Margins, nicht-default `healthyRetries` und
    Cap-Fraction ergänzen und gegen handgerechnete Literale prüfen statt gegen die gespiegelte Formel.

- [x] 2. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ReactiveResilienceDecorator.kt:100` — bis `:103`] {High} {Confidence: high (Lücke) / medium (Wirkung)} {test quality / false confidence}
  - Kein Test des Webclient-Moduls prüft, dass ein TimeLimiter-Timeout vom Breaker gezählt wird oder dass ein
    offener Breaker keinen Bulkhead-Permit nimmt. `numberOfFailedCalls` kommt nur bei 5xx- und
    Verdict-Fällen vor, nie bei einem Timeout.
    - Werden die TimeLimiter- und CircuitBreaker-Operatoren vertauscht, erreicht der Timeout den Breaker
      nicht mehr, und alle Tests bleiben grün: Der Circuit-open-Test arbeitet nur mit 500ern, der
      legatium-Test prüft nur `outcome=cancelled`.
    - Der blockierende Zwilling ist gepinnt
      (`./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecoratorTest.kt:237-270`).
    - Triggering condition: hängendes Backend nach einer Refaktorierung der Operatorkette
    - Impact: Der Breaker öffnet bei Timeouts nie, und genau dafür ist er da.
  - Fix strategy: Unit-Test auf `ReactiveResilienceDecorator` mit einem nie emittierenden `Sinks.one()`: Der
    Timeout zählt als Failure, ein offener Breaker abonniert die Quelle nicht, der Bulkhead bleibt unberührt.

### 🟡 Medium

- [x] 3. [`./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceCustomizers.kt:97` — mit `:93`, `:70-75` (KDoc) und `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/MeshResilienceStartupGuard.kt:113`, `:156-158`] {Medium} {Confidence: high} {correctness / robustness}
  - Die KDoc verspricht: „the record predicate is AND-ed … a benign 4xx cannot be recorded by it.“ Das gilt
    nicht für eine `record-exceptions`-**Klassenliste**.
    - `builder.recordException(pred)` ersetzt nur das Prädikat-Feld.
    - R4j 2.4.0 baut beim `build()`
      `createExceptionsPredicate(recordExceptionPredicate, recordExceptions)` = `classes(recordExceptions) OR
      predicate` (`CircuitBreakerConfig.java:1046-1048`, `PredicateCreator.java:13-18`). Die Klassen landen
      also ungefiltert wieder im Ergebnis.
    - Ausführungsskizze: Ein Operator schreibt
      `record-exceptions: [org.springframework.web.reactive.function.client.WebClientResponseException]`, um
      5xx explizit zu zählen. Das finale Prädikat ist dann `isInstance(WCRE) OR (!benign && isInstance(WCRE))`,
      und damit zählt auch der 404.
    - Der Guard bemerkt das (`recordExceptionPredicate.test(probe)`) und bricht den Start ab, aber mit der
      Diagnose „the mesh customizer did not reach it … registry was not built by Boot's auto-configuration“.
      Die Diagnose ist falsch, denn der Customizer hat gegriffen.
    - Dazu: Die Record-Komposition ist ungetestet (`grep recordExceptions */src/test` ergibt 0 Treffer;
      JaCoCo zeigt einen verpassten Branch in Zeile 98). Fällt `&& configured…` weg, wird kein Test rot.
    - Triggering condition: `record-exceptions` mit einer Oberklasse der 4xx-Exception des Stacks
      (`RestClientResponseException`, `HttpClientErrorException`, `WebClientResponseException`)
    - Impact: Startfehler mit irreführender Ursache. Ohne den Guard (handgebaute Registry) würde der Breaker
      still auf Business-404 öffnen.
  - Fix strategy: Im Customizer die Klassenliste des Builders leeren (`recordExceptions()` ohne Argumente) und
    ihre Wirkung ausschließlich über das gelesene `configured`-Prädikat einfließen lassen. Zusätzlich die
    Guard-Meldung für „Customizer erreicht, aber überstimmt“ unterscheiden und je einen Test für Klassenliste
    und Prädikat ergänzen.

- [x] 4. [`./veredium-restclient/docs/timeouts-bulkheads-threads.md:228` — mit `./veredium-restclient/docs/restclient-vs-webclient.md:225`, `./veredium-restclient/docs/decisions.md:17`, `./veredium-restclient/src/test/resources/application.yml:218`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt:176-180`] {Medium} {Confidence: high} {test quality / robustness}
  - Doku und Referenz-YAML nennen `ResilienceConfigContractTest` („enforces `sum(max-concurrent-calls) <=
    max-connections`“) und `MeshAliasContractTest` als bestehende Schutznetze. Beide Klassen existieren im
    Port nicht; sie sind beim Port aus tool-box verloren gegangen.
    - Zur Laufzeit prüft niemand die Invariante, auch der Startup-Guard nicht.
    - Ohne sie verlagert sich die Warteschlange in den gemeinsamen Pool (`maxConnPerRoute = maxConnTotal`,
      Reactor Netty analog pro Zieladresse).
    - Ein Pool-Lease-Timeout ist laut eigener Tabelle (`./veredium-restclient/docs/timeouts-bulkheads-threads.md:401`)
      ein **Failure**. Er trifft den Breaker des Backends, dessen Aufruf gerade wartet, und das muss nicht
      das Backend sein, das die Verbindungen hält.
    - Triggering condition: ein Host erhöht einen Bulkhead oder fügt ein Endpoint hinzu, ohne den Pool
      anzupassen; danach Last auf einem langsamen Backend
    - Impact: Breaker gesunder Backends öffnen (Fehlerfortpflanzung über den gemeinsamen Pool); die Doku
      verspricht einen Schutz, den es nicht gibt.
  - Fix strategy: Die Invariante dort prüfen, wo beide Werte bekannt sind (`MeshResilienceStartupGuard` kennt
    `bulkheadProperties` und `MeshProperties`), mindestens als WARN, und die Doku-Verweise auf die tatsächlich
    vorhandenen Tests ziehen.

- [x] 5. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClient.kt:199` — mit `:207`; `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt:328`, `:377`] {Medium} {Confidence: high (Mechanik) / medium (Häufigkeit)} {robustness}
  - Ein 2xx ohne Body (204 oder leerer 200) verhält sich auf den beiden Stacks verschieden.
    - Blockierend: `readValue("")` wirft eine Jackson-Exception, die mit Stacktrace als WARN geloggt wird
      (Ursprungsbefund 7, weiter offen).
    - Reaktiv: `bodyToMono(String)` ist leer, `map` läuft nicht, das `Mono<T>` endet leer. Es gibt keine
      Fehlerzeile, der Breaker zählt einen Erfolg, und der Aufrufer bekommt aus `awaitSingle()` eine
      `NoSuchElementException` ohne Bezug zum Backend.
    - Ungetestet auf beiden Seiten (kein `204`/`noContent()`-Stub).
    - Triggering condition: nur Edge Case; ein Backend antwortet legitim ohne Body
    - Impact: falsche bzw. fehlende Diagnose; die beiden Stacks widersprechen ihrer „one contract“-Zusage
  - Fix strategy: Einen Vertrag festlegen (z. B. benannter Fehler „empty body for <type>“ auf beiden Stacks
    oder ein dokumentierter optionaler Rückgabewert) und je Stack einen 204- und einen leeren-200-Test ergänzen.

- [x] 6. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClient.kt:221` — mit `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:304`, `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/client/ClientCallLog.kt:13`] {Medium} {Confidence: medium} {robustness / logging context}
  - `ClientCallLog` verspricht Zeilen „under the caller's correlation keys“. Auf dem reaktiven Stack laufen
    `doOnCancel`, `doOnError` und `doFinally` aber auf Netty-Threads.
    - `contextCapture()` legt die ThreadLocals nur in den Reactor-Context.
    - Zurück in die MDC kommen sie nur mit `Hooks.enableAutomaticContextPropagation()` **und** einem vom Host
      registrierten MDC-`ThreadLocalAccessor`.
    - Den Hook schaltet veredium nur bei `http-observation-enabled=true` ein, Default ist `false`.
    - Im Default fehlen den Fehler- und Cancel-Zeilen damit die Korrelationsschlüssel. Die blockierende Seite
      kopiert die MDC explizit.
    - Triggering condition: immer bei abgeschalteter HTTP-Observation (Default) bzw. ohne MDC-Accessor des Hosts
    - Impact: WARN-Zeilen ohne Trace- und Correlation-ID, also schlechtere Störungsanalyse. Datenverlust
      entsteht nicht.
  - Fix strategy: Die Voraussetzung (Hook plus Accessor) in KDoc und Guide als Bedingung der Zusage nennen und
    den Zustand in der Startup-Zeile berichten; alternativ die Zeilen in `ClientCallLog` mit einem
    explizit aus dem Reactor-Context gelesenen MDC-Snapshot schreiben.

- [x] 7. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt:584` — ebenso `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientIntegrationTest.kt:565-567`] {Medium} {Confidence: high} {test quality / false confidence}
  - `.isInstanceOfAny(TimeoutException, ResourceAccessException/WebClientRequestException)` bei einem Budget
    von 300 ms und einem responseTimeout von 1400 ms. Der Transport-Zweig ist nur erreichbar, wenn der
    TimeLimiter fehlt. Ohne TimeLimiter bleibt der Test grün (Transport nach ≈1,4 s).
    - Triggering condition: Regression in der TimeLimiter-Verdrahtung
    - Impact: Der Test behauptet „releases the caller“, ohne es zu belegen.
  - Fix strategy: Genau `TimeoutException` erwarten und optional die Dauer klar unter responseTimeout sowie den
    Breaker-Zähler prüfen.

- [x] 8. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt:1243` — ebenso `:1344`; `./veredium-webclient/src/test/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientIntegrationTest.kt:968`, `:1040`] {Medium} {Confidence: medium} {test quality / flakiness}
  - Die Client-Helfer nutzen `TimeLimiterConfig.ofDefaults()` (1 s), obwohl dieselben Dateien an anderer
    Stelle davor warnen (Restclient `:1375-1376`, Webclient `:1060-1061`).
    - Betroffen sind ≈24 + 16 Tests mit echtem Loopback-HTTP, einschließlich des ersten Requests der JVM
      (JIT, Pool, Jackson).
    - Triggering condition: ausgelastete CI-Runner
    - Impact: rote Builds ohne Defekt; Log-Assertions (`single{…}`) scheitern an einer unerwarteten
      TimeoutException.
  - Fix strategy: Großzügiges Default-Budget (z. B. 30 s) im Helfer; kurze Budgets nur in den Timeout-Tests.

- [x] 9. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt:380` — mit `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:262-273`] {Medium} {Confidence: high} {test quality / coverage gap}
  - Die Drain-Schleife wird in keinem Test betreten (JaCoCo: `:381` nie ausgeführt). Die drei
    Shutdown-Tests decken „Deadline schon vorbei“, „schon interrupted“ und „nichts geleast“ ab; löscht man die
    Schleife, bleiben alle grün.
    - Der WARN-Zweig des Webclient-Zwillings ist ganz ungetestet, und es gibt dort keine injizierbare Uhr.
    - Triggering condition: Regression im Shutdown-Pfad
    - Impact: Ein Graceful Shutdown, der laufende Aufrufe abschneidet, bliebe unbemerkt.
  - Fix strategy: Eine Verbindung über einen kurzen WireMock-Delay leasen, `shutdown` mit kontrollierter Uhr
    aufrufen und prüfen, dass der laufende Aufruf erfolgreich endet und kein WARN erscheint; den
    Webclient-Timeout-Zweig testbar machen.

- [x] 10. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReaderTest.kt:19` — bis `:58`, gegen `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReader.kt:51-83`] {Medium} {Confidence: high} {test quality}
  - Alle Eingaben sind `ByteArrayInputStream`s unter `CHUNK_SIZE` (8 KiB), oder der erste Read liefert alles.
    Kein Test macht einen zweiten, akkumulierenden Read.
    - Kurze Reads (1 Byte, 0 Byte) kommen nicht vor.
    - Die Mutation „nur letzten Chunk behalten“ bleibt in den Unit-Tests grün.
    - Triggering condition: Regression an einer peer-kontrollierten Grenze (Score 4)
    - Impact: Die KDoc-Zusage „the boundary is pinned on both sides“ ist für mehrteilige Bodies unbelegt.
  - Fix strategy: Einen tröpfelnden Stream (n Byte pro Read) sowie Bodies über 8 KiB, passend und
    überschreitend, ergänzen; das ist zugleich das natürliche Jazzer-Target (Befund 20).

- [x] 11. [`./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecoratorTest.kt:559` — bis `:573`] {Medium} {Confidence: medium} {test quality / flakiness, concurrency}
  - `workerInterrupted.countDown()` läuft im Callable, also **bevor** das `finally` von
    `Bulkhead.decorateCallable` den Permit freigibt. Der Test ruft danach sofort `decorateAsync` auf einem
    Bulkhead mit `maxWaitDuration = 0` auf.
    - Triggering condition: Der alte Worker wird direkt nach dem `countDown` entplant; selten, unter Last
      möglich.
    - Impact: sporadische `BulkheadFullException`, also ein roter Test ohne Defekt.
  - Fix strategy: Vor dem zweiten Aufruf begrenzt auf `availableConcurrentCalls == 1` warten.

### 🟢 Low

- [x] 12. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt:260` — mit `:294-304` und `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecorator.kt:317-327`] {Low} {Confidence: medium} {robustness}
  - Der `CancellationException`-Zweig von `executeAsync` loggt nur und schließt `result` nicht ab. Das ist
    korrekt, solange die Cancellation vom Aufrufer kam, weil `result` dann schon cancelled ist. Endet die
    Pipeline aus anderem Grund mit einer `CancellationException`, hängt das Future des Aufrufers für immer;
    heute ist kein solcher Pfad bekannt.
  - Zweitens: `orTimeout`/`completeOnTimeout` auf dem ausgegebenen Future schließen es über
    `completeExceptionally` ab, nicht über `cancel`. Der Worker läuft dann bis zum TimeLimiter-Budget weiter
    und hält Permit und Verbindung, obwohl der Aufrufer das anders erwartet.
  - Fix strategy: Im Cancel-Zweig `result.cancel(...)` bzw. `completeExceptionally` idempotent nachziehen und
    das `orTimeout`-Verhalten in der KDoc nennen.

- [x] 13. [`./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecorator.kt:173` — ebenso `:155`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt:147`, `:178`] {Low} {Confidence: high} {maintainability / Kotlin interop}
  - `decorate`, `get` und `post` werfen checked Exceptions (`TimeoutException`, `InterruptedException`) ohne
    `@Throws`. Ein Java-Host, für den ADR-0002 den Decorator ausdrücklich als eigenständige Komponente anbietet,
    kann `catch (TimeoutException e)` nicht schreiben („exception is never thrown“) und muss `Exception`
    fangen.
  - Fix strategy: `@Throws(TimeoutException::class, InterruptedException::class)` an die öffentlichen
    blockierenden Methoden.

- [x] 14. [`./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/ResolvedTimeouts.kt:95` — mit `:58`; `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:240`] {Low} {Confidence: high} {robustness / config}
  - `globalMarginMs` und `responseTimeoutMarginMs` haben keine eigene Untergrenze. Ein negatives
    `global-margin-ms` passiert alle Guards und kann `sidecarGlobalMs` unter die Summe der Versuche drücken.
    Bei sehr negativen Werten ergibt sich ein `slowCallDuration <= 0`, das erst im R4j-Builder mit einer
    schlüssellosen Meldung scheitert.
  - `connectTimeoutMs.toInt()` läuft bei Werten über `Int.MAX_VALUE` still über.
  - Fix strategy: `require(globalMarginMs >= 0)` mit Schlüsselnamen sowie eine Obergrenze für die Transport-Millis.

- [x] 15. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/BoundedBodyReader.kt:54` — mit `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt:377`, `:328`] {Low} {Confidence: medium} {performance / resources}
  - Pro Antwort entstehen hintereinander Kopien: der wachsende `ByteArrayOutputStream` (bis 2× durch
    Verdopplung), dann `toByteArray()`, dann `String(...)` (UTF-16, bis 2× Bytes). Das ergibt grob 4–5× des
    Limits als Spitzenbedarf je gleichzeitiger Antwort; bei `max-in-memory-size` im MB-Bereich und 100
    Verbindungen ist das relevant.
  - Fix strategy: Puffer mit `Content-Length` vordimensionieren bzw. direkt aus dem Byte-Array deserialisieren
    (Jackson `readValue(ByteArray)`) und den String nur für die Größenmessung vermeiden.

- [ ] 16. [`./veredium-restclient/docs/timeouts-bulkheads-threads.md:401` — bis `:405`] {Low} {Confidence: high} {maintainability / logging}
  - Ein TimeLimiter-Ablauf, ein unterbrochener Aufrufer und ein Pool-Lease-Timeout erscheinen im
    Exchange-Strom als **ERROR** `adapter_outcome=failure`. Ursache: legatium 1.2.0 klassifiziert weder
    `Closed by interrupt` noch `ConnectionRequestTimeoutException`. Das ist dokumentiert, bedeutet aber
    Alarmrauschen unter Last (Ursprungsbefund 9, weiter offen).
  - Fix strategy: die Klassifikation im legatium-Projekt erweitern; bis dahin die Alarmregel auf Outcome plus
    Exception-Typ schärfen.

- [x] 17. [`./veredium-core/src/test/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecoratorTest.kt:99` — ebenso `:161-179`] {Low} {Confidence: high} {test quality}
  - Zwei Tests versprechen im Namen bzw. Kommentar, dass MDC und Scope nach dem Aufruf wiederhergestellt
    werden, prüfen das aber nicht. Weil jeder Worker ein frischer Virtual Thread ist, bleibt das Löschen von
    `BlockingResilienceDecorator.kt:291-292` grün.
  - Fix strategy: die Wiederherstellung auf einem wiederverwendeten Thread prüfen oder die Tests ehrlich umbenennen.

- [x] 18. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/LegatiumExchangeLoggingIntegrationTest.kt:846` — ebenso der Webclient-Zwilling um `:930`] {Low} {Confidence: medium} {test quality / isolation}
  - Der `RecordingAppender` zählt jedes Event auf dem prozessweiten Exchange-Logger, ohne Backend-Filter.
    Ein spätes Event eines abgebrochenen Workers aus einem früheren Test kann mitgezählt werden; den gleichen
    Mechanismus hat `ResilientWebClientIntegrationTest` bereits als CI-Fehler dokumentiert und dort behoben.
  - Fix strategy: auf `adapter_name` filtern oder die vorhandene `InFlightCallBarrier` nutzen.

- [x] 19. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ConfigurationFiles.kt:12` — mit `./veredium-restclient/src/test/resources/application.yml:6-7` und dem Webclient-Zwilling] {Low} {Confidence: high} {test quality / maintainability}
  - `[SHIPPED]` heißt laut KDoc `src/main/resources/application.yml`, „the file the artefact runs on“. Kein
    Modul hat `src/main/resources`; die Datei ist eine Test-Ressource, die kein Konsument erhält. Die
    „Shipped“-Tests pinnen also Referenzdokumentation.
  - Die Datei trägt noch `com: ing: DEBUG` aus dem Ursprungsprojekt; die KDoc von `LoggingLevelContractTest`
    spricht von `eu.inqudium.veredium`.
  - Fix strategy: in „reference config“ umbenennen, Logger-Präfix auf `eu.inqudium.veredium` ziehen und die
    KDocs angleichen.

- [x] 20. [`./.github/workflows/fuzz.yml:28` — bis `:33`, mit `./CONTRIBUTING.md:119-127`] {Low} {Confidence: high} {test quality / convention deviation}
  - CONTRIBUTING verlangt Jazzer-Targets für parsende und begrenzende Oberflächen. Es gibt kein `src/test/java`.
  - Der nächtliche Fuzz-Job läuft mit dem Platzhalter `VerediumFuzzTest` und dem veralteten Kommentar „the
    scaffold carries no parser of caller- or peer-controlled data“. Er ist damit grün, ohne etwas zu fuzzen.
  - Kandidaten sind `BoundedBodyReader`, `ResponseBodyMetrics.utf8Size` (Orakel: JDK-Encoder),
    `WebClientMetrics.uriTag` und das Triplet-Parsing.
  - Fix strategy: `BoundedBodyReaderFuzzTest.java` (Ergebnis ≤ Limit, `null` genau dann, wenn die Eingabe das
    Limit übersteigt, Bytes unverändert) und ein `utf8Size`-Target anlegen und die Matrix aktualisieren.

- [x] 21. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:297` — bis `:311`] {Low} {Confidence: high} {test quality / coverage gap}
  - `WebClientReactorContextPropagationConfig` ist zu 0 % abgedeckt. Alle Observation-Nesting-Tests laufen
    ohne `Hooks.enableAutomaticContextPropagation()`, also in einem anderen Modus als die Produktion bei
    eingeschalteter Observation.
  - Fix strategy: Test in einem eigenen JVM-Fork (der Hook ist JVM-weit) oder zumindest ein Unit-Test der Toggle-Entscheidung.

- [x] 22. [`./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt:187` — mit `:200-210`, `:281`; `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClient.kt:403`; `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/ResolvedTimeouts.kt:96-106`; `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/MeshProperties.kt:189-216`; `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecorator.kt:261`] {Low} {Confidence: high} {test quality / coverage gap}
  - Nie ausgeführt: per-Endpoint-`headers` und -`baseUrl`, ein nicht-null `maxLifeTime`/`evictInBackground`
    (Evictor), ein Nicht-UTF-8-`charsetOf`, die Init-Guards von `ResolvedTimeouts` (Meldungs-Lambdas),
    `MarginsOverride`/`overriddenBy` (0/8 Zeilen) und der Zweig „cancel vor Waiter-Zuweisung“.
  - Fix strategy: je einen gezielten Test pro Zweig; die Margin-Overrides gemeinsam mit Befund 1 erledigen.

- [x] 23. [`./veredium-*/src/test/kotlin/**/*Test.kt` — Konvention `./CONTRIBUTING.md:107-110`] {Low} {Confidence: medium} {test quality / convention deviation}
  - 114 von 286 Testmethoden fehlt der Rationale-Block `What is tested:`/`Success criteria:`/`Why it matters:`,
    aus dem die Test-Evidence-Seite generiert wird; 15 fehlt Given/When/Then. Die Überschrift variiert („How
    success is judged“, „How it is deemed successful“, „Success criteria“). Zählbasis: Skript über die
    Methodenrümpfe, siehe Abschnitt 2.
  - Fix strategy: Blöcke nachziehen und eine einheitliche Überschrift wählen, damit die Evidence-Seite vollständig ist.

- [ ] 24. [`./pom.xml:459` — JaCoCo-Konfiguration, mit `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/client/ClientCallLog.kt:58`] {Low} {Confidence: high} {test quality / coverage illusion}
  - JaCoCo misst pro Modul, und das Badge summiert die Modul-CSVs. `ClientCallLog` (86 Zeilen, 34 Branches)
    erscheint deshalb mit 0 %, obwohl beide Client-Suiten es ausführen; einen eigenen Unit-Test hat es nicht.
  - Ein Build ohne `clean` zählt außerdem verwaiste Klassen der ADR-0001/0002-Verschiebungen mit
    (`target/classes`).
  - Fix strategy: `report-aggregate` einsetzen, einen Unit-Test für `ClientCallLog` mit Stub-Verdicts
    schreiben und im Coverage-Workflow immer `clean` bauen.

- [x] 25. [`./veredium-restclient/src/test/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientIntegrationTest.kt:1176` — bis `:1213`] {Low} {Confidence: high} {test quality}
  - Der Titel sagt „the exchange is abandoned, not merely detached“. Geprüft werden aber nur der Future-Zustand
    und die Log-Zeile. Fällt `waiter.get()?.cancel(true)` weg (`BlockingResilienceDecorator.kt:322`), bleibt der
    Test grün; nur der Core-Test fängt das.
  - Fix strategy: die Freigabe über Bulkhead-Permit oder Pool-Lease prüfen.

## 6. Systemic Patterns

1. **Doku/Referenz-Konfiguration zitiert Schutznetze, die der Port nicht mitgenommen hat.** ≈10 Verweise auf
   `ResilienceConfigContractTest`/`MeshAliasContractTest` (Zählbasis: `grep -rn "ResilienceConfigContractTest\|MeshAliasContractTest"`
   über `*.md`/`*.yml` außerhalb `target/`), bei 0 existierenden Klassen. Dazu „SHIPPED“ für eine Test-Ressource
   und der Fuzz-Kommentar „no parser“. Repräsentativ: Befunde 4, 19, 20.
2. **Echte Zeit in Resilience-Tests.** 14× `withFixedDelay` in 4 Dateien, dazu ≈40 Real-HTTP-Tests auf dem
   1-s-Default-Budget (Zählbasis: `grep -rn withFixedDelay */src/test`; Skript über die `@Test`-Rümpfe der zwei
   Integrationstests). Für den blockierenden TimeLimiter ist das teils unvermeidbar; die Abstände zu den Budgets
   sind das Risiko. Repräsentativ: Befunde 8, 11.
3. **Spiegel- und Single-Point-Orakel an Grenzlogik.** Der Leiter-Sweep auf identischen Defaults, der Bounded
   Reader nur mit Single-Reads und Timeout-Assertions, die beide Ursachen akzeptieren (Zählbasis: 1 Sweep, 1
   Reader-Testklasse, 2 `isInstanceOfAny`-Timeout-Asserts, jeweils per grep). Repräsentativ: Befunde 1, 7, 10.
4. **Asymmetrie zwischen den Stacks an Randfällen.** Leerer Body, MDC auf Fehlerzeilen, Pool-Semantik (Apache:
   Gesamtgrenze über alle Routen; Reactor Netty: pro Zieladresse) und Operator-Order (blockierend gepinnt,
   reaktiv nicht); 4 Stellen, gezählt beim manuellen Abgleich der Zwillingsdateien. Repräsentativ: Befunde 2, 4, 5, 6.

Statistik-Block: **4 systemische Muster**.

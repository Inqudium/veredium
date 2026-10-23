# Architecture & Appropriateness Analysis: veredium (veredium-core, veredium-restclient, veredium-webclient)

1. Identification of the Codebase
   - **Repository:** `https://github.com/Inqudium/veredium.git`
   - **Commit-Hash:** `0cf22ee906a27c69911f8b3c7bcbc2573c305f7a` (Full)
   - **Reference (Branch/Tag):** `refs/heads/main` / Tag: keiner (unveröffentlicht, laut `./README.md` „(unreleased)“); Arbeitsbaum sauber
2. Scope of the Analysis
   - **Included:**
     - Produktionscode `./veredium-core/src/main/kotlin/`, `./veredium-restclient/src/main/kotlin/`, `./veredium-webclient/src/main/kotlin/` (40 Dateien, rund 7 900 Zeilen).
     - Zur Einordnung gelesen: `./veredium-examples/src/main/kotlin/` (wie ein Host die Bibliothek verdrahtet), `./veredium-test-support/src/main/kotlin/`, `./docs/adr/`, `./docs/engine-behaviour.md`, die Guides unter `./veredium-*/docs/`, `./CHANGELOG.md` und die Git-Historie seit `1b72625602728c0ed578289986fc99e512a7c073` (Einführung der Engine-Wahl).
   - **Excluded:** generierte Artefakte (`./**/target/`), `./veredium-coverage/` (enthält keinen Code), `./.github/` (nur als Kontext erwähnt), die MkDocs-Site.
   - **Test code:** Testcode ist **kein** eigenständiger Prüfgegenstand. Gelesen wird er in der Tiefe „Architektursignal“, also nach Form der Pyramide, Kontext-/Infrastrukturlast je Test, Setup-Aufwand und gespiegelten Suiten (`./veredium-*/src/test/kotlin/`, 52 Testklassen, rund 18 000 Zeilen). Die Interna einzelner Tests werden nicht auditiert.
3. Analysis Environment & Tools
   - **Target Environment:** Java 21 (Bytecode-Ziel), Build-JDK 24+ (lokal Oracle JDK 26.0.1), Kotlin 2.4.21, Spring Boot 4.1.1 / Spring Framework 7.0, Resilience4j 2.4.0, legatium 1.3.0. Engines: Apache HttpClient 5.6, Jetty 12.1, Reactor Netty 1.3 und `java.net.http`.
   - **Build system:** Maven 3.9.15, Multi-Modul-Reaktor
   - **Analysis tools used:** manuelles Review (Claude Code, Opus 5.5) des gesamten Produktionscodes; `grep`/`wc` für die Zählungen (Testform, Nutzungsstellen, Commit- und Befundanteile); die Vorberichte `./docs/assessment/DEFECT_ANALYSIS-2026-10-22T14-01-49.md` und `./docs/assessment/DEFECT_ANALYSIS-2026-10-23T05-20-01.md` als Kostendaten. Keine Architektur-Werkzeuge (ArchUnit, jQAssistant, Sonar) vorhanden.
4. Placement & Output
   - **Working directory (workdir):** `/home/dirk/IdeaProjects/veredium` (absoluter Bezugspunkt; alle relativen Pfade beziehen sich darauf)
   - **Report output path:** `./docs/assessment/ARCHITECTURE_REVIEW-2026-10-23T22-22-51.md` (relativ zum workdir; Präfix und ISO-8601-Zeitstempel). Es ist der erste `ARCHITECTURE_REVIEW` dieses Repositorys. Die vorhandenen `CODE_STYLE-*`- und `DEFECT_ANALYSIS-*`-Berichte bleiben unverändert.
   - **Scope root (relative to the workdir):** `./` (Modulwurzeln `./veredium-core/`, `./veredium-restclient/`, `./veredium-webclient/`)
   - **Path convention for findings:** `<path relative to the workdir>:<line>`. Abkürzungen im Fließtext: `core/…` steht für `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/…`, `restclient/…` für `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/…`, `webclient/…` für `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/…`. In den Befundköpfen stehen die Pfade ausgeschrieben.

---

## 1. Executive Summary

Veredium ist eine kleine Spring-Boot-Bibliothek mit rund 7 900 Zeilen Produktionscode und einem Maintainer. Sie gibt
einem Host je Mesh-Endpoint einen resilienten HTTP-Client, wahlweise blockierend oder reaktiv. Die Timeouts leiten
sich aus einem einzigen gemessenen Anker ab. Der **Kern ist angemessen gebaut**:

- Die Leiter (`MeshProperties`, `ResolvedTimeouts`), die Breaker-Verdicts, der Startup-Guard und der blockierende
  Decorator sind schmal, rein und ohne Spring-Kontext testbar.
- Das Gewicht dieser Teile ist tragend. Es löst echte Probleme (Timeout-Ordnung, Breaker-Fehlurteile, Abbruch
  blockierender Aufrufe) und ist in ADR-0001/0002 begründet.
- Die Kernabstraktionen haben je zwei echte Implementierungen (`HttpEngine`, `MeshConnectionPools`,
  `HttpObservationReport`, `MeshResilienceRegistrar`), eine pro Stack. Spekulative Single-Impl-Interfaces gibt es
  praktisch nicht.

Die Tendenz ist **punktuelles Over-Engineering in der Breite des Funktionsangebots, nicht in der Tiefe der
Abstraktion**:

- **Befund 1, die Engine-Wahl:** Vier wählbare HTTP-Engines je Stack ergeben acht Transport-Implementierungen. Sie
  binden rund ein Drittel des Produktionscodes und ein Viertel der Testzeilen. Auf sie entfielen 13 von 30 Commits
  seit ihrer Einführung und 34 von 43 Befunden der beiden letzten Defektanalysen. Eine Anforderung, die das trägt,
  ist nirgends dokumentiert.
- **Befund 2, die Verdrahtung:** Die Bibliothek wird per Component-Scan ihres eigenen Pakets eingebunden statt über
  Boots Auto-Configuration. Ein Host muss interne Paketnamen kennen und kann nichts gezielt ersetzen oder abschalten.
  Das verschärft auch das in ADR-0003 beschriebene Problem, dass beide Stacks nicht in einem Kontext laufen.

Unter-Engineering ist selten. Die Reste der früheren Zwillingsstruktur zwischen den Stacks (Fabriken, Engine-Schicht)
driften bereits auseinander (Befund 3, Muster P1). Beide großen Befunde sind **jetzt billig korrigierbar**, weil
noch kein Release existiert. Nach dem ersten Release werden sie öffentliche Konfiguration und damit teuer.

**Test-Urteil (Architektur-Perspektive)**

1. **Testbarkeit der Architektur: gut.** Die fachlich harten Teile liegen als reine Klassen ohne I/O vor:
   - die Leiter-Ableitung, die Verdicts mit Ursachenkette und der Guard (`verify()` direkt mit Registries
     konstruierbar);
   - die Validierung, das Call-Log und der Decorator (Registries oder Instanzen direkt übergeben).

   Die Transportschicht ist zwangsläufig nur gegen echte Sockets prüfbar. Das ist der Gegenstand der Bibliothek,
   kein fehlender Seam.
2. **Nutzung / Pyramide: gesund, mit schwerem Engine-Block.**
   - Es gibt **keinen einzigen `@SpringBootTest`**. Kontexte entstehen nur über `ApplicationContextRunner` in rund
     16 Verdrahtungstests.
   - Die Kernlogik wird durchweg isoliert geprüft (core: 14 Klassen, 129 Tests, kein HTTP).
   - Das obere Ende ist breit, aber begründet: Integrationstests gegen WireMock und rohe Sockets je Stack.
   - Ausnahme ist der Engine-Matrix-Block mit rund 4 200 Testzeilen. Er wächst multiplikativ mit Engines × Stacks
     × Szenarien.
3. **Größte Lücken / Auffälligkeiten:**
   - Die Testkosten skalieren mit der Engine-Matrix (Befund 1). Das schwerste Testgewicht liegt auf einem Feature
     ohne dokumentierten Bedarf.
   - Die Testsuiten sind über die Stacks gespiegelt (Muster P2, 9 Paare, rund 10 000 Zeilen). Jede
     Verhaltensänderung wird zweimal getestet. Das ist überwiegend durch die verschiedenen APIs gerechtfertigt.
   - Kein Test und kein Build-Gate hält fest, dass beide Stacks dieselben Startregeln anwenden. Die bereits
     eingetretene Divergenz bei der JDK-Host-Prüfung (Befund 3) blieb deshalb unbemerkt.
   - Ein Seam „nur für Mockbarkeit“ existiert nicht. Die Funktionsparameter (`nanoTime`, `engineLibraryPresent`,
     `jdkAllowsHostHeader`) sind schlanke, echte Testnähte.

---

## 2. Problem baseline & methodology

### Kerndomäne

Ausgehende HTTP-Aufrufe eines Spring-Boot-Hosts an Backends hinter einem Mesh-Sidecar, adressiert über
Endpoint-Triplets (`/endpoint/<host>/<METHOD>/<path>`). Aus einem gemessenen Wert je Endpoint
(`individual-request-timeout-ms`) leitet `ResolvedTimeouts` die gesamte Timeout-Leiter ab:

- das Sidecar-Global-Budget,
- den Response-Timeout des Transports,
- die Slow-Call-Schwelle,
- das TimeLimiter-Budget.

Resilience4j bekommt diese Werte samt Breaker-Verdicts über seine eigenen `*ConfigCustomizer`. Dazu kommen
begrenztes Body-Lesen, Bean Validation der Antworten, Metriken, die Observation-Diagnose und das Exchange-Logging
über legatium.

Die Domäne hat echte Invarianten: die Ordnung der Leiter, die Breaker-Urteile (benigne 4xx, selbstverursachte
Fehler) und die Abbruchsemantik blockierender Aufrufe. Sie ist kein CRUD. Die Komplexität des Kerns ist dem
angemessen.

### Anforderungen & Maßstab

- Bibliothek, unveröffentlicht, ein Maintainer (ADR-Deciders: „Dirk Haase (maintainer)“).
- Keine Last- oder SLA-Angaben im Repository. Der Maßstab ist „ein Service mit einer Handvoll Mesh-Backends“
  (Beispiel-YAMLs und Guides).
- Hoher Qualitätsanspruch an Betriebstransparenz: Startup-Zeilen erklären jede abgeleitete Entscheidung, und
  Fehlkonfigurationen scheitern beim Start (fail fast).

### Dokumentierte Architekturabsicht

- **ADR-0001 (Accepted):** Was zwischen den Stacks konstruktionsbedingt identisch ist, kommt in `veredium-core`.
  Jeder Stack behält nur Client, Decorator, Fabrik, Properties und Verdicts. Die Regel lautet ausdrücklich
  „identisch → Kern“.
- **ADR-0002 (Accepted):** Der blockierende Decorator ist eine Kernkomponente, auch ohne Client nutzbar, inklusive
  `decorateAsync`.
- **ADR-0003 (Proposed):** Jeder Mesh-Endpoint gehört genau einem Stack. Heute kollidieren beide Stacks in einem
  Kontext. Das ist eine bekannte, dokumentierte Grenze und wird hier nicht als eigener Befund geführt. Befund 2
  berührt sie.
- Die **Engine-Wahl** (Commit `1b72625`, PR #8) ist als Feature in Guide, CHANGELOG und `./docs/engine-behaviour.md`
  beschrieben: *was* die Engines tun und *wie* die Isolation gegenüber Boot gewahrt wird. Ein *Warum*, also welcher
  Host welche Engine braucht, fehlt. Es gibt dazu kein ADR.
- **Wiring-Modell** (Component-Scan des Stack-Pakets): Es wird in ADR-0001 und den Guides als gegeben vorausgesetzt
  („a host that scans its stack's package gets the shared wiring“). Eine Abwägung gegen Auto-Configuration ist
  nicht dokumentiert.

### Technologie-Kohärenz

Blockierend und reaktiv werden bewusst als zwei getrennte Module angeboten. Der Host wählt, das ist ein Produkt-
und kein Paradigmen-Mix. Der blockierende Stack nutzt virtuelle Threads statt eines Pools, was zu Java 21 passt.

Ein JVM-weiter Eingriff ist der Reactor-Hook in `webclient/WebClientConfiguration.kt:60`. Er ist begründet, an den
Observation-Schalter gekoppelt und in eine eigene `@Configuration` isoliert. Das ist angemessen und kein Befund.

### Testtopologie (Baseline-Signal)

| Modul | Testklassen | Tests | `@SpringBootTest` | `ApplicationContextRunner` | mit echtem HTTP | Testzeilen |
|---|---|---|---|---|---|---|
| veredium-core | 14 | 129 | 0 | 1 | 0 | 4 281 |
| veredium-restclient | 14 | 161 | 0 | 4 | 7 | 6 408 |
| veredium-webclient | 19 | 163 | 0 | 5 | 6 | 6 700 |
| veredium-examples | 4 | 19 | 0 | 0 (eigene Meta-Annotation) | 2 | 451 |

Gezählt wurden `@Test`-, `@ParameterizedTest`- und `@FuzzTest`-Annotationen per `grep`. Parametrisierte Fälle sind
nicht expandiert, deshalb liegt die Surefire-Zahl höher (724).

Die vier größten Testklassen sind Integrationstests gegen echte Server, zwei davon die Engine-Matrizen
(`RestClientEngineIntegrationTest` mit 1 370 Zeilen, `WebClientEngineIntegrationTest` mit 1 523 Zeilen).

### Analysiert / nicht analysiert, blinde Flecken

- Analysiert wurde der gesamte Produktionscode. Die Engine-Transportklassen sind in ihrer Struktur gelesen, nicht
  Zeile für Zeile auf Korrektheit; dafür sind die Defektanalysen zuständig.
- Ob ein realer Host eine Nicht-Default-Engine braucht, ist aus dem Repository nicht ablesbar. Befund 1 trägt
  deshalb nur Konfidenz *medium*. Ein solcher Bedarf wäre eine rechtfertigende Kraft und würde den Befund auf
  „ADR nachtragen“ reduzieren.
- Die CI-eigenen Doku-Gates (`./.github/workflows/ci.yml`, sieben `grep`-Schritte und zwei Python-Skripte) wurden
  nur gesichtet. Sie sind billig und zweckgebunden, deshalb kein Befund.

---

## 3. Statistics

| Schwere | Anzahl |
|---|---|
| 🔴 Critical | 0 |
| 🟠 High | 2 |
| 🟡 Medium | 1 |
| 🟢 Low | 5 |
| **Befunde gesamt** | **8** |
| Systemische Muster | 2 |

---

## 4. Ranking table

| Einheit | Score | Begründung |
|---|---|---|
| Engine-Transportschicht beider Stacks (`restclient/{Apache,Jdk,Jetty,ReactorNetty}EngineTransport.kt`, `restclient/EngineTransport.kt`, `webclient/WebClientEngineTransports.kt`, `webclient/WebClientEngineChecks.kt`, `core/engine/*`) | 5 | Rund 2 600 Zeilen und 8 Implementierungen. Größte Masse, größte Fehlerdichte, kein dokumentierter Bedarf. |
| Wiring-Modell (`restclient/RestClientConfiguration.kt`, `webclient/WebClientConfiguration.kt`, die `@Component`-Klassen, `core/mesh/MeshResilienceConfiguration.kt`) | 4 | Architekturnaht zum Host, wirkt in jeden Consumer. Bekannte Kollision (ADR-0003). |
| Client-Fabriken (`restclient/ResilientRestClientFactory.kt`, 415 Zeilen; `webclient/ResilientWebClientFactory.kt`, 465 Zeilen) | 4 | Zentrale Knoten (Engine, Validierung, Metriken, Lebenszyklus). Zwillingsreste. |
| `core/resilience/MeshResilienceCustomizers.kt` (Registrar) | 3 | Eigene Bean-Registrierung per `BeanRegistrar`. Begründet (Bean-Anzahl erst nach Binding bekannt), → tragend. |
| `core/resilience/MeshResilienceStartupGuard.kt` | 3 | 275 Zeilen reine Prüflogik. Schützt vor stillem Lauf auf Registry-Defaults, → tragend. |
| `core/resilience/BlockingResilienceDecorator.kt` | 3 | Nebenläufigkeitskern, sync und async. ADR-0002, → tragend. |
| `restclient/ResilientRestClient.kt` / `webclient/ResilientWebClient.kt` | 3 | Per-Call-Pfad. Die Async-API ist durch ADR-0002 begründet. |
| `core/mesh/MeshProperties.kt`, `core/mesh/ResolvedTimeouts.kt` | 3 | Konfigurationsfläche mit Override-Zwillingsklassen. |
| `core/client/ClientCallLog.kt` | 2 | Gemeinsame Per-Call-Logik, 11 Konstruktorparameter. ADR-0001. |
| `webclient/ResilientWebClientFactory.kt` (`WebClientMetrics`) | 2 | Mehr-Template-Logik für einen Ein-Template-Gebrauch. |
| `core/resilience/BreakerVerdicts.kt` | 2 | Zwei Policies plus Bündel, je Stack instanziiert. Angemessen. |
| `core/observation/HttpObservationReport.kt`, `core/validation/ResponseValidation.kt` | 2 | Je zwei Subklassen- bzw. Nutzerstacks. Angemessen. |
| `core/BaseAdapter.kt` | 2 | Bibliotheks-Interface, das die Bibliothek selbst nie liest. |
| `veredium-test-support` | 2 | Geteilte Testinfrastruktur (565 Zeilen) für drei Module. README begründet das eigene Modul, angemessen. |
| Gespiegelte Testsuiten der Stacks | 3 | 9 Paare, rund 10 000 Zeilen (Muster P2). |
| `core/Veredium.kt`, `core/client/EmptyResponseBodyException.kt`, `core/client/ResponseBodyMetrics.kt`, `core/engine/EngineDrain.kt` | 1 | Kleine, fokussierte Helfer. |
| `veredium-examples` | 1 | Build-only, Anschauungscode. |

Abgearbeitet wurde von Score 5 abwärts. Score-1/2-Einheiten wurden nur überflogen, soweit sie nicht unten als
Befund auftauchen.

---

## 5. Findings

### 🟠 High

- [x] 1. [Engine-Transportschicht, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/RestClientEngine.kt:24`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngine.kt:29`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt:176`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:174`] {High} {Confidence: medium} {Over-Engineering / Speculative Generality}
  - Vier wählbare HTTP-Engines je Stack (Apache, JDK, Jetty, Reactor Netty), also acht Transport-Implementierungen, für eine Bibliothek, deren Clients einen lokalen Mesh-Sidecar ansprechen.
    - Actual structure: `app.restclient.engine` / `app.webclient.engine` mit Default plus Override je Alias.
      - Je Engine und Stack eine Transportklasse: Pool, Timeouts, Meter, Drain beim Shutdown und Abbruchsemantik.
      - Dazu Workarounds für Lücken in Springs Adaptern, etwa `AbortingRequest`/`AbortingResponse`, `InterruptRestoringRequest` (Jetty), `AbortableAsyncClient` (reaktives Apache) und den Reactor-Netty-Head-Handoff.
      - 12 `Assumption:`-Kommentare markieren Bibliotheksverhalten, das bei jedem Dependency-Update neu geprüft werden muss.
      - Umfang: rund 2 600 Zeilen Produktionscode (`restclient/*EngineTransport.kt` 1 382, `webclient/WebClientEngineTransports.kt` 807, `WebClientEngineChecks.kt` 206, Enums, `core/engine/*` 155), also rund ein Drittel des Produktionscodes.
      - Tests: rund 4 200 Zeilen (Engine-Integrations-, Isolations-, Lebenszyklus- und Handoff-Tests).
    - Solved problem / justifying force: nicht dokumentiert.
      - Guide, CHANGELOG und `./docs/engine-behaviour.md` beschreiben *was* jede Engine tut und *wie* die Isolation gegenüber Boot gewahrt wird. Welcher Host welche Engine braucht, steht nirgends.
      - Es gibt kein ADR. Die Defaults (Apache bzw. Reactor Netty) decken jeweils den ganzen Funktionsumfang ab.
      - Ein echter zweiter Bedarf ist im Repository nicht sichtbar: die Beispiele nutzen nur die Defaults, ein Consumer existiert noch nicht.
    - Cost:
      - **Fehlerdichte:** 34 von 43 Befunden der beiden letzten Defektanalysen (`DEFECT_ANALYSIS-2026-10-22T14-01-49.md`: 20/24, `DEFECT_ANALYSIS-2026-10-23T05-20-01.md`: 14/19) betreffen Engines.
      - **Wartung:** 13 von 30 Nicht-Merge-Commits seit `1b72625` sind Engine-Fixes, -Tests oder -Doku.
      - **Kopplung an Interna:** Jedes Update von Spring, Jetty, Reactor Netty oder httpclient5 verlangt, die 12 Annahmen und die Engine-Matrix-Tests erneut zu prüfen, und die Matrix verlängert die CI-Laufzeit.
      - **Multiplikation:** Jedes neue Querschnittsthema (Drain, Abbruch, Meter) muss achtmal gelöst werden.
      - **Kognitive Last:** Ein Leser des Clients muss für jede Zeile wissen, welche der acht Transportsemantiken gilt.
    - Simpler alternative: pro Stack eine Engine, nämlich der heutige Default (Apache classic bzw. Reactor Netty). Die übrigen werden erst angeboten, wenn ein konkreter Host sie braucht, und dieser Bedarf wird dann als ADR festgehalten. Falls der Bedarf heute schon existiert: ADR nachtragen, mit Benennung der Engines, die er tatsächlich trägt (vermutlich nicht alle vier je Stack).
    - Reversibility: **jetzt billig, nach dem ersten Release teuer.** Unveröffentlicht heißt: Die Keys `app.*.engine` sind noch kein öffentlicher Vertrag. Der Rückbau ist überwiegend Löschen (Transportklassen, Matrix-Tests, optionale Dependencies) plus das Vereinfachen der Fabriken. Risiko gering, weil die Defaults unverändert bleiben.

- [ ] 2. [Wiring-Modell, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/RestClientConfiguration.kt:38`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientConfiguration.kt:28`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/ResilientRestClientFactory.kt:59`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:59`, `./veredium-examples/src/main/kotlin/eu/inqudium/veredium/examples/blocking/BlockingExampleApplication.kt:11`] {High} {Confidence: medium} {Layering & Dependencies}
  - Die Bibliothek wird über einen Component-Scan ihres eigenen Pakets eingebunden statt über Boots Auto-Configuration.
    - Actual structure: Es gibt keine `AutoConfiguration.imports`.
      - Ein Host muss `scanBasePackages = [..., "eu.inqudium.veredium.restclient"]` setzen. Dann findet Spring die `@Component`-Klassen (Fabrik, Observation-Report, im reaktiven Stack auch `ReactiveResilienceDecorator`) und die `@Configuration`.
      - Die `@Configuration` importiert den Kern-Teil (`MeshResilienceConfiguration`, Registrar).
      - Keine Bean ist `@ConditionalOnMissingBean` oder `@ConditionalOnClass`.
    - Solved problem / justifying force: nicht dokumentiert.
      - ADR-0001 und die Guides setzen den Scan voraus, wägen ihn aber nicht gegen Auto-Configuration ab.
      - Die Schwesterbibliothek legatium bindet sich per Auto-Configuration ein. Die Guides beschreiben sogar, wie legatiums Auto-Configuration sich an Boots Builder hängt (`./veredium-webclient/docs/logging-guide.md`).
    - Cost:
      - **Öffentliche Kopplung an Paketnamen:** Der Paketname wird faktisch öffentliche API. Jede Paketumstrukturierung bricht Hosts.
      - **Keine Steuerung durch den Host:** Ein Host kann keine einzelne Bean der Bibliothek ersetzen oder abschalten (kein Back-off, kein `exclude`). Er bekommt alles oder nichts.
      - **Scan-Reichweite:** Der Scan erfasst alles, was künftig in diesem Paket landet, auch Klassen, die nicht zur Verdrahtung gedacht sind.
      - **Verbindung zu ADR-0003:** Das Kollisionsproblem beider Stacks in einem Kontext wird durch das Modell verschärft. Ohne bedingte Konfigurationsschicht braucht der Lösungsvorschlag einen eigenen `BeanFactoryPostProcessor` als Eigentumsprüfung.
      - **Beleg:** Alle vier Beispiel-Apps müssen das interne Paket nennen.
    - Simpler alternative: je Client-Modul eine `@AutoConfiguration`, eingetragen in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. Ihre Beans sind `@ConditionalOnMissingBean`, die `@Component`-Annotationen entfallen. Ein Host bindet dann nur die Dependency ein, ohne Scan. Das ist das Boot-Standardmuster für Bibliotheken und legatiums Modell.
    - Reversibility: vor dem ersten Release mittel bis billig: eine Konfigurationsklasse je Stack, Annotationen umstellen, Kontexttests anpassen. Danach muss jeder Host seine `scanBasePackages` ändern. Sinnvoll im Zusammenhang mit der Umsetzung von ADR-0003 zu entscheiden.

### 🟡 Medium

- [ ] 3. [Engine-Auswahl und -Lebenszyklus, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/EngineTransport.kt:37`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineTransports.kt:77`, `./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/WebClientEngineChecks.kt:54`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/JdkEngineTransport.kt:169`] {Medium} {Confidence: high} {Consistency}
  - Dasselbe Konzept, eine Engine-Transportschicht mit Startprüfungen, Startzeile, Lebenszyklus und Drain, ist in beiden Stacks in zwei verschiedenen Formen gebaut und driftet bereits.
    - Actual structure:
      - **Blockierend:** abstrakte Klasse `EngineTransport` mit `settingsLine`. Die Transporte werden **eager** in `createAll` erzeugt, mit Aufräumen bei Teilfehlschlag. Die Prüfungen laufen direkt in der Fabrik (`checkedEngineSelection`), der Shutdown-Zustand liegt als `@Volatile` Flag in der Fabrik.
      - **Reaktiv:** `interface EngineTransport` plus Halter `EngineTransports`. Die Transporte werden **lazy** erzeugt, Lock und `closed` liegen im Halter. Die Prüfungen und Abdeckungszeilen stehen in einem eigenen Objekt `WebClientEngineChecks` mit `Knobs`/`Coverage`.
      - Commit `4479734` („one engine-selection shape for both stacks“) hat nur die Auswahl-Prüfungen in `core/engine/EngineSelection.kt` vereinheitlicht, nicht die Transportschicht.
      - **Bereits eingetretene Drift:** Der blockierende Stack lässt `jdk` zu, wenn der Host-Header der Authority der Base-URL entspricht (`hostHeaderConflict`, Prüfung je Alias). Der reaktive Stack lehnt `jdk` ab, sobald die JVM den Host-Header sperrt, unabhängig von der Base-URL (`requireJdkHostHeader`). Dieselbe Konfiguration startet also auf einem Stack und scheitert auf dem anderen. Die Korrektheitsseite gehört in eine Defektanalyse und wird hier nur vermerkt.
    - Solved problem / justifying force: Eager gegen lazy könnte begründet sein (reaktive Engines starten Event-Loops). Dokumentiert ist das nicht, und der Rest der Formdifferenz ist historisch gewachsen. Die Regel „identisch → Kern“ aus ADR-0001 spricht für eine Form.
    - Cost: Jede Querschnittsänderung an der Engine-Schicht (Drain, Startzeile, Prüfregel) wird zweimal unterschiedlich implementiert und zweimal getestet (`EngineTransportLifecycleTest` gegen `EngineTransportsTest`). Gleichheitsregeln (Host-Prüfung) driften unbemerkt, weil kein Test sie über beide Stacks gemeinsam festhält.
    - Simpler alternative: Hängt an Befund 1. Fällt die Engine-Wahl weg, entfällt der Befund weitgehend. Bleibt sie, gehören eine gemeinsame Lebenszyklus-Form (Halter mit Lazy-Start, Lock und Drain) und die Startregeln, inklusive der JDK-Host-Prüfung, in `core/engine`. Die Stacks liefern dann nur die engine-spezifischen Fabrikfunktionen.
    - Reversibility: mittel, intern (`internal`-Klassen, keine öffentliche API). Am billigsten zusammen mit Befund 1.

### 🟢 Low

- [ ] 4. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ResilientWebClientFactory.kt:415`] {Low} {Confidence: high} {Premature Abstraction}
  - `WebClientMetrics.tagsOf` sortiert mehrere Templates nach Präfixlänge und bricht Gleichstände nach dem Template. Der einzige Produktionsaufrufer `uriTagger` übergibt genau ein Template, weil er laut eigener KDoc die Mehr-Template-Mehrdeutigkeit bewusst vermeidet. Die Sortier- und Tie-Break-Logik und ihre Tests (`WebClientMetricsTest`) pflegen einen Fall, den der Produktionspfad ausschließt. Alternative: `uriTagger` direkt über das eine Template (Präfix bis `{`) ausdrücken, `tagsOf` entfernen. Billig.

- [ ] 5. [`./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/mesh/MeshProperties.kt:248`] {Low} {Confidence: medium} {Excessive Configurability}
  - `MarginsOverride` spiegelt alle sechs Margins als nullable Zwilling für Overrides je Endpoint. Ebenso spiegelt `BackendToggleOverride` (Zeile 282) `BackendToggle` (Zeile 273), eine Klasse mit nur einem Boolean.
    - Weder Beispiele noch Test-YAMLs noch Guides zeigen einen Endpoint, der eine Margin überschreibt. Der Guide nennt die Möglichkeit nur („partial overrides are field-merged, per endpoint too“).
    - Kosten: Jede neue Margin braucht zwei Felder, einen Merge-Zweig und Validierung samt Quellenhinweis, und die Konfigurationsfläche wird größer.
    - Alternative: Margins nur global. Overrides je Endpoint erst dann, wenn ein gemessener Endpoint sie braucht. Die Zeitbudgets je Endpoint deckt bereits `individual-request-timeout-ms` ab.
    - Vor dem Release billig, danach ein Breaking Change der Konfiguration.

- [ ] 6. [`./veredium-webclient/src/main/kotlin/eu/inqudium/veredium/webclient/ReactiveResilienceDecorator.kt:97`, `./veredium-restclient/src/main/kotlin/eu/inqudium/veredium/restclient/RestClientConfiguration.kt:46`] {Low} {Confidence: high} {Consistency}
  - Die beiden Decoratoren sind verschieden verdrahtet. Der blockierende ist annotationsfrei und wird per `@Bean` in `RestClientConfiguration` deklariert (ADR-0002). Der reaktive ist ein gescanntes `@Component`.
    - Kosten: Ein Leser muss zwei Verdrahtungswege kennen. Ein Host kann den reaktiven Decorator nur über den Scan bekommen.
    - Alternative: beide als `@Bean` der Stack-Konfiguration. Das fällt mit Befund 2 automatisch an.

- [ ] 7. [`./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/client/ClientCallLog.kt:282`, `./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/resilience/BlockingResilienceDecorator.kt:300`] {Low} {Confidence: medium} {Coupling & Cohesion}
  - Die MDC-Übergabe an einen anderen Thread ist zweimal implementiert: `ClientCallLog.withMdc` (nur vom reaktiven Client benutzt) und `BlockingResilienceDecorator.propagate` (dazu noch Observation-Scope). `withMdc` sitzt im Companion einer Logging-Klasse, obwohl es Kontextweitergabe ist.
    - Kosten: Zwei Stellen für dieselbe Restore-Semantik (`null` → `clear`), die auseinanderlaufen können.
    - Alternative: ein kleiner MDC-Helfer im Kern, den beide nutzen.
    - Billig, rein intern.

- [ ] 8. [`./veredium-core/src/main/kotlin/eu/inqudium/veredium/core/BaseAdapter.kt:26`] {Low} {Confidence: low} {Speculative Generality}
  - `BaseAdapter` ist ein öffentliches Interface mit zwei Budget-Gettern. Die Bibliothek implementiert und liest es selbst nie, nur die Host-Adapter der Beispiele implementieren es.
    - Dieselben Werte liefern `ResilientRestClient` und `ResilientWebClient` bereits als Properties.
    - Das KDoc trägt zudem Client-Verhalten (`DatabindException`, Breaker-Einordnung), das nicht zum Interface gehört.
    - Kosten: gering. Ein Typ im Kernpaket, dessen Nutzen allein beim Host liegt, und Dokumentation am falschen Ort.
    - Alternative: als Empfehlung im Guide führen oder bewusst behalten, wenn Hosts damit Budgets über mehrere Adapter summieren sollen. Dann sollte das im KDoc stehen, und das Client-Verhalten wandert in die Client-KDoc.
    - Taste-nah, Backlog.

---

## 6. Systemic patterns

### P1: Zwillingsreste zwischen den Stack-Fabriken (rund 7 Stellen, gezählt per Gegenüberstellung der beiden Fabriken)

ADR-0001 zieht die Grenze bei „Client, Decorator, Fabrik, Properties, Verdicts bleiben je Stack“. Innerhalb der
beiden Fabriken stehen aber mehrere konstruktionsbedingt identische Blöcke, die nach ADR-0001s eigener Regel in den
Kern gehören:

- die Validierungs-Startzeile (`restclient/ResilientRestClientFactory.kt:132` / `webclient/ResilientWebClientFactory.kt:121`);
- die `poolDisposeTimeout`-Formel (`:156` / `:149`);
- die Fehlermeldung „No mesh endpoint configured“ (`:289` / `:221`);
- der Header-Block in `build()` (Host, User-Agent, Endpoint-Header, Accept, Adapter-Attribut) (`:282` / `:217`);
- der Meter-Aufbau `responseBodySize` (`:354` / `:284`);
- der Client-Cache `create`/`computeIfAbsent`;
- die Ableitung `meshByBackend`/`meshTransport`.

Die Engine-Schicht kommt hinzu (Befund 3). Die Drift ist bereits sichtbar: Der Shutdown-Check sitzt einmal als Flag
in der Fabrik, einmal im Lock des Transporthalters, und die JDK-Host-Prüfung unterscheidet sich. Das Muster
wiederholt in kleinerem Maßstab genau das Problem, das ADR-0001 für die Kernlogik gelöst hat.

*Richtung:* Eine kleine Kernklasse („Mesh-Client-Plan“: Endpoints, Dispose-Budget, Default-Header, Meter-Fabrik,
Startzeilen) bekommt den stackneutralen Teil. Die Fabriken behalten nur Builder, Engine und Client-Konstruktion.
Schwere: Medium, wenn die Engine-Wahl bleibt, sonst Low. Der größte Teil der Divergenz steckt in der Engine-Schicht.

### P2: Gespiegelte Testsuiten (9 Paare, rund 10 000 Zeilen, gezählt per Dateiname)

`ResilientXIntegrationTest`, `XEngineIntegrationTest`, `LegatiumExchangeLoggingIntegrationTest`,
`XEngineIsolationTest`, `XFactoryBuilderWiringTest`, `LoggingLevelContractTest`, `MeshResilienceShippedConfigTest`,
`ResilienceConfigContractTest`, `XVerdictsTest`/`XPropertiesTest`: Sie existieren je Stack mit gleicher Gliederung
und gleichen Rationale-Blöcken.

Das ist **überwiegend tragend**. Die beiden APIs (blockierend/`CompletableFuture` gegen `Mono`) lassen sich nicht
ohne eine eigene Test-DSL gemeinsam ansprechen, und eine solche wäre selbst Over-Engineering. Die geteilten
Bausteine liegen bereits in `veredium-test-support`.

Kosten entstehen fast nur im Engine-Teil (rund 2 900 Zeilen Engine-Matrix plus 577 Zeilen Isolation) und folgen
damit Befund 1. Was fehlt, ist ein **gemeinsamer** Vertragstest für stackübergreifende Startregeln. Er hätte die
Drift aus Befund 3 gezeigt: eine kleine, parametrisierte Tabelle im Kern, die beide Prüfungen gegen dieselben Fälle
laufen lässt.

---

## Bearbeitungsstand

- **Befund 1 (2026-10-23): nicht behoben, bewusste Entscheidung des Maintainers.** Die Engine-Auswahl ist gewollte
  Funktionalität. Damit liegt die rechtfertigende Kraft vor, die der Befund vermisst hat; die genannten Kosten
  (Fehlerdichte, Wartung, Testmatrix) werden als Preis dieses Features getragen.
  Festgehalten in `./docs/adr/ADR-0004-the-http-engine-is-the-hosts-choice.md` (Accepted).
  - Folge für Befund 3: Es gilt der Zweig „bleibt sie“ der dort genannten Alternative. Gemeinsame Lebenszyklus-Form
    und gemeinsame Startregeln (inklusive JDK-Host-Prüfung) in `core/engine` sind damit der Weg, die Kosten der
    Engine-Matrix zu begrenzen.
  - Folge für Muster P1: Die Schwere bleibt Medium (die Engine-Schicht bleibt bestehen).

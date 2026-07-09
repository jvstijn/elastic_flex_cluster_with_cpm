# Werkinstructie — `scripts/cpm_groepen.py`

## 1. Doel

`cpm_groepen.py` verdeelt rijen uit een CSV (bijv. datastreams met een
record-telling) over **N groepen**, zó dat de **som per groep zo gelijk mogelijk**
is. Elke groep krijgt bovendien een **minimum aantal items**.

Typisch gebruik: een lijst datastreams (met hun volume) evenwichtig verdelen over
een vast aantal buckets — bijvoorbeeld pipelines, workers of clusters — zodat geen
enkele bucket veel zwaarder belast wordt dan de andere.

## 2. Vereisten

- **Python 3** (alleen de standaardbibliotheek — geen `pip install` nodig).
- Een invoer-CSV met een **header** en minimaal **2 kolommen**.

## 3. Invoerformaat (CSV)

- **Header verplicht.** Het scheidingsteken (`,` `;` `|` of tab) wordt automatisch
  gedetecteerd; bij twijfel valt het script terug op komma.
- **Laatste kolom = de waarde** (het getal dat verdeeld wordt).
- **Alle kolommen ervóór** worden samengevoegd tot één sleutel, gescheiden door `-`.

Voorbeeld (4 kolommen, `;`-gescheiden):

```csv
"data_stream.type";"data_stream.dataset";"data_stream.namespace";"Count of records"
metrics;"elastic_agent.filebeat_input";default;50316177
logs;"nginx.access";prod;2.318.582
```

Voorbeeld (2 kolommen, `,`-gescheiden):

```csv
event.dataset,aantal
apache.access,123456
```

### Ondersteunde getalnotaties

De waardekolom mag in diverse notaties staan; het script normaliseert ze:

| Invoer | Wordt gelezen als |
|--------|-------------------|
| `2318582` | 2318582 |
| `2.318.582` (punt = duizendtallen) | 2318582 |
| `2.318.582,677` (EU-notatie) | 2318582.677 |
| `2318582.677` (punt = decimaal) | 2318582.677 |
| `2 318 582` (spaties) | 2318582 |

Rijen met een **ongeldige** of **negatieve** waarde worden overgeslagen met een
waarschuwing (het script stopt niet).

## 4. Gebruik

Basis:

```bash
python3 scripts/cpm_groepen.py <invoer.csv>
```

Met opties:

```bash
python3 scripts/cpm_groepen.py <invoer.csv> --groepen 8 --min-items 2 --output resultaat.csv
```

### Opties

| Optie | Standaard | Betekenis |
|-------|-----------|-----------|
| `invoer` (verplicht) | — | Pad naar de invoer-CSV |
| `--groepen N` | `8` | Aantal groepen (minimaal 2) |
| `--min-items M` | `2` | Minimum aantal items per groep (minimaal 1) |
| `--output BESTAND` | `<invoernaam>_groepen.csv` | Pad voor de uitvoer-CSV |

> **Let op:** het aantal rijen moet minstens `--groepen × --min-items` zijn, anders
> stopt het script met een foutmelding.

## 5. Uitvoer

### a) Overzicht op het scherm (stdout)

```
  Inlezen: sample.csv
  8 rijen ingelezen.
  Reparatie: 'logs-oracle.audit-default' (330,000) van groep 2 naar groep 1 (minimum 2 items)

============================================================
  Totaal:      81,658,302
  Doel per groep: 27,219,434
  Aantal groepen: 3
============================================================

  Groep  1  |  som:   50,646,177  |  afwijking: +23,426,743  |  items: 2
                metrics-elastic_agent.filebeat_input-default     50,316,177
                logs-oracle.audit-default                           330,000
  Groep  2  |  som:   15,441,023  |  afwijking: -11,778,411  |  items: 3
                metrics-elastic_agent.metricbeat-default         12,345,678
                logs-nginx.access-prod                            2,318,582
                logs-nginx.error-prod                               776,763
  Groep  3  |  som:   15,571,102  |  afwijking: -11,648,332  |  items: 3
                ...
```

- **Doel per groep** = totaal ÷ aantal groepen (de ideale som).
- **afwijking** = som van de groep t.o.v. het doel (`+` = boven doel, `-` = onder).

### b) Uitvoer-CSV

Standaard `<invoernaam>_groepen.csv`, met vier kolommen:

```csv
groep,sleutel,aantal,groep_som
1,metrics-elastic_agent.filebeat_input-default,50316177,50646177
1,logs-oracle.audit-default,330000,50646177
2,metrics-elastic_agent.metricbeat-default,12345678,15441023
...
```

| Kolom | Betekenis |
|-------|-----------|
| `groep` | Groepsnummer (1..N) |
| `sleutel` | Samengevoegde sleutel uit de invoerkolommen (met `-`) |
| `aantal` | De waarde van die rij |
| `groep_som` | Totale som van de groep (herhaald op elke rij van die groep) |

## 6. Hoe de verdeling werkt (kort)

1. **Greedy least-loaded:** de rijen worden van groot naar klein gesorteerd en
   één voor één toegewezen aan de groep met op dat moment de **laagste som**. Dit
   geeft een goede (niet gegarandeerd perfecte) balans.
2. **Minimum-reparatie:** heeft een groep na stap 1 minder dan `--min-items`
   items, dan verschuift het script het **kleinste** item uit een groep die er
   genoeg heeft naar de te kleine groep. Elke verschuiving wordt geprint als
   `Reparatie: ...`.

> Bij sterk uiteenlopende waarden (één hele grote rij) kan één groep flink boven
> het doel uitkomen — dat is inherent: een enkel item kan nu eenmaal niet
> gesplitst worden.

## 7. Voorbeelden

Verdeel `streams.csv` over 8 groepen (standaard), schrijf naar `streams_groepen.csv`:

```bash
python3 scripts/cpm_groepen.py streams.csv
```

Over 4 groepen, minstens 3 items per groep, eigen uitvoernaam:

```bash
python3 scripts/cpm_groepen.py streams.csv --groepen 4 --min-items 3 --output verdeling.csv
```

## 8. Foutmeldingen

| Melding | Oorzaak / oplossing |
|---------|---------------------|
| `bestand '...' niet gevonden` | Controleer het pad naar de invoer-CSV |
| `CSV heeft minder dan 2 kolommen` | Header/scheidingsteken klopt niet; zorg voor ≥ 2 kolommen |
| `geen geldige rijen gevonden in CSV` | Alle waarden ongeldig/leeg; controleer de laatste kolom |
| `... is te weinig voor N groepen met minimaal M items` | Meer rijen nodig, óf verlaag `--groepen` / `--min-items` |
| `minimaal 2 groepen vereist` | `--groepen` moet ≥ 2 zijn |
| `kan minimum niet garanderen (te weinig items)` | Totaal aantal items < `groepen × min-items` |
| `Waarschuwing: rij X overgeslagen` | Die rij had een ongeldige/negatieve waarde; script gaat door |

## 9. Snelle referentie

```bash
# standaard (8 groepen, min 2 items, uitvoer = <naam>_groepen.csv)
python3 scripts/cpm_groepen.py input.csv

# volledige vorm
python3 scripts/cpm_groepen.py input.csv --groepen 8 --min-items 2 --output resultaat.csv
```

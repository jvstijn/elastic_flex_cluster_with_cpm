# CPM voor ontwikkelaars

Alles wat je nodig hebt om aan Cluster Pipeline Manager te werken. Dit document
staat op zichzelf: je hoeft er geen ander document bij te pakken.

Twee talen komen voorbij die je waarschijnlijk niet kent. **Painless** is de
scripttaal van Elasticsearch en draagt vrijwel alle logica van CPM. **Mustache**
is de sjabloontaal waarmee een watcher zijn HTTP-aanroepen opbouwt. Hoofdstuk 3
en 4 leren je precies zoveel van allebei als je hier nodig hebt.

---

## 1. Het idee in één bladzijde

Er zijn meerdere Elasticsearch-clusters en één Kafka-cluster met per data stream
een topic. Iemand moet bepalen welk cluster welke topics consumeert. CPM doet dat
automatisch en legt het resultaat vast als Logstash-pipelines.

CPM is geen service en geen container. Het is een verzameling **watchers**:
geplande taken die binnen Elasticsearch zelf draaien. Elke watcher zoekt wat op,
rekent er iets mee, en schrijft het resultaat terug via de REST-API.

De uitkomst is een set pipelines onder `/_logstash/pipeline` op het centrale
cluster. De Logstash-instanties halen die zelf op; CPM praat nooit rechtstreeks
met Logstash.

```
   monitoring-data  ──►  registry ──► scores ──► suggesties ──► state ──► pipelines
   (wat bestaat er)      (welke      (hoe vol   (wat zou      (wat     (wat draait
                          clusters)   zit het)   beter zijn)   hoort)   er echt)
```

Alle CPM-toestand staat in gewone Elasticsearch-indices op het centrale cluster,
met het prefix `cpm-`. Er is geen database, geen state-bestand en geen geheugen
tussen runs. Wil je weten wat CPM denkt, dan query je een index.

---

## 2. Anatomie van een watcher

Een watcher is één JSON-document met vijf onderdelen. Zo ziet het skelet eruit,
sterk ingekort:

```json
{
  "trigger":   { "schedule": { "cron": "0 15 0 * * ?" } },
  "input":     { "chain": { "inputs": [ ... zoekopdrachten ... ] } },
  "condition": { "always": {} },
  "transform": { "script": { "source": "... Painless ...", "lang": "painless" } },
  "actions":   { "index_state": { "webhook": { ... } } }
}
```

**trigger** bepaalt wanneer hij draait. Bij CPM is dat meestal een cron, één keer
per nacht. `cpm-forecast-trigger` is de uitzondering en draait op een interval van
een uur.

**input** haalt data op. CPM gebruikt vrijwel altijd een `chain` van meerdere
zoekopdrachten. Elke zoekopdracht krijgt een naam, en het resultaat komt onder
die naam in de payload te staan. Dit:

```json
"input": { "chain": { "inputs": [
  { "registry": { "search": { "request": {
      "indices": ["cpm-cluster-registry"],
      "body": { "size": 32, "query": { "match_all": {} } } } } } },
  { "scores":   { "search": { "request": {
      "indices": ["cpm-scores"],
      "body": { "size": 1, "sort": [{"scored_at": "desc"}] } } } } }
] } }
```

levert in het script `ctx.payload.registry.hits.hits` en
`ctx.payload.scores.hits.hits` op.

**condition** bepaalt of de acties mogen draaien. CPM zet dit meestal op
`always`, en filtert pas in de acties zelf.

**transform** is het rekenwerk, in Painless. Wat het script `return`t vervangt de
payload voor de acties.

**actions** doen het schrijfwerk. CPM gebruikt uitsluitend `webhook`-acties die
de REST-API van het eigen cluster aanroepen. Dat voelt omslachtig, maar een
watcher heeft geen andere manier om een willekeurig endpoint te benaderen, en CPM
moet naar `/_logstash/pipeline` en `/_ml/.../_forecast` kunnen schrijven.

### Uitvoeringsvolgorde

`trigger` → `input` → `condition` → `transform` → `actions`. De acties draaien in
de volgorde waarin ze in het document staan.

### Een watcher testen zonder te wachten

```bash
curl -sk -u elastic:$PW -X POST \
  https://central.example.net/_watcher/watch/cpm-state-manager/_execute \
  -H 'Content-Type: application/json' \
  -d '{"record_execution":false,"action_modes":{"_all":"simulate"}}'
```

Twee vlaggen die je uit elkaar moet houden:

- `record_execution: false` onderdrukt alleen het schrijven naar de
  watcher-historie. **De acties draaien gewoon.** Dit is dus geen droogloop.
- `action_modes: {"_all": "simulate"}` maakt het wél een droogloop.

De respons bevat het volledige tussenresultaat: `result.input.payload` met wat de
zoekopdrachten opleverden, en `result.transform.payload` met wat je script
teruggaf. Dat is je debugger.

### De belangrijkste valkuil in het hele project

Een watcher kan `success` melden terwijl er niets gebeurt. Kijk daarom altijd
eerst naar de status van de **transform**, niet naar die van de acties:

```json
"transform": { "status": "failure", "reason": "runtime error", ... }
```

Faalt de transform, dan is de payload leeg en doen de acties niets, maar de
watcher zelf rapporteert geen fout op het niveau waar de meeste mensen kijken.

---

## 3. Painless in twintig minuten

Painless is de scripttaal van Elasticsearch. Het lijkt op Java, maar is bewust
beperkt: geen imports, geen eigen classes, geen bestandstoegang, geen netwerk.
Je krijgt een script, een payload en een returnwaarde.

### Wat je nodig hebt

```painless
// typen
String naam = "logs-nginx-default";
long teller = 0L;
double score = 20.5;
boolean actief = true;

// def = "wat het ook is", vergelijkbaar met var/any
def waarde = ctx.payload.registry.hits.hits;

// collecties
Map m = new HashMap();
m.put('cluster_id', 'dc-a');
List l = new ArrayList();
l.add('logs-nginx-default');

// lezen mag met punt of met haakjes
String cid = (String)m.get('cluster_id');
String c2  = (String)m['cluster_id'];

// itereren
for (def hit : ctx.payload.registry.hits.hits) {
  String id = (String)hit._source.cluster_id;
}
for (int i = 0; i < l.size(); i++) { ... }

// stringbewerkingen die CPM veel gebruikt
naam.startsWith('logs-');
naam.endsWith('-default');
naam.indexOf('-');
naam.lastIndexOf('-');
naam.substring(0, 5);
naam.equals('x');          // gebruik dit, niet ==
naam.compareTo('y') < 0;   // alfabetische volgorde
```

### Wat er anders is dan je gewend bent

- **Vergelijk strings met `equals`.** `==` werkt soms wel en soms niet.
- **Er is geen `var`.** Gebruik `def` of een echt type.
- **Geen lambda's in CPM.** Painless kent ze, maar de code hier gebruikt gewone
  lussen. Houd dat aan; het scheelt verrassingen bij het debuggen.
- **Regex kan, maar wordt niet gebruikt.** Elasticsearch staat regex in Painless
  standaard beperkt toe. CPM doet alle patroonherkenning met de hand, met
  `startsWith` en `endsWith`. Dat is een historische keuze, geen technische eis.
- **Casten is vaak verplicht.** Haal je iets uit een `Map` of uit de payload, dan
  is het een `def`. Wil je er een string-methode op aanroepen, cast dan eerst:
  `((String)m.get('topic')).startsWith('logs-')`.

### De payload lezen

`ctx.payload` is de uitvoer van de inputs. Voor een zoekopdracht met de naam
`registry` zijn dit de bruikbare paden:

```painless
ctx.payload.registry.hits.hits           // lijst met documenten
ctx.payload.registry.hits.hits[0]._id    // document-id
ctx.payload.registry.hits.hits[0]._source.cluster_id
ctx.payload.registry.aggregations.by_cluster.buckets
```

`ctx.execution_time` is het moment van uitvoeren, bruikbaar als tijdstempel:

```painless
String ts = ctx.execution_time.toString();
```

### Altijd op null controleren

Dit is geen stijlkwestie maar de meest voorkomende storing in dit project. Een
zoekopdracht over een **cross-cluster alias** komt bij een onbereikbaar cluster
niet terug met een fout maar met een leeg resultaat **zonder** `aggregations`.
Dit crasht dan:

```painless
def buckets = ctx.payload.dataset_rates.aggregations.by_cluster.buckets;
// cannot access method/field [by_cluster] from a null def reference
```

Doe het altijd zo:

```painless
def buckets = new ArrayList();
if (ctx.payload.dataset_rates != null
    && ctx.payload.dataset_rates.aggregations != null
    && ctx.payload.dataset_rates.aggregations.by_cluster != null
    && ctx.payload.dataset_rates.aggregations.by_cluster.buckets != null) {
  buckets = ctx.payload.dataset_rates.aggregations.by_cluster.buckets;
}
```

Zonder die controle stopt de hele transform, schrijft de watcher niets weg, en
meldt hij nog steeds `success` op actieniveau.

### Wat het script teruggeeft

De returnwaarde vervangt de payload voor de acties. CPM geeft standaard een `Map`
terug met lijsten erin:

```painless
Map out = new HashMap();
out.put('state', stateEntries);              // lijst met documenten
out.put('state_deletions', stateDeletions);  // lijst met ids
return out;
```

Een actie kan daarna over zo'n lijst itereren, zie het volgende hoofdstuk.

### Painless los uitproberen

```bash
curl -sk -u elastic:$PW -X POST \
  https://central.example.net/_scripts/painless/_execute \
  -H 'Content-Type: application/json' -d '{
    "script": {
      "source": "return params.t.startsWith(params.p);",
      "params": { "t": "logs-nginx-default", "p": "logs-" }
    }}'
```

Dit is verreweg de snelste manier om een stukje logica te testen zonder een
watcher uit te rollen.

---

## 4. Mustache in tien minuten

Mustache is de sjabloontaal van de acties. Waar Painless bepaalt *wat* er
geschreven wordt, bepaalt Mustache *hoe* de HTTP-aanroep eruitziet.

Je hebt maar drie dingen nodig.

### Een waarde invullen

```json
"path": "/cpm-pipeline-state/_doc/{{ctx.payload.topic}}"
```

Alles tussen dubbele accolades wordt vervangen door de waarde uit de payload.

### Een heel object als JSON wegschrijven

```json
"body": "{{#toJson}}ctx.payload{{/toJson}}"
```

`toJson` is een uitbreiding van Elasticsearch op standaard-Mustache. Het zet het
hele object om naar JSON. Zonder die wrapper krijg je de tekstweergave van een
Java-object, en dat is geen geldige JSON.

### Herhalen over een lijst

Dit gebeurt niet in Mustache zelf maar met het veld `foreach` op de actie:

```json
"index_state": {
  "foreach": "ctx.payload.state",
  "max_iterations": 16384,
  "webhook": {
    "method": "put",
    "path": "/cpm-pipeline-state/_doc/{{ctx.payload.data_stream_type}}-{{ctx.payload.dataset}}-{{ctx.payload.namespace}}",
    "body": "{{#toJson}}ctx.payload{{/toJson}}"
  }
}
```

Let goed op wat er gebeurt: **binnen een `foreach` betekent `ctx.payload` niet
meer de hele payload, maar het huidige element uit de lijst.** Dat is de meest
verwarrende regel van het hele mechanisme. De actie hierboven draait dus één PUT
per item in `ctx.payload.state`, en `{{ctx.payload.dataset}}` is het veld
`dataset` van dát item.

### Een actie faalt op een niet-2xx antwoord

Een `webhook`-actie meldt `failure` bij elke statuscode buiten 2xx, ook bij een
404 op een DELETE van iets dat er niet is. Wil je dat niet, dan moet je in
Painless zorgen dat je zo'n aanroep niet aanmaakt.

---

## 5. De Jinja-laag eromheen

De watchers worden uitgerold met Ansible. De bestanden in
`ansible/roles/elastic_cpm/templates/` zijn Jinja2-sjablonen met de extensie
`.json.j2`, en Ansible vult daar omgevingsspecifieke waarden in.

Hier zit een probleem: **Jinja2 en Mustache gebruiken allebei `{{ }}`.** Zou
Ansible het hele bestand verwerken, dan probeert Jinja2 `{{ctx.payload.topic}}`
te vervangen door een variabele `ctx` die niet bestaat.

De oplossing is dat het hele bestand in een `{% raw %}`-blok staat, dat alleen
opengebroken wordt waar Ansible écht iets moet invullen:

```jinja
{% raw %}
{
  "actions": {
    "index_state": {
      "webhook": {
        "host": "{% endraw %}{{ webhook_host }}{% raw %}",
        "path": "/cpm-pipeline-state/_doc/{{ctx.payload.topic}}",
        "headers": {
          "Authorization": "ApiKey {% endraw %}{{ watcher_api_key.encoded }}{% raw %}"
        }
      }
    }
  }
}
{% endraw %}
```

Lees dat zorgvuldig. `{{ webhook_host }}` staat **buiten** raw en wordt door
Ansible ingevuld. `{{ctx.payload.topic}}` staat **binnen** raw en blijft staan,
zodat Mustache hem later ziet.

Vergeet je een `{% endraw %}`, dan krijg je meestal een lege waarde of een
Jinja-fout bij het uitrollen. Vergeet je een `{% raw %}`, dan verdwijnt je
Mustache-expressie zonder foutmelding.

### Uitrollen

```bash
cd ansible
.venv/bin/ansible-playbook -i inventories/<omgeving>/hosts site.yml --tags watchers
```

De taak leest het sjabloon en doet er een `PUT /_watcher/watch/<id>` mee. Een
watcher heeft geen versiebeheer: de PUT vervangt hem volledig.

---

## 6. Het datamodel

Alle toestand staat in indices met het prefix `cpm-` op het centrale cluster.

| index | rol |
|---|---|
| `cpm-cluster-registry` | welke clusters bestaan er en doen ze mee |
| `cpm-routing-config` | globale gewichten (`_global`) en stream locks |
| `cpm-stream-exclusions` | patronen van streams die buiten CPM blijven |
| `cpm-scores` | uitkomst van de laatste scoringsronde |
| `cpm-routing-suggestions` | voorstellen van de advisor |
| `cpm-pipeline-state` | **de waarheid**: welk topic in welke pipeline hoort |
| `cpm-pipeline-templates` | de Logstash-sjablonen en hun instellingen |
| `cpm-pipeline-history` | historie van verplaatsingen |
| `cpm-stream-coverage` | streams die door geen enkele pipeline gedekt worden |
| `cpm-watcher-executions` | afgevlakte watcher-historie voor het dashboard |
| `cpm-bytes-per-event` | gemiddelde eventgrootte per stream |
| `cpm-cluster-scores` | scores per cluster als tijdreeks |

### cpm-pipeline-state

De belangrijkste index. Eén document per data stream, met als id
`<type>-<dataset>-<namespace>`.

```json
{
  "data_stream_type": "logs",
  "dataset": "nginx",
  "namespace": "default",
  "topic": "logs-nginx-default",
  "pipeline_type": "catchall",
  "pipeline_id": "dc-central_cpm-catchall-Xa732PlTR4S3tlxY9n2FSQ",
  "cluster_id": "Xa732PlTR4S3tlxY9n2FSQ",
  "dc": "dc-central",
  "home_cluster_id": "Xa732PlTR4S3tlxY9n2FSQ",
  "promoted_reason": "cluster had geen dedicated pipeline",
  "updated_at": "2026-09-03T10:23:11.000Z"
}
```

`home_cluster_id` is het cluster waar de data volgens monitoring fysiek staat.
`cluster_id` is waar CPM hem naartoe stuurt. Verschillen die twee, dan is de
stream verplaatst.

### Strikte mappings

Bijna alle `cpm-`-indices hebben `"dynamic": "strict"`. Schrijf je een veld dat
niet in de mapping staat, dan **weigert Elasticsearch het hele document**:

```
StrictDynamicMappingException: mapping set to strict,
dynamic introduction of [promoted_reason] within [_doc] is not allowed
```

Voeg je dus een veld toe in Painless, voeg het dan in dezelfde wijziging toe aan
`ansible/roles/elastic_cpm/files/json/index_<naam>.json`. De taak met de tag
`indices` stuurt het hele `mappings`-blok naar `PUT /<index>/_mapping`, dus
bestaande omgevingen worden bij de eerstvolgende run bijgewerkt.

Eén index wijkt bewust af. `cpm-watcher-executions` staat op `"dynamic": false`,
omdat daar documenten uit de watcher-historie van Elasticsearch zelf in landen.
Die bron kan velden toevoegen waar wij geen zeggenschap over hebben, en `strict`
zou de transform dan permanent laten falen.

---

## 7. De watchers, één voor één

Zeven watchers. Zes vormen een keten, de zevende staat er los van.

### cpm-registry-sync

Ontdekt welke clusters bestaan.

Leest een aggregatie over de monitoring-index, gegroepeerd op
`elasticsearch.cluster.id`, en schrijft per cluster een document in
`cpm-cluster-registry` met de capaciteit: totale schijf, maximale heap en het
aantal nodes.

Belangrijk voor ontwikkelaars: de velden `dc`, `ingest_hosts` en `active` worden
**overgenomen uit het bestaande document** en anders op een default gezet. Een
nieuw ontdekt cluster krijgt `dc: unknown` en `active: false`. Die velden komen
uit de Ansible-inventory, niet uit monitoring, en niet uit het node-attribuut
`node.attr.dc` ondanks wat een comment elders suggereert.

### cpm-forecast-trigger

Roept per ML-job `POST /_ml/anomaly_detectors/<job>/_forecast` aan, met een
horizon van 24 uur. Verder gebeurt er niets. Deze watcher heeft geen input en
geen transform.

### cpm-scoring

Rekent per cluster een score uit.

Leest vier aggregaties over `.ml-anomalies-*`, elk gefilterd op één job en op
`result_type: model_forecast` binnen het venster `now` tot `now+24h`, gegroepeerd
op `partition_field_value` (dat is de cluster-id) met `max(forecast_prediction)`.

De score is dus een **voorspelde piek**, geen huidige meting. De vier waarden
worden genormaliseerd naar 0-100 en gewogen opgeteld:

```painless
double diskScore  = dtotal  > 0 ? (diskB  / dtotal)  * 100.0 : 0.0;
double jvmScore   = jvmP;
double shardScore = smax    > 0 ? (shardC / smax)    * 100.0 : 0.0;
double loadScore  = qthresh > 0 ? (queueD / qthresh) * 100.0 : 0.0;
double total = wDisk*diskScore + wJvm*jvmScore + wShard*shardScore + wLoad*loadScore;
```

Standaardgewichten zijn 0,50 voor disk, 0,25 voor jvm, 0,05 voor shard en 0,20
voor load. Ze worden overschreven door `cpm-routing-config/_global` als dat
document bestaat. De noemer voor disk komt uit `disk_total_bytes` in de registry.

Het resultaat is **één document** met een array `clusters` erin, niet één
document per cluster.

### cpm-routing-advisor

Stelt verplaatsingen voor.

Berekent per stream een rate uit de groei van
`elasticsearch.index.total.indexing.index_total` tussen de eerste en de laatste
uurbucket binnen `now-2h`:

```painless
def tb = ib['over_time']['buckets'];
if (tb.size() >= 2) {
  def v0 = tb.get(0)['max_total']['value'];
  def v1 = tb.get(tb.size() - 1)['max_total']['value'];
  if (v0 != null && v1 != null) {
    double diff = ((Number)v1).doubleValue() - ((Number)v0).doubleValue();
    if (diff > 0.0) delta = (long)diff;
  }
}
...
if (rate <= 0L) continue;
```

Die laatste regel is belangrijker dan hij lijkt. Een stream zonder **groei**
tussen twee uurbuckets telt niet mee. Eén grote bulk-load levert dus geen rate op:
de teller springt één keer en staat daarna stil. Voor testen heb je doorlopende
instroom nodig.

Daarna sorteert hij de streams op rate en neemt de top-N, waarbij N het aantal
clusters is. Elk cluster mag hoogstens één keer doelwit zijn. Er zijn twee fasen:

1. **Greedy.** Voor elke stream het lichtste nog ongebruikte cluster. Is bron
   gelijk aan doel, dan wordt het een lokale dedicated pipeline. Is het doel
   lichter dan de bron (`tgtScore < srcScore`), dan een verplaatsing. Anders naar
   fase 2.
2. **Gepaarde ruil.** Een stream naar een zwaarder cluster mag alsnog, maar alleen
   als dat cluster in dezelfde ronde meer rate kwijtraakt dan deze stream
   toevoegt.

De conditie `tgtScore < srcScore` verklaart waarom er bij vrijwel gelijke scores
weinig gebeurt: er is dan nooit een lichter cluster.

### cpm-state-manager

Het hart. Bepaalt welk topic in welke pipeline hoort. Verwerkt in deze volgorde:

1. **Stream locks** uit `cpm-routing-config`. Handmatige overrides, gaan voor
   alles.
2. **Routing-suggesties** van de advisor. Hieruit ontstaan dedicated pipelines.
3. **Bestaande state.** Een stream die al toegewezen was, blijft waar hij zit.
   Hier worden ook uitgesloten streams opgeruimd.
4. **Ontdekking uit monitoring.** Nieuwe streams krijgen als thuiscluster het
   cluster waar ze volgens monitoring fysiek staan.
5. **Catchall-toewijzing.** Alles wat niet dedicated is, gaat naar de catchall van
   zijn cluster.
6. **Promotie.** Heeft een cluster na stap 1 tot 5 nog geen dedicated pipeline,
   dan wordt zijn drukste topic gepromoveerd. Pas vanaf twee topics, anders zou
   de catchall leeg achterblijven.

Twee sets sturen dit aan. `seenClusters` bevat de clusters die al een dedicated
hebben, waardoor er nooit twee ontstaan. `managedTopics` bevat de topics die al
zijn toegewezen, waardoor stap 3 tot 6 elkaar niet overschrijven.

De acties schrijven met `refresh=true`. Dat is geen detail: zonder refresh zijn de
documenten nog niet zichtbaar voor de zoekopdracht van de pipeline-manager, die
direct erna draait, en rendert die de state van de vorige cyclus.

### cpm-pipeline-manager

Rendert en pusht de Logstash-pipelines.

Leest `cpm-pipeline-state`, `cpm-cluster-registry`, `cpm-pipeline-templates` en
`cpm-routing-config`. Groepeert de state-documenten op `pipeline_id` en bouwt per
groep één pipeline door tekstvervanging in het sjabloon.

Deze watcher kent **geen uitsluitingen**. Wat in `cpm-pipeline-state` staat, komt
in een pipeline. Werkt een uitsluiting niet, zoek dan bij de state-manager.

Twee stille `continue`s om te kennen:

```painless
Map cluster = (Map)registryMap.get(cid);
if (cluster == null) continue;                       // cluster niet actief
String ingestHosts = (String)cluster.get('ingest_hosts');
if (ingestHosts == null || ingestHosts.isEmpty()) continue;   // geen host
```

Een cluster zonder `ingest_hosts` in de registry krijgt dus geen pipeline, zonder
enige melding.

### cpm-stream-coverage

Staat buiten de keten. Vergelijkt de streams die monitoring rapporteert met wat er
in `cpm-pipeline-state` staat, en schrijft het verschil naar
`cpm-stream-coverage`. Puur rapportage.

---

## 8. Van state naar Logstash-pipeline

De pipeline-manager bouwt de configuratie met platte tekstvervanging. Er zit geen
sjabloontaal in; het zijn letterlijk `String.replace`-aanroepen.

De sjablonen staan in `cpm-pipeline-templates`, met de documenten `catchall` en
`dedicated`. Elk document bevat het veld `template` met de Logstash-configuratie
en placeholders in hoofdletters.

| placeholder | catchall | dedicated | wordt |
|---|---|---|---|
| `__TOPICS_LIST__` | ja | nee | `"a", "b", "c"` |
| `__TOPIC__` | nee | ja | één topicnaam |
| `__GROUP_ID__` | ja | ja | de Kafka consumer group |
| `__CONSUMER_THREADS__` | ja | ja | getal uit het sjabloondocument |
| `__ES_HOSTS__` | ja | ja | `"https://host:9200"` |
| `__KAFKA_BOOTSTRAP_DHL__` | ja | ja | eerste Kafka-cluster |
| `__KAFKA_BOOTSTRAP_MLD__` | ja | ja | tweede Kafka-cluster |
| `__DATASET__` | nee | ja | datasetnaam |
| `__NAMESPACE__` | nee | ja | namespace |

De gerenderde catchall ziet er zo uit, ingekort:

```ruby
input {
  kafka {
    bootstrap_servers => "kafka:9092,kafka2:9092,kafka3:9092"
    topics => ["logs-nginx-default", "logs-nginx-ns1"]
    group_id => "cpm-ingest"
    consumer_threads => 16
    codec => json
  }
  kafka { ... tweede Kafka-cluster, zelfde topics ... }
}
output {
  elasticsearch {
    hosts => ["https://es-central:9200"]
    data_stream => true
    api_key => "${VAR_API_KEY}"
  }
}
```

Twee dingen die je moet weten:

- **Er zijn twee Kafka-inputs per pipeline**, voor twee Kafka-clusters. Het aantal
  consumer-threads geldt dus per input, niet per pipeline.
- **`${VAR_API_KEY}` is geen placeholder van CPM.** Dat lost Logstash zelf op uit
  zijn omgevingsvariabelen. CPM zet daar niets in.

### De naam van een pipeline

```
<dc>_cpm-<catchall|dedicated>-<cluster_id>
```

Het `dc`-prefix is functioneel. Elke Logstash-instantie haalt alleen zijn eigen
pipelines op met `xpack.management.pipeline.id: ["dc-central_*"]`. Verander je
`dc`, dan verandert de pipelinenaam mee, blijft de oude pipeline achter in
`/_logstash/pipeline` en pakt Logstash de nieuwe niet op.

### De topic-extensie

Voor gescheiden omgevingen op één Kafka-cluster bestaat
`kafka_topic_extension`. Ansible zet die waarde in de sjabloondocumenten, en de
pipeline-manager plakt hem achter elke topicnaam:

```painless
String topic = streamType + '-' + dataset + '-' + namespace + topicExt;
```

Voor de catchall gebeurt hetzelfde per topic in de lijst. Let op de reikwijdte:
de extensie zit **alleen op de Kafka-topicnaam**, niet op de naam van de data
stream in Elasticsearch. Alles wat tegen streamnamen matcht, zoals uitsluitingen
en locks, gebruikt dus de kale naam.

Deelt een acceptatie-omgeving een Kafka-cluster met productie, dan moet ook
`cpm_kafka_group_id` verschillen. Binnen één consumer group verdeelt Kafka de
partities over álle leden, ongeacht op welke topics een lid heeft ingeschreven.
Een productie-consumer krijgt dan een acceptatie-partitie toegewezen, commit de
offset en gooit de events weg.

---

## 9. Het uitsluitingsmechanisme

Een uitsluiting is een document in `cpm-stream-exclusions` met een veld
`topic_pattern`. De matcher zit met de hand in Painless:

```painless
if (pattern.indexOf('*') < 0) {
  if (topic.equals(pattern)) { topicExcluded = true; break; }
} else {
  int star = pattern.indexOf('*');
  String pre = pattern.substring(0, star);
  String suf = pattern.substring(star + 1);
  if (pre.isEmpty()) {
    if (topic.endsWith(suf)) { topicExcluded = true; break; }
  } else if (suf.isEmpty()) {
    if (topic.startsWith(pre)) { topicExcluded = true; break; }
  } else {
    if (topic.startsWith(pre) && topic.endsWith(suf)) { topicExcluded = true; break; }
  }
}
```

Lees goed wat hier staat: er wordt alleen naar het **eerste** sterretje gekeken.
Alles daarna is letterlijke tekst. `*-webapp*` wordt dus `endsWith("-webapp*")`
en matcht nooit iets.

| patroon | wordt | raakt |
|---|---|---|
| `logs-webapp-*` | `startsWith("logs-webapp-")` | alle namespaces van die dataset |
| `*-webapp-default` | `endsWith("-webapp-default")` | ook `metrics-webapp-default` |
| `logs-*-default` | beide | alles van type logs op namespace default |
| `*-webapp*` | `endsWith("-webapp*")` | niets |

**Dit blok staat negen keer in de repo**: zeven keer in de state-manager, één keer
in de advisor en één keer in de stream-coverage. Plus nog een aparte
Python-implementatie voor het controlecommando. Wil je de matcher uitbreiden,
bijvoorbeeld naar meerdere sterretjes, dan moet dat op alle tien de plekken
identiek gebeuren. Eén afwijkende kopie geeft een uitsluiting die in de ene
watcher wel werkt en in de andere niet.

Een glob met meerdere sterretjes is technisch prima haalbaar in Painless, zonder
regex: splits het patroon op `*`, controleer het eerste deel met `startsWith`, het
laatste met `endsWith`, en scan de tussendelen op volgorde met `indexOf`. Dat is
ongeveer vijftien regels.

---

## 10. De Kibana-plugin

De plugin geeft een beheerscherm onder Stack Management → Ingest → Cluster
Pipeline Manager. De broncode staat in `kibana_plugin/cpm/`.

```
cpm/
  kibana.json         manifest, met kibanaVersion
  common/             constanten en types, gedeeld
  public/             React-frontend
    application.tsx   mountpunt
    components/       de tabbladen
    services/         HTTP-aanroepen naar de eigen server-routes
  server/             Kibana-serverkant
    routes/index.ts   de /api/cpm/* endpoints
    lib/check_access.ts
```

De frontend praat nooit rechtstreeks met Elasticsearch. Hij roept de eigen
server-routes aan, en die gebruiken de Elasticsearch-client van Kibana. Dat is
waarom de plugin de rechten van de ingelogde gebruiker respecteert.

| tabblad | index of API |
|---|---|
| Clusters | `cpm-cluster-registry`, zet `active` |
| Scoring weights | `cpm-routing-config/_global` |
| Stream locks | `cpm-routing-config`, documenten met `config_type: stream_lock` |
| Run CPM | `POST /_watcher/watch/<id>/_execute` |

De routes zijn interne Kibana-API's. Roep je ze met curl aan, dan krijg je:

```
uri [/api/cpm/clusters] with method [get] exists but is not available
with the current configuration
```

Dat is geen fout in de plugin. Voeg de headers toe die Kibana verwacht:

```bash
curl -s -u elastic:$PW \
  -H 'kbn-xsrf: true' -H 'x-elastic-internal-origin: Kibana' \
  http://localhost:5601/api/cpm/clusters
```

De plugin wordt gecompileerd tegen een exacte Kibana-versie. `kibanaVersion` in
`kibana.json` en `kibana.version` in `package.json` moeten allebei kloppen, anders
weigert Kibana de plugin te laden.

---

## 11. Een wijziging maken

### Logica in een watcher aanpassen

1. Pas het `.json.j2`-sjabloon aan in
   `ansible/roles/elastic_cpm/templates/`. Let op de `{% raw %}`-blokken.
2. Voeg je een veld toe aan een document, pas dan ook de mapping aan in
   `files/json/index_<naam>.json`. Bijna alle indices zijn `strict`.
3. Rol uit: `ansible-playbook -i inventories/<omgeving>/hosts site.yml --tags watchers`
   en bij een mappingwijziging ook `--tags indices`.
4. Test met `_execute` in simulate-modus en lees `result.transform.status`.
5. Draai de keten en vergelijk `cpm-pipeline-state` met `/_logstash/pipeline`.

### Painless in een JSON-string bewerken

Het script staat als één geëscapete regel in het sjabloon, met `\n` voor
regeleindes. Handmatig bewerken is foutgevoelig. Werk met een klein
hulpprogramma: haal de `source`-string eruit, decodeer hem, bewerk hem, codeer
hem terug. Controleer daarna of de haakjes in balans zijn en of het bestand nog
geldige JSON oplevert na het renderen van Jinja.

### Wat je nooit moet doen

- **Een `for` over een aggregatie zonder null-controle.** Zie hoofdstuk 3.
- **Een veld schrijven dat niet in de mapping staat.** De hele transform valt om.
- **Een DELETE in een `foreach` zonder te controleren of het document bestaat.**
  Een 404 telt als `failure` en zet een nepfout naast je echte fouten.
- **Aannemen dat een tag op een `include_tasks` doorwerkt naar de taken erin.**
  Dat doet hij niet. De taken moeten de tag zelf dragen.
- **De pipeline-templates aanpassen zonder na te denken over productie.** De
  bestanden `cpm-pipeline-template-*.json` gelden voor alle omgevingen.

### Testen zonder productie te raken

Er zijn twee Docker-omgevingen met elk drie Elasticsearch-clusters, een eigen
monitoring-cluster en drie Logstash-instanties. De inventories `local` en `acc`
wijzen daarnaartoe. Draai daar je wijziging, laat de keten een paar rondes lopen
en vergelijk de uitkomst met de state.

---

## 12. Waar het pijn doet

Een eerlijke lijst van dingen die je gaat tegenkomen.

**De matcher staat negen keer in de repo.** Elke uitbreiding is een
kopieeroefening. Dit is de grootste rem op het verbeteren van de
uitsluitingslogica.

**De veldsubstitutie is half aangesloten.** Een Ansible-taak zoekt op welke
veldnamen de monitoring-index gebruikt en bouwt daar een feit
`cpm_field_substitutions` van. Dat feit wordt nergens gebruikt. Alleen de
ML-datafeeds krijgen de gevonden namen mee, via losse `replace`-aanroepen. De
watchers hebben alle veldnamen hard staan. Stapt de omgeving over van Metricbeat
naar Elastic Agent, dan moeten vier watcher-sjablonen met de hand mee.

**De monitoring-index staat op twee plekken.** De watchers hebben
`monitoring:.monitoring-es-8-*` hard staan; de datafeeds en de veldprobe bouwen
hem uit een variabele. Lopen die uit de pas, dan zoeken de datafeeds op de
verkeerde plek, vinden niets, en blijven alle scores op nul zonder foutmelding.

**Een dedicated pipeline is sticky.** Eenmaal toegewezen blijft een stream
dedicated, ook als een andere stream later veel drukker wordt. Er is geen
mechanisme dat dat herziet.

**De scores liggen in de praktijk dicht bij elkaar.** Daardoor komt de advisor
zelden in actie, want die verplaatst alleen naar een lichter cluster. De
promotiestap in de state-manager is daar de compensatie voor, geen oplossing.

**Watcher-code is niet te unit-testen.** Er is geen testharnas. De snelste
feedbacklus is `_scripts/painless/_execute` voor een los stuk logica, en daarna
`_execute` met `action_modes: simulate` op de hele watcher.

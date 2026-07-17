# Handleiding — CPM Kibana plugin bouwen voor Kibana 9.4.2

Deze handleiding beschrijft hoe je de CPM-plugin bouwt tot een installeerbare
`cpm-9.4.2.zip`. Er zijn twee delen:

- **Deel A — Bouwen met het script** (`build_kibana_cpm_plugin_docker_v9.sh`). Aanbevolen.
- **Deel B — Handmatig bouwen** (voor als het script niet gebruikt kan/mag worden).

> **Belangrijk:** een Kibana-plugin moet gecompileerd worden tegen **exact** de
> Kibana-versie die in productie draait. Een zip gebouwd voor 9.4.2 werkt niet op
> 9.4.3 en andersom. Voor 8.19.16 is er een apart script (`..._v8.sh`).

Bronbestanden van de plugin staan **alleen** in `kibana_plugin/cpm/`.
De artifact komt in `kibana_plugin/build/cpm-9.4.2.zip`.

---

## Verschillen 8.19.16 → 9.4.2 (waarom een apart v9-script)

| Onderwerp | 8.19.16 | 9.4.2 |
|-----------|---------|-------|
| `kibana.json` → `kibanaVersion` | `8.19.16` | **`9.4.2`** |
| `package.json` → `kibana.version` | `8.19.16` | **`9.4.2`** |
| Node.js versie (engine check) | 22.22.2 | **24.14.1** |
| Docker base image | `node:22.22.2-bookworm-slim` | `node:24.14.1-bookworm-slim` |

Het v9-script regelt deze drie dingen automatisch (manifests + Node-versie).

---

# Deel A — Bouwen met het script

## A.1 Vereisten

| Nodig | Toelichting |
|-------|-------------|
| Docker | Docker Desktop draaiend |
| Netwerk | Alleen bij de **eerste** build (clone Kibana + bootstrap, ~15-20 min) |
| `python3`, `curl` | Alleen op de host, voor de manifest-update en de optionele versie-check |
| Schijfruimte | ~enkele GB voor de Docker-image met de Kibana-checkout |

Node.js hoef je op de host **niet** te installeren — dat gebeurt in de container.

## A.2 Uitvoeren

Vanuit de repo-root:

```bash
./scripts/build_kibana_cpm_plugin_docker_v9.sh
```

Optioneel eerst valideren dat de live Kibana echt 9.4.2 is (Step 1 uit de notes):

```bash
KIBANA_URL=https://<jouw-kibana>:5601 ./scripts/build_kibana_cpm_plugin_docker_v9.sh
```

Het script breekt af als de live versie afwijkt van de doelversie.

### Instelbare variabelen (env)

| Variabele | Default | Waarvoor |
|-----------|---------|----------|
| `KIBANA_VERSION` | `9.4.2` | Doelversie (manifests + Kibana-checkout + zip-naam) |
| `NODE_VERSION` | `24.14.1` | Node base image in de Docker-build |
| `KIBANA_URL` | *(leeg)* | Als gezet: `/api/status`-check vooraf; leeg = overslaan |

Voorbeeld — een andere 9.x-versie bouwen:

```bash
KIBANA_VERSION=9.4.5 NODE_VERSION=24.14.1 ./scripts/build_kibana_cpm_plugin_docker_v9.sh
```

## A.3 Welke stappen doorloopt het script

1. **Step 1 — Versie-check (optioneel).** Alleen als `KIBANA_URL` is gezet: haalt
   `/api/status` op en vergelijkt met `KIBANA_VERSION`. Wijkt af → stop.
2. **Step 2 — Manifests pinnen.** Schrijft `9.4.2` in `cpm/kibana.json`
   (`kibanaVersion`) en `cpm/package.json` (`kibana.version`). De plugin-eigen
   `version` (bijv. `1.0.6`) blijft ongewijzigd.
3. **Step 3/4b — Docker build met Node 24.** Maakt een wegwerp-`Dockerfile.build.v9`
   afgeleid van `Dockerfile.build`, met de Node base image omgezet naar
   `NODE_VERSION`. De gedeelde `Dockerfile.build` (die het v8-script gebruikt)
   blijft ongemoeid. In de container: Kibana v9.4.2 clonen → `yarn kbn bootstrap`
   → `build-shared` → plugin `yarn build`.
4. **Artifact kopiëren.** De container kopieert `cpm-9.4.2.zip` naar
   `kibana_plugin/build/` op de host.
5. **Opruimen.** De wegwerp-Dockerfile wordt verwijderd (een mislukte opruiming
   laat de build niet falen).

## A.4 Resultaat en installeren

Output:

```
kibana_plugin/build/cpm-9.4.2.zip
```

Installeren op Kibana:

```bash
bin/kibana-plugin install file:///.../kibana_plugin/build/cpm-9.4.2.zip
# of in Docker Compose:
docker compose exec kibana bin/kibana-plugin install file:///tmp/cpm-9.4.2.zip
```

## A.5 Foutafhandeling (Step 5)

Faalt de plugin-`yarn build` op TypeScript-/plugin-API-fouten, dan zijn dat
meestal 8.x → 9.x wijzigingen in de Kibana-API's (`management`, `features`,
types). Aanpak:

1. Lees de TypeScript-fouten in de build-output.
2. Pas `kibana_plugin/cpm/` aan (meestal `public/plugin.ts`, `server/`, imports).
3. Draai het script opnieuw met dezelfde `KIBANA_VERSION=9.4.2`.

> De CPM-plugin is nog niet volledig geverifieerd op 9.4.2 — reken op mogelijke
> codewijzigingen.

## A.6 Terug naar 8.19.16

Het v9-script wijzigt de manifests naar `9.4.2` in de repo. Wil je weer voor 8.x
bouwen, draai dan het v8-script (dat zet de manifests op `8.19.16`):

```bash
./scripts/build_kibana_cpm_plugin_docker_v8.sh
```

of zet de wijziging handmatig terug met `git checkout -- kibana_plugin/cpm/kibana.json kibana_plugin/cpm/package.json`.

---

# Deel B — Handmatig bouwen (zonder v9-script)

Gebruik dit als je de stappen los wilt uitvoeren, of als Docker/het script niet
beschikbaar is. Twee routes: **B.I native op Mac** of **B.II handmatig met Docker**.

## Stap 1 — Doelversie bevestigen

```bash
curl -sk https://<jouw-kibana>:5601/api/status \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['version']['number'])"
```

Moet `9.4.2` teruggeven (niet 9.4.3).

## Stap 2 — Plugin-manifests aanpassen

In `kibana_plugin/cpm/`:

`kibana.json`:
```json
"kibanaVersion": "9.4.2"
```

`package.json`:
```json
"kibana": {
  "version": "9.4.2"
}
```

Laat de plugin-eigen `"version"` (bijv. `1.0.6`) staan; alleen de Kibana-versie wijzigt.

## Stap 3 — Juiste Node-versie

Kibana 9.4.2 pint **Node 24.14.1** (8.19.16 gebruikt 22.22.2):

```bash
eval "$(fnm env)"
fnm install 24.14.1
fnm use 24.14.1
node -v          # moet v24.14.1 zijn
corepack enable
```

---

### Route B.I — Native bouwen op Mac

Gebruik een **verse** Kibana-checkout (hergebruik niet de 8.x `.kibana-build`):

```bash
cd /pad/naar/elastic_cpm
KIBANA_VERSION=9.4.2 \
KIBANA_DIR=$HOME/.cache/kibana-9.4.2 \
./scripts/build_kibana_cpm_plugin.sh
```

Wat dit script doet (equivalent van handmatig):
1. `git clone --depth 1 --branch v9.4.2 https://github.com/elastic/kibana.git $KIBANA_DIR`
2. `cd $KIBANA_DIR && corepack enable && yarn kbn bootstrap --skip-os-packages`
3. Plugin-bron kopiëren naar `$KIBANA_DIR/plugins/cpm`
4. `cd $KIBANA_DIR/plugins/cpm && yarn build`
5. Zip kopiëren naar `kibana_plugin/build/`

Output: `kibana_plugin/build/cpm-9.4.2.zip`

Volledig handmatig (zonder het script) komt neer op exact die 5 commando's.

---

### Route B.II — Handmatig met Docker

De gedeelde `Dockerfile.build` gebruikt `node:22.22.2`, wat de 9.4.2-engine-check
laat falen. Zet daarom eerst de Node-versie om. Twee opties:

**Optie A — tijdelijk de FROM-regel wijzigen** in `kibana_plugin/Dockerfile.build`:
```dockerfile
FROM node:24.14.1-bookworm-slim
```

**Optie B — een kopie maken en die gebruiken** (laat het origineel intact, dit is
wat het v9-script doet):
```bash
cd kibana_plugin
sed -E 's|^FROM node:[0-9.]+-bookworm-slim|FROM node:24.14.1-bookworm-slim|' \
  Dockerfile.build > Dockerfile.build.v9
```

Daarna bouwen en de zip eruit halen:

```bash
cd kibana_plugin
docker build \
  -f Dockerfile.build.v9 \
  --build-arg KIBANA_VERSION=9.4.2 \
  -t cpm-kibana-plugin-build:9.4.2 .

docker run --rm -v "$(pwd)/build:/output" cpm-kibana-plugin-build:9.4.2

# opruimen
rm -f Dockerfile.build.v9
```

Output: `kibana_plugin/build/cpm-9.4.2.zip`

## Stap 5 — 9.x compile-fouten oplossen (indien nodig)

8.x → 9.x breekt vaak plugin-API's (`management`, `features`, types). Als
`yarn build` faalt:

1. Lees de TypeScript-fouten in de build-output.
2. Pas `kibana_plugin/cpm/` aan (meestal `public/plugin.ts`, `server/`, imports).
3. Bouw opnieuw met dezelfde `KIBANA_VERSION=9.4.2`.

---

## Snelle referentie

| Doel | Commando |
|------|----------|
| Bouwen (aanbevolen) | `./scripts/build_kibana_cpm_plugin_docker_v9.sh` |
| Bouwen + versie-check | `KIBANA_URL=https://host:5601 ./scripts/build_kibana_cpm_plugin_docker_v9.sh` |
| Native bouwen op Mac | `KIBANA_VERSION=9.4.2 KIBANA_DIR=$HOME/.cache/kibana-9.4.2 ./scripts/build_kibana_cpm_plugin.sh` |
| Terug naar 8.x | `./scripts/build_kibana_cpm_plugin_docker_v8.sh` |
| Installeren | `bin/kibana-plugin install file://.../build/cpm-9.4.2.zip` |
| Artifact | `kibana_plugin/build/cpm-9.4.2.zip` |

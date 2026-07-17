#!/usr/bin/env python3
"""
Verdeelt datastream-rijen in N groepen met een zo gelijk mogelijke som (greedy least-loaded),
met een minimum aantal items per groep.

Gebruik:
    python groepen.py input.csv
    python groepen.py input.csv --groepen 8
    python groepen.py input.csv --groepen 8 --min-items 2 --output resultaat.csv

CSV-formaat (header verplicht, ; of , als scheidingsteken):
    De laatste kolom is de waarde; alle kolommen ervoor vormen samen de sleutel.

    Voorbeeld (4 kolommen):
        "data_stream.type";"data_stream.dataset";"data_stream.namespace";"Count of records"
        metrics;"elastic_agent.filebeat_input";default;50316177
        metrics;"elastic_agent.filebeat_input";tuning;1194559

    Voorbeeld (2 kolommen):
        event.dataset,aantal
        apache.access,123456
"""

import argparse
import csv
import heapq
import sys
from pathlib import Path
from typing import List, Tuple

MIN_ITEMS_PER_GROEP = 2


def parse_getal(s):  # type: (str) -> str
    """
    Zet een getal-string om naar een float-parseerbare string.
    Ondersteunt:
      - Punt als duizendtallen-separator:  2.318.582      -> 2318582
      - Komma als decimaalteken:           2.318.582,677  -> 2318582.677
      - Punt als decimaalteken:            2318582.677    -> 2318582.677
      - Spaties als duizendtallen:         2 318 582      -> 2318582
    """
    s = s.strip().replace(" ", "").replace("\u202f", "")
    n_punten = s.count(".")
    n_kommas = s.count(",")
    if n_kommas == 1 and n_punten >= 1:
        # Europese notatie: 2.318.582,677 of 1.234,56
        s = s.replace(".", "").replace(",", ".")
    elif n_kommas == 1 and n_punten == 0:
        # Alleen komma: decimaal (1234,56) of duizendtallen (1,234)
        na_komma = s.split(",")[1]
        s = s.replace(",", "") if len(na_komma) == 3 else s.replace(",", ".")
    elif n_punten == 1:
        # Één punt: duizendtallen (776.763) als er precies 3 cijfers na de punt staan
        na_punt = s.split(".")[1]
        if len(na_punt) == 3:
            s = s.replace(".", "")
        # Anders: gewone decimale punt (1234567.89) — ongewijzigd
    elif n_punten >= 2:
        # Meerdere punten: duizendtallen (2.318.582)
        s = s.replace(".", "")
    return s


def lees_csv(pad):  # type: (str) -> List[Tuple[str, float]]
    """Leest CSV; laatste kolom = waarde, overige kolommen samengevoegd als sleutel."""
    rijen = []
    with open(pad, newline="", encoding="utf-8") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        except csv.Error:
            dialect = csv.excel  # fallback: komma
        reader = csv.DictReader(f, dialect=dialect)

        velden = reader.fieldnames or []
        if len(velden) < 2:
            sys.exit(f"Fout: CSV heeft minder dan 2 kolommen (gevonden: {velden})")

        naam_kolommen = velden[:-1]   # alles behalve de laatste
        getal_kolom = velden[-1]      # bijv. "Count of records" of "aantal"

        for i, rij in enumerate(reader, start=2):
            naam = "-".join((rij[k] or "").strip() for k in naam_kolommen)
            try:
                waarde = float(parse_getal(rij[getal_kolom]))
            except (ValueError, TypeError):
                print(f"  Waarschuwing: rij {i} overgeslagen (ongeldige waarde: '{rij[getal_kolom]}')")
                continue
            if waarde < 0:
                print(f"  Waarschuwing: rij {i} heeft negatieve waarde ({waarde}), overgeslagen")
                continue
            rijen.append((naam, waarde))

    if not rijen:
        sys.exit("Fout: geen geldige rijen gevonden in CSV.")

    return rijen


def verdeel_greedy(rijen, n_groepen):  # type: (List[Tuple[str, float]], int) -> List[List[Tuple[str, float]]]
    """Greedy least-loaded verdeling: elk getal gaat naar de groep met de laagste som."""
    gesorteerd = sorted(rijen, key=lambda x: x[1], reverse=True)

    # min-heap: (huidige_som, groep_index)
    heap = [(0.0, i) for i in range(n_groepen)]
    heapq.heapify(heap)
    groepen = [[] for _ in range(n_groepen)]  # type: List[List[Tuple[str, float]]]

    for naam, waarde in gesorteerd:
        som, idx = heapq.heappop(heap)
        groepen[idx].append((naam, waarde))
        heapq.heappush(heap, (som + waarde, idx))

    return groepen


def repareer_minimum(groepen, minimum=MIN_ITEMS_PER_GROEP):
    # type: (List[List[Tuple[str, float]]], int) -> List[List[Tuple[str, float]]]
    """Zorgt dat elke groep minstens `minimum` items heeft door de kleinste
    items te verplaatsen vanuit groepen die er meer dan `minimum` hebben."""
    while True:
        tekort = [i for i, g in enumerate(groepen) if len(g) < minimum]
        if not tekort:
            return groepen

        # Kandidaat-donoren: groepen die na donatie nog >= minimum items houden
        donoren = [i for i, g in enumerate(groepen) if len(g) > minimum]
        if not donoren:
            sys.exit("Fout: kan minimum niet garanderen (te weinig items).")

        # Kies per tekortgroep het kleinste item uit alle donoren,
        # zodat de som van de (al zware) tekortgroep minimaal toeneemt.
        doel = tekort[0]
        beste = min(
            ((d, j) for d in donoren for j, _ in enumerate(groepen[d])),
            key=lambda dj: groepen[dj[0]][dj[1]][1],
        )
        d, j = beste
        item = groepen[d].pop(j)
        groepen[doel].append(item)
        print(f"  Reparatie: '{item[0]}' ({item[1]:,.0f}) "
              f"van groep {d+1} naar groep {doel+1} (minimum {minimum} items)")


def druk_resultaat(groepen):  # type: (List[List[Tuple[str, float]]]) -> None
    """Print een overzicht naar stdout."""
    totaal = sum(waarde for groep in groepen for _, waarde in groep)
    doel = totaal / len(groepen)

    print(f"\n{'='*60}")
    print(f"  Totaal: {totaal:>15,.0f}")
    print(f"  Doel per groep: {doel:>10,.0f}")
    print(f"  Aantal groepen: {len(groepen)}")
    print(f"{'='*60}\n")

    for i, groep in enumerate(groepen, 1):
        som = sum(w for _, w in groep)
        afwijking = som - doel
        teken = "+" if afwijking >= 0 else ""
        print(f"  Groep {i:>2}  |  som: {som:>12,.0f}  |  "
              f"afwijking: {teken}{afwijking:>10,.0f}  |  "
              f"items: {len(groep)}")
        for naam, waarde in sorted(groep, key=lambda x: x[1], reverse=True):
            print(f"           {'':3}  {naam:<45}  {waarde:>12,.0f}")

    print()


def schrijf_csv(groepen, uitvoer_pad):  # type: (List[List[Tuple[str, float]]], str) -> None
    """Schrijft resultaat naar CSV met kolommen: groep, sleutel, aantal, groep_som."""
    groep_sommen = dict((i + 1, sum(w for _, w in g)) for i, g in enumerate(groepen))

    with open(uitvoer_pad, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["groep", "sleutel", "aantal", "groep_som"])
        for i, groep in enumerate(groepen, 1):
            for naam, waarde in groep:
                writer.writerow([i, naam,
                                 int(waarde) if waarde == int(waarde) else waarde,
                                 int(groep_sommen[i]) if groep_sommen[i] == int(groep_sommen[i]) else groep_sommen[i]])

    print(f"  Resultaat opgeslagen in: {uitvoer_pad}")


def main():
    parser = argparse.ArgumentParser(
        description="Verdeelt datastream-rijen in N groepen met gelijke som."
    )
    parser.add_argument("invoer", help="Pad naar invoer-CSV")
    parser.add_argument("--groepen", type=int, default=8, metavar="N",
                        help="Aantal groepen (standaard: 8)")
    parser.add_argument("--min-items", type=int, default=MIN_ITEMS_PER_GROEP, metavar="M",
                        help=f"Minimum aantal items per groep (standaard: {MIN_ITEMS_PER_GROEP})")
    parser.add_argument("--output", metavar="BESTAND",
                        help="Pad voor uitvoer-CSV (optioneel)")
    args = parser.parse_args()

    invoer_pad = Path(args.invoer)
    if not invoer_pad.exists():
        sys.exit(f"Fout: bestand '{invoer_pad}' niet gevonden.")
    if args.groepen < 2:
        sys.exit("Fout: minimaal 2 groepen vereist.")
    if args.min_items < 1:
        sys.exit("Fout: --min-items moet minimaal 1 zijn.")

    print(f"\n  Inlezen: {invoer_pad}")
    rijen = lees_csv(str(invoer_pad))
    print(f"  {len(rijen)} rijen ingelezen.")

    if len(rijen) < args.groepen * args.min_items:
        sys.exit(f"Fout: {len(rijen)} rijen is te weinig voor {args.groepen} groepen "
                 f"met minimaal {args.min_items} items per groep.")

    groepen = verdeel_greedy(rijen, args.groepen)
    groepen = repareer_minimum(groepen, args.min_items)
    druk_resultaat(groepen)

    uitvoer = args.output or invoer_pad.stem + "_groepen.csv"
    schrijf_csv(groepen, uitvoer)


if __name__ == "__main__":
    main()

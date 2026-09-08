#!/usr/bin/env python3
"""Convert a Markdown document to two Confluence-pasteable formats.

Confluence accepts content in two practical ways, and which one works depends
on the version:

  html  Open the file in a browser, select all, paste in the editor. Confluence
        converts headings, tables, lists and code blocks to native elements.
        Works on Cloud and on Server/DC and is the most faithful route.
  wiki  Paste in the "Insert markup" dialog and pick Confluence wiki. Only
        available where that dialog still exists.

Handles the subset of Markdown this repository uses: ATX headings, fenced code
blocks, GitHub tables, bullet and numbered lists, blockquotes, horizontal rules,
bold, and inline code.

Only stdlib is used.

Examples:
  ./md_to_confluence.py docs/ONTWIKKELAARSHANDLEIDING.md --out-dir docs/export
  ./md_to_confluence.py docs/OVERDRACHT-DEVOPS.md --format html
"""
from __future__ import annotations

import argparse
import html
import pathlib
import re
import sys


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("source", help="het Markdown-bestand")
    p.add_argument("--out-dir", default="docs/export")
    p.add_argument("--format", choices=["html", "wiki", "both"], default="both")
    return p.parse_args()


# ---------------------------------------------------------------- blokken

def blocks(lines):
    """Splits de tekst in blokken: (soort, regels)."""
    out, i, n = [], 0, len(lines)
    while i < n:
        line = lines[i]
        if line.startswith("```"):
            lang = line[3:].strip()
            body, i = [], i + 1
            while i < n and not lines[i].startswith("```"):
                body.append(lines[i]); i += 1
            i += 1
            out.append(("code", lang, body)); continue
        if re.match(r"^\|.*\|\s*$", line) and i + 1 < n \
                and re.match(r"^\|[\s:\-|]+\|\s*$", lines[i + 1]):
            body = []
            while i < n and line.startswith("|"):
                body.append(lines[i]); i += 1
                line = lines[i] if i < n else ""
            out.append(("table", "", body)); continue
        if re.match(r"^#{1,6} ", line):
            out.append(("head", "", [line])); i += 1; continue
        if re.match(r"^\s*[-*] ", line):
            body = []
            while i < n and (re.match(r"^\s*[-*] ", lines[i]) or
                             (lines[i].startswith("  ") and lines[i].strip())):
                body.append(lines[i]); i += 1
            out.append(("ul", "", body)); continue
        if re.match(r"^\s*\d+\. ", line):
            body = []
            while i < n and (re.match(r"^\s*\d+\. ", lines[i]) or
                             (lines[i].startswith("   ") and lines[i].strip())):
                body.append(lines[i]); i += 1
            out.append(("ol", "", body)); continue
        if line.startswith("> "):
            body = []
            while i < n and lines[i].startswith(">"):
                body.append(lines[i].lstrip(">").strip()); i += 1
            out.append(("quote", "", body)); continue
        if re.match(r"^-{3,}\s*$", line):
            out.append(("rule", "", [])); i += 1; continue
        if not line.strip():
            i += 1; continue
        body = []
        while i < n and lines[i].strip() and not lines[i].startswith("```") \
                and not re.match(r"^#{1,6} |^\s*[-*] |^\s*\d+\. |^\||^>|^-{3,}\s*$", lines[i]):
            body.append(lines[i]); i += 1
        out.append(("p", "", body))
    return out


def split_row(row):
    return [c.strip() for c in row.strip().strip("|").split("|")]


# ---------------------------------------------------------------- html

def _protect(t):
    """Haal inline code eruit zodat vetopmaak er dwars doorheen kan lopen."""
    spans = []
    def keep(m):
        spans.append(m.group(1))
        return "\x00%d\x00" % (len(spans) - 1)
    return re.sub(r"`([^`]+)`", keep, t), spans


def inline_html(t):
    t, spans = _protect(t)
    t = html.escape(t)
    t = re.sub(r"\*\*([^*]+?)\*\*", r"<strong>\1</strong>", t)
    for i, s in enumerate(spans):
        t = t.replace("\x00%d\x00" % i, "<code>%s</code>" % html.escape(s))
    return t


def to_html(bl, titel):
    o = ['<meta charset="utf-8">',
         "<title>%s</title>" % html.escape(titel),
         "<style>body{font-family:-apple-system,Segoe UI,Arial,sans-serif;max-width:900px;"
         "margin:2rem auto;padding:0 1rem;line-height:1.55}"
         "table{border-collapse:collapse}th,td{border:1px solid #ccc;padding:6px 10px;"
         "text-align:left;vertical-align:top}th{background:#f4f5f7}"
         "pre{background:#f4f5f7;border:1px solid #ddd;padding:10px;overflow-x:auto}"
         "code{font-family:Menlo,Consolas,monospace}</style>"]
    for kind, lang, body in bl:
        if kind == "head":
            lvl = len(re.match(r"^#+", body[0]).group(0))
            o.append("<h%d>%s</h%d>" % (lvl, inline_html(body[0][lvl + 1:]), lvl))
        elif kind == "p":
            o.append("<p>%s</p>" % inline_html(" ".join(x.strip() for x in body)))
        elif kind == "code":
            o.append("<pre><code>%s</code></pre>" % html.escape("\n".join(body)))
        elif kind == "quote":
            o.append("<blockquote><p>%s</p></blockquote>"
                     % inline_html(" ".join(body)))
        elif kind == "rule":
            o.append("<hr>")
        elif kind in ("ul", "ol"):
            tag = "ul" if kind == "ul" else "ol"
            items, cur = [], None
            for raw in body:
                m = re.match(r"^\s*(?:[-*]|\d+\.) (.*)$", raw)
                if m:
                    if cur is not None:
                        items.append(cur)
                    cur = m.group(1)
                elif cur is not None:
                    cur += " " + raw.strip()
            if cur is not None:
                items.append(cur)
            o.append("<%s>" % tag)
            o += ["<li>%s</li>" % inline_html(x) for x in items]
            o.append("</%s>" % tag)
        elif kind == "table":
            rows = [r for r in body if not re.match(r"^\|[\s:\-|]+\|\s*$", r)]
            o.append("<table><thead><tr>")
            o += ["<th>%s</th>" % inline_html(c) for c in split_row(rows[0])]
            o.append("</tr></thead><tbody>")
            for r in rows[1:]:
                o.append("<tr>" + "".join("<td>%s</td>" % inline_html(c)
                                          for c in split_row(r)) + "</tr>")
            o.append("</tbody></table>")
    return "\n".join(o) + "\n"


# ---------------------------------------------------------------- wiki

def esc_wiki(t, in_code=False):
    """Tekens die Confluence als opmaak leest onschadelijk maken.

    Binnen {{monospace}} moet er meer weg: Confluence verwerkt daar nog steeds
    opmaaktekens, dus een * of _ in een codefragment zou vet of cursief starten.
    """
    # Geen koppelteken: strikethrough vraagt om -tekst- met witruimte eromheen,
    # en elke identifier ontsnappen maakt de uitvoer onleesbaar.
    chars = "{}[]*_+^~|" if in_code else "{}[]"
    for ch in chars:
        t = t.replace(ch, "\\" + ch)
    return t


def inline_wiki(t):
    t, spans = _protect(t)
    t = esc_wiki(t)
    t = re.sub(r"\*\*([^*]+?)\*\*", r"*\1*", t)
    for i, s in enumerate(spans):
        t = t.replace("\x00%d\x00" % i, "{{%s}}" % esc_wiki(s, in_code=True))
    return t


def to_wiki(bl):
    o = []
    for kind, lang, body in bl:
        if kind == "head":
            lvl = len(re.match(r"^#+", body[0]).group(0))
            o.append("h%d. %s" % (lvl, inline_wiki(body[0][lvl + 1:]))); o.append("")
        elif kind == "p":
            o.append(inline_wiki(" ".join(x.strip() for x in body))); o.append("")
        elif kind == "code":
            known = {"bash": "bash", "json": "javascript", "yaml": "yaml",
                     "painless": "java", "ruby": "ruby", "jinja": "none",
                     "jinja2": "none", "": "none"}
            o.append("{code:language=%s}" % known.get(lang, "none"))
            o += body
            o.append("{code}"); o.append("")
        elif kind == "quote":
            o.append("{quote}"); o.append(inline_wiki(" ".join(body)))
            o.append("{quote}"); o.append("")
        elif kind == "rule":
            o.append("----"); o.append("")
        elif kind in ("ul", "ol"):
            bullet = "*" if kind == "ul" else "#"
            cur = None
            for raw in body:
                m = re.match(r"^\s*(?:[-*]|\d+\.) (.*)$", raw)
                if m:
                    if cur is not None:
                        o.append("%s %s" % (bullet, inline_wiki(cur)))
                    cur = m.group(1)
                elif cur is not None:
                    cur += " " + raw.strip()
            if cur is not None:
                o.append("%s %s" % (bullet, inline_wiki(cur)))
            o.append("")
        elif kind == "table":
            rows = [r for r in body if not re.match(r"^\|[\s:\-|]+\|\s*$", r)]
            o.append("||" + "||".join(inline_wiki(c) for c in split_row(rows[0])) + "||")
            for r in rows[1:]:
                o.append("|" + "|".join(inline_wiki(c) or " " for c in split_row(r)) + "|")
            o.append("")
    return "\n".join(o) + "\n"


def main() -> int:
    args = parse_args()
    src = pathlib.Path(args.source)
    if not src.exists():
        print("Niet gevonden: %s" % src, file=sys.stderr)
        return 2
    lines = src.read_text().splitlines()
    bl = blocks(lines)
    titel = next((l[2:] for l in lines if l.startswith("# ")), src.stem)

    outdir = pathlib.Path(args.out_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    made = []
    if args.format in ("html", "both"):
        p = outdir / (src.stem + ".confluence.html")
        p.write_text(to_html(bl, titel)); made.append(p)
    if args.format in ("wiki", "both"):
        p = outdir / (src.stem + ".confluence.txt")
        p.write_text(to_wiki(bl)); made.append(p)

    print("blokken: %d" % len(bl))
    for p in made:
        print("  %s  (%d regels)" % (p, len(p.read_text().splitlines())))
    return 0


if __name__ == "__main__":
    sys.exit(main())

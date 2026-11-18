"""Leak scan: prove the public copy carries nothing from the private original.

    python scripts/leak_scan.py [--terms-file PATH] [--extra-dir DIR ...] [--duckdb PATH] [--report FILE]

Without --terms-file only the generic patterns run (that is what CI does: the
term list is private and never leaves the owner's machine).

What it scans
  * every file git would publish (tracked + untracked-not-ignored), text and binary;
  * the git history's author / committer names, e-mails and messages;
  * --extra-dir: generated output to check too (data/, the docs site, FIXML samples);
  * --duckdb: every VARCHAR column of every table / view in the built database,
    matched exactly against the id-like terms (so no real id can hide in the data).

What it looks for
  * generic patterns built in below (personal Windows paths, drive-letter paths,
    e-mail addresses, cloud bucket URIs, key-like strings);
  * the terms in --terms-file. That file is kept OUTSIDE this repo, because the
    terms themselves are what must not be published: the original exchange's
    name, its real ticker prefixes, the private bucket and user names, the
    pricing model's column names, real figures, and a sample of real ids (fill,
    quote, order and RFQ ids, requester hashes, market and combo tickers)
    pulled from the private database. Format: `[category]` headers, one term
    per line, `re:` prefix for a regular expression, `#` comments.

The report never prints a term, only its category, a term number and where it
was found. Exit code 1 if anything is found.
"""
import argparse
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

GENERIC = {
    "windows_user_path": re.compile(r"(?i)\b[a-z]:[\\/]+users[\\/]"),
    "drive_letter_path": re.compile(r"(?i)(?<![a-z0-9])[d-z]:[\\/]"),
    "unix_home_path": re.compile(r"(?<![\w.])/(home|Users)/[a-z][\w.-]+/"),
    "email_address": re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    "bucket_uri": re.compile(r"(?i)\b(s3|gs|az)://"),
    "aws_key_id": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
}
# e-mail addresses that are expected: commit attribution and placeholders
EMAIL_ALLOW = re.compile(r"(?i)^(noreply@anthropic\.com|[\w.-]+@users\.noreply\.github\.com|[\w.-]+@example\.(com|org))$")
# generated HTML (dbt docs / Elementary) bundles third-party JavaScript with
# its own e-mails and URLs; generic patterns are not applied to these
GENERIC_SKIP_SUFFIX = (".html",)
TEXT_LIMIT = 50_000_000
TOKEN = re.compile(rb"[A-Za-z0-9][A-Za-z0-9_\-]*")


def load_terms(path):
    cats = defaultdict(list)
    cat = "uncategorized"
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            m = re.fullmatch(r"\[(.+)\]", line)
            if m:
                cat = m.group(1)
                continue
            cats[cat].append(line)
    literal_tokens, substrings, regexes = {}, [], []
    for cat, terms in cats.items():
        for i, t in enumerate(terms, 1):
            label = (cat, i)
            if t.startswith("re:"):
                regexes.append((label, re.compile(t[3:].encode())))
            elif len(t) >= 16 and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_\-]*", t):
                literal_tokens[t.encode()] = label          # id-like: matched as a whole token
            else:
                substrings.append((label, t.lower().encode()))
    return cats, literal_tokens, substrings, regexes


def publishable_files():
    out = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
                         cwd=ROOT, capture_output=True, check=True).stdout
    return [ROOT / p for p in out.decode().split("\0") if p]


def scan_bytes(data, where, terms, hits, generic=True):
    _, literal_tokens, substrings, regexes = terms
    low = data.lower()
    for label, s in substrings:
        if s in low:
            hits[label].append(where)
    for label, rx in regexes:
        if rx.search(data):
            hits[label].append(where)
    if literal_tokens:
        for tok in set(TOKEN.findall(data)):
            label = literal_tokens.get(tok)
            if label:
                hits[label].append(where)
    if generic:
        text = data.decode("utf-8", errors="ignore")
        for name, rx in GENERIC.items():
            for m in rx.finditer(text):
                if name == "email_address" and EMAIL_ALLOW.match(m.group(0)):
                    continue
                hits[("generic:" + name, 0)].append(f"{where}: {m.group(0)[:60]}")
                break


def scan_duckdb(path, terms, hits):
    import duckdb
    _, literal_tokens, _, _ = terms
    con = duckdb.connect(path, read_only=True)
    con.execute("create temp table leak_terms (term varchar)")
    con.executemany("insert into leak_terms values (?)", [[t.decode()] for t in literal_tokens])
    cols = con.execute("""
        select table_schema, table_name, column_name from information_schema.columns
        where data_type = 'VARCHAR' and table_schema not in ('information_schema', 'pg_catalog')
          and table_catalog = current_database()
    """).fetchall()
    n_cols = 0
    for schema, table, col in cols:
        try:
            found = con.execute(f'select distinct t.term from leak_terms t join "{schema}"."{table}" x '
                                f'on x."{col}" = t.term').fetchall()
        except duckdb.Error:
            continue                                        # e.g. a view over files that moved
        n_cols += 1
        for (term,) in found:
            hits[literal_tokens[term.encode()]].append(f"duckdb {schema}.{table}.{col}")
    return n_cols


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--terms-file")
    ap.add_argument("--extra-dir", action="append", default=[])
    ap.add_argument("--duckdb")
    ap.add_argument("--report")
    a = ap.parse_args()
    terms = load_terms(a.terms_file) if a.terms_file else ({}, {}, [], [])
    cats = terms[0]
    hits = defaultdict(list)

    files = publishable_files()
    for f in files:
        if a.terms_file and f.resolve() == Path(a.terms_file).resolve():
            sys.exit("the terms file is inside the repo and would be published: move it out")
        data = f.read_bytes()[:TEXT_LIMIT]
        scan_bytes(data, str(f.relative_to(ROOT)), terms, hits)
    git_meta = subprocess.run(["git", "log", "--all", "--format=%an%n%ae%n%cn%n%ce%n%B"],
                              cwd=ROOT, capture_output=True, check=True).stdout
    scan_bytes(git_meta, "git history (authors, committers, messages)", terms, hits)

    n_extra = 0
    pq = None
    for d in a.extra_dir:
        for p in sorted(Path(d).rglob("*")):
            if not p.is_file():
                continue
            n_extra += 1
            if p.suffix == ".parquet":
                # Parquet is compressed: scan its decoded values, not its bytes
                if pq is None:
                    import duckdb
                    pq = duckdb.connect()
                rows = pq.execute("select * from read_parquet(?)", [str(p)]).fetchall()
                data = "\n".join("\t".join("" if v is None else str(v) for v in r) for r in rows).encode()
                scan_bytes(data, str(p), terms, hits)
            else:
                scan_bytes(p.read_bytes()[:TEXT_LIMIT], str(p), terms, hits,
                           generic=not p.name.endswith(GENERIC_SKIP_SUFFIX))
    n_cols = scan_duckdb(a.duckdb, terms, hits) if a.duckdb else 0

    lines = ["# Leak scan result", "",
             f"- publishable files scanned: {len(files)} (+ git history metadata)",
             f"- generated files scanned: {n_extra} ({', '.join(a.extra_dir) or 'none'})",
             f"- DuckDB VARCHAR columns checked: {n_cols}",
             f"- terms: {sum(len(v) for v in cats.values())} in {len(cats)} categories, plus "
             f"{len(GENERIC)} generic patterns", "",
             "| category | terms | hits |", "|---|---:|---:|"]
    for cat, ts in cats.items():
        n = sum(len(v) for (c, _), v in hits.items() if c == cat)
        lines.append(f"| {cat} | {len(ts)} | {n} |")
    for name in GENERIC:
        n = len(hits.get(("generic:" + name, 0), []))
        lines.append(f"| generic: {name} | 1 | {n} |")
    total = sum(len(v) for v in hits.values())
    lines += ["", f"**Total hits: {total}**"]
    if total:
        lines += ["", "Hits (terms redacted):"]
        for (cat, i), where in sorted(hits.items()):
            for w in where[:20]:
                lines.append(f"- {cat} #{i}: {w}")
    report = "\n".join(lines) + "\n"
    print(report)
    if a.report:
        Path(a.report).write_text(report, encoding="utf-8")
    sys.exit(1 if total else 0)


if __name__ == "__main__":
    main()

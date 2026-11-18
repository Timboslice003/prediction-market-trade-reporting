"""Download the FIXML schema, verify it, and build the patched validation copy.

    python scripts/fetch_fixml_schema.py [--out work/fixml_schema] [--zip PATH]

The schema is the FIX Trading Community's "FIXML Schema - Latest EP" package
(EP309). It is not redistributed in this repo; this script fetches it from
fixtrading.org (or takes a local copy of the same zip with --zip), checks its
sha256, and unzips it to <out>/latest_ep309/.

EP309 does not compile as published: NoTradingServices_t in
fixml-fields-base-Latest.xsd derives from a type `NumInGroup` that the package
never defines. The validation copy <out>/latest_ep309_patched/ adds that one
type (a non-negative integer, FIX's NumInGroup datatype) to
fixml-datatypes-Latest.xsd. Nothing else changes, and no field this project
writes touches that type. scripts/check_fixml.py validates against
<out>/latest_ep309_patched/fixml-main-Latest.xsd.
"""
import argparse
import hashlib
import io
import os
import shutil
import sys
import urllib.request
import zipfile

URL = "https://fixtrading.org/packages/fixml-schema-latest-ep/?wpdmdl=2556"
SHA256 = "2c1f08003bcfe40699190b1799e646ace7dc2b0f10d1518f2e12d972c79931a3"
PATCH = """   <xs:simpleType name="NumInGroup">
      <xs:annotation>
         <xs:documentation>PATCH (local validation copy only): FIX datatype NumInGroup (int, number of repeating-group entries) is referenced by NoTradingServices_t in fixml-fields-base-Latest.xsd but not defined in EP309.</xs:documentation>
      </xs:annotation>
      <xs:restriction base="xs:nonNegativeInteger"/>
   </xs:simpleType>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="work/fixml_schema")
    ap.add_argument("--zip", help="use a local copy of the package zip instead of downloading it")
    a = ap.parse_args()

    if a.zip:
        with open(a.zip, "rb") as fh:
            data = fh.read()
    else:
        req = urllib.request.Request(URL, headers={"User-Agent": "fixml-schema-fetch/1.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
    digest = hashlib.sha256(data).hexdigest()
    if digest != SHA256:
        sys.exit(f"sha256 mismatch: got {digest}, expected {SHA256}. The package changed; review it before use.")
    print(f"package ok: {len(data):,} bytes, sha256 {digest}")

    plain = os.path.join(a.out, "latest_ep309")
    patched = os.path.join(a.out, "latest_ep309_patched")
    for d in (plain, patched):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d)
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for name in z.namelist():
            if name.startswith("__MACOSX") or not name.endswith(".xsd"):
                continue
            with open(os.path.join(plain, os.path.basename(name)), "wb") as fh:
                fh.write(z.read(name))
    for f in os.listdir(plain):
        shutil.copy(os.path.join(plain, f), os.path.join(patched, f))
    target = os.path.join(patched, "fixml-datatypes-Latest.xsd")
    with open(target, encoding="utf-8") as fh:
        text = fh.read()
    if 'name="NumInGroup"' in text:
        sys.exit("NumInGroup is already defined: the package no longer needs the patch; review it.")
    i = text.index("<xs:simpleType")
    line_start = text.rfind("\n", 0, i) + 1
    text = text[:line_start] + PATCH + text[line_start:]
    with open(target, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)
    print(f"{len(os.listdir(plain))} schema files -> {plain}; patched copy -> {patched}")


if __name__ == "__main__":
    main()

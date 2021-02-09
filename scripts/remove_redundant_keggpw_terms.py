import gzip
import os
import csv
import sys
import argparse

def find_header(input_eggnog):
    eggnog_f = gzip.open(input_eggnog, "rt")
    header = None
    for i, line in enumerate(eggnog_f):
        if not line.startswith("#"):
            break
        header = line
    if header is None or (not header.startswith("# query_name") and not header.startswith("#query_name")):
        raise ValueError("No valid header line found in eggnog-mapper output.\nheader is {}".format(header))
    header = header.strip().strip("#").strip().split("\t")
    print("header:", header, flush=True)
    return header, i



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("in_eggnog", type=str)
    ap.add_argument("--outfile", "-o", type=str, default="eb_nonredundant_keggpw.gz")
    args = ap.parse_args()

    header, skiplines = find_header(args.in_eggnog)

    #with gzip.open(os.path.basename(args.in_eggnog).replace(".gz", ".non_redundant_keggpw.gz"), "wt") as eggnog_out, gzip.open(args.in_eggnog, "rt") as eggnog_in:
    with gzip.open(args.outfile, "wt") as eggnog_out, gzip.open(args.in_eggnog, "rt") as eggnog_in:
        for i in range(skiplines):
            print(next(eggnog_in), end="", flush=True, file=eggnog_out)
        
        for i, row in enumerate(csv.DictReader(eggnog_in, fieldnames=header, delimiter="\t")):
            if row and row["KEGG_Pathway"]:
                keggpw = row["KEGG_Pathway"]
                if keggpw:
                    keggpw = keggpw.strip().split(",")
                    kmap = set(item.strip("map") for item in keggpw if item.startswith("map"))
                    kko = set(item.strip("ko") for item in keggpw if item.startswith("ko"))

                    if kmap != kko:
                        raise ValueError("Inconsistent kmap/kko values in row {i}\n{row}".format(i=i, row=row))
                    
                    terms = list("ko" + item for item in sorted(kmap))
                    row["KEGG_Pathway"] = ",".join(terms)

            print(*row.values(), sep="\t", file=eggnog_out, flush=i%1000 == 0)


if __name__ == "__main__":
    main()

import sys
import os
import csv
import argparse
import sqlite3

def query_db(cursor, gid):
	for row in cursor.execute("SELECT sequence_id, 'refseq', 'eggnog', start, end, '.', strand, '.', external_id FROM gene WHERE external_id = '{}' LIMIT 1".format(gid)):
		return row

def generate_col9(annotation, columns):
	fields = list()
	for col in columns:
		values = annotation.get(col)
		if values:
			fields.append("{col}={values}".format(col=col.replace(" ", "_"), values=values))

	return ";".join(fields)

def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("input_db", type=str)
	ap.add_argument("input_eggnog", type=str)
	args = ap.parse_args()

	eggnog_f = open(args.input_eggnog)
	out_f = open(os.path.basename(args.input_eggnog).replace(".emapper.annotations", "") + ".gff", "w")

	conn = sqlite3.connect(args.input_db)
	cursor = conn.cursor()

	extract_columns = {6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 20}

	with eggnog_f as eggnog_in, out_f as gff_out:
		header = [next(eggnog_in) for i in range(4)][-1].strip("#").strip().split("\t")
		extract_columns = [col for i, col in enumerate(header) if i in extract_columns]

		print("##gff-version 3", file=gff_out, flush=True)

		for i, row in enumerate(csv.DictReader(eggnog_in, fieldnames=header, delimiter="\t")):
			gene_data = query_db(cursor, row["query_name"])
			col9 = "ID={gid};{col9}".format(gid=row["query_name"], col9=generate_col9(row, extract_columns))
			print(*gene_data[:-1], col9, sep="\t", file=gff_out, flush=i%1000==0)
		

if __name__ == "__main__":
	main()

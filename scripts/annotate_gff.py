import sys
import os
import csv
import argparse
import sqlite3
import gzip

BASE_QUERY = "SELECT sequence_id, 'refseq', 'eggnog', start, end, '.', strand, '.', external_id FROM gene WHERE {};"


def query_db(cursor, gid):
	for row in cursor.execute(BASE_QUERY.format("external_id = '{}' LIMIT 1").format(gid)):
		return row

def batch_query_db(cursor, gids):
	condition = " or ".join("external_id = '{}'".format(gid.replace("'", "''")) for gid in gids)
	for row in cursor.execute(BASE_QUERY.format(condition)):
		yield row

def generate_col9(annotation, columns):
	fields = list()
	for col in columns:
		values = annotation.get(col)
		if values:
			fields.append("{col}={values}".format(col=col.replace(" ", "_"), values=values))
	return ";".join(fields)

def process_query_results(cursor, queries, columns, gff_out=sys.stdout):
	#print("Q", type(queries))
	gene_data = list(batch_query_db(cursor, (q[0] for q in queries)))
	#print(gene_data)
	queries = dict(queries)

	missing = set(queries).difference(gd[-1] for gd in gene_data)
	if missing:
		print(*("MISSING DATA FOR: {}".format(m) for m in missing), sep="\n", flush=True)
	for gd in gene_data:
		col9 = "ID={gid};{col9}".format(gid=queries[gd[-1]]["query_name"], col9=generate_col9(queries[gd[-1]], columns))
		print(*gd[:-1], col9, sep="\t", file=gff_out)
	gff_out.flush()




def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("input_db", type=str)
	ap.add_argument("input_eggnog", type=str)
	ap.add_argument("--batch_size", type=int, default=1000)
	args = ap.parse_args()

	eggnog_f = gzip.open(args.input_eggnog, "r")
	header = None
	for i, line in enumerate(eggnog_f):
		if not line.startswith("#"):
			break
		header = line
	if header is None or (not header.startswith("# query_name") and not header.startswith("#query_name")):
		raise ValueError("No valid header line found in eggnog-mapper output.\nheader is {}".format(header))
	header = header.strip().strip("#").strip().split("\t")
	print("header:", header, flush=True)
	skiplines = i

	extract_columns = {6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 20}
	extract_columns = [col for i, col in enumerate(header) if i in extract_columns]

	eggnog_f = open(args.input_eggnog)
	out_f = open(os.path.basename(args.input_eggnog).replace(".emapper.annotations", "") + ".gff", "w")

	conn = sqlite3.connect(args.input_db)
	cursor = conn.cursor()

	with eggnog_f as eggnog_in, out_f as gff_out:

		for i in range(skiplines):
			print("skipping", next(eggnog_in), flush=True)
		#header = [next(eggnog_in) for i in range(i + 1)][-1].strip("#").strip().split("\t")

		print("##gff-version 3", file=gff_out, flush=True)

		queries = list()
		for i, row in enumerate(csv.DictReader(eggnog_in, fieldnames=header, delimiter="\t")):
			query = row["query_name"]
			# correcting freeze12-specific issues
			query = query.replace(",", "")
			#query = query.replace("'", "''")
			#print(query)
			queries.append((query, row))

			#print(len(queries))
			if len(queries) == args.batch_size:
				process_query_results(cursor, queries, extract_columns, gff_out=gff_out)
				queries.clear()

				#gene_data = list(query_db(cursor, (q[0] for q in queries)))
				#missing = set(q[0] for q in queries).difference(gd[-1] for gd in gene_data)
				#if missing:
				#	print(*("MISSING DATA FOR: {}".format(m) for m in missing), sep="\n", flush=True)
				#for gd in gene_data:
				#	col9 = "ID={gid};{col9}".format(gid=row["query_name"], col9=generate_col9(row, extract_columns))
				#	print(*gd[:-1], col9, sep="\t", file=gff_out, flush=i%1000==0)                           					

			#gene_data = query_db(cursor, query)
			# print(i, gene_data, flush=True)
			#if gene_data is None:
			#	print("MISSING DATA FOR: {}".format(query, flush=True)
			#else:
			#	col9 = "ID={gid};{col9}".format(gid=row["query_name"], col9=generate_col9(row, extract_columns))
			#	print(*gene_data[:-1], col9, sep="\t", file=gff_out, flush=i%1000==0)
		
		process_query_results(cursor, queries, extract_columns, gff_out=gff_out)


if __name__ == "__main__":
	main()

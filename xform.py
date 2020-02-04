#!/usr/bin/env python3

import csv
from collections import OrderedDict
import os


def data_in(filename):
    """
    #:-D data_in assumes the file has a non-delmited header then followed by
    #:-D a delimited list of field names followed by actual comma delimited data.
    #:-O An interator is returned that yields
    #:-O FILE HEADER
    #:-O CSV HEADER
    #:-O CSV DATA
    #:-|
    """
    with open(filename, "r", newline=None) as fin:
        file_header = next(fin)
        yield file_header
        csvin = csv.DictReader(fin, lineterminator="\n", delimiter=",")
        for doc in csvin:
            yield doc


def xform_doc(doc, header_out, xformer):
    #Clean up field names in case they have leading/trailing spaces
    retval = OrderedDict([(k.strip(), v) for k, v in doc.items()])

    #Skip the file if it has already been transformed.
    #This is a weak test, but shows this edge case must be considered
    if "Client Security Code" not in retval.keys():
        retval = OrderedDict([(k, retval.get(xformer.get(k), xformer.get(k)))
                             for k in header_out])
    return retval


def xform_file(filename_in, filename_out, verbose=True):
    """Transform the CSV filename_in to have the record structure
    specified by header_out.
    The xformer dictionary will replace the field names found in
    the incoming record with field names found in header_out.
    If the field name is not found it will add the constant value
    corresponding with the header_out key.

    Note: If there is an execption that occurs while writing
    it will skip that record and continue to allow the next file
    to be processed.
    """
    msg = "Transforming {0} to {1}".format
    header_out = [
        "Client Security Code",
        "Quantity",
        "Security ID",
        "Country",
        "Currency"
    ]
    xformer = {
        "Client Security Code": "SEC ID",
        "Quantity": "QTY APPROVED",
        "Security ID": "T",
        "Country": "US",
        "Currency": "US",
    }

    file_data = data_in(filename_in)
    file_type = next(file_data)

    if verbose:
        print(msg(filename_in, filename_out))

    with open(filename_out, "w") as fout:
        fout.write(file_type)
        csvout = csv.DictWriter(fout, fieldnames=header_out)
        csvout.writeheader()
        for doc in file_data:
            xformed_doc = xform_doc(doc, header_out, xformer)
            try:
                csvout.writerow(xformed_doc)
            except Exception:
                print(xformed_doc)
                continue


def files_to_change(folder_in, folder_out):
    """List all the csv files in the folder_in directory and pair them
    with a corresponding file name in foulder_out.
    """
    for f in os.listdir(folder_in):
        if f.endswith(".csv"):
            yield (os.path.join(folder_in, f), os.path.join(folder_out, f))


if __name__ == "__main__":
    for in_out in files_to_change("./data/lz", "./data/pub"):
        xform_file(in_out[0], in_out[1])

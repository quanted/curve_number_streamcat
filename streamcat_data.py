from decimal import Decimal
import json
import csv
from ftplib import FTP
from zipfile import ZipFile
import os


def open_file(path):
    with open(path, 'r') as f:
        return f.read()


catchment_ftp_url = "newftp.epa.gov"
catchment_ftp_dir = "/EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/HydroRegions/"

region_nlcd = {}
region_statsgo = {}


def get_streamcat_data(files):
    print("Importing epa streamcat files...")
    for sfile, file in files.items():
        ofile = "Data/{}".format(file)
        if not os.path.isfile(ofile):
            print("Downloading {} from {}".format(file, catchment_ftp_url))
            ftp = FTP(catchment_ftp_url, "", "")
            ftp.login()
            ftp.cwd(catchment_ftp_dir)
            with open(ofile, 'wb') as fp:
                res = ftp.retrbinary('RETR %s' % file, fp.write)
                if not res.startswith('226 Transfer complete'):
                    print('Download failed')
                    if os.path.isfile(ofile):
                        os.remove(ofile)
            ftp.close()
            print("Download complete.")
        if not os.path.isfile(sfile):
            print("Extracting {}".format(ofile))
            with ZipFile(ofile) as zipfile:
                zipfile.extractall("Data")
    for file in files.keys():
        if "NLCD2011" in file:
            print("Importing {} to region_nlcd".format(file))
            with open(file, newline='') as f:
                data = csv.DictReader(f)
                json_data = {}
                for row in data:
                    json_data[row["COMID"]] = row
                global region_nlcd
                region_nlcd = json_data
            print("Import complete.")
        if "STATSGO" in file:
            print("Importing {} to region_statsgo".format(file))
            with open(file, newline='') as f:
                data = csv.DictReader(f)
                json_data = {}
                for row in data:
                    json_data[row["COMID"]] = row
                global region_statsgo
                region_statsgo = json_data
            print("Import complete.")
    print("Completed import of streamcat files.")


def main():
    region = "17"

    files = {
        "Data/NLCD2011_Region{}.csv".format(region): "NLCD2011_Region{}.zip".format(region),
        "Data/STATSGO_Set1_Region{}.csv".format(region): "STATSGO_Set1_Region{}.zip".format(region)}
    get_streamcat_data(files)


if __name__ == "__main__":
    main()

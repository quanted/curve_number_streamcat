from decimal import Decimal
import sqlite3
# import concurrent.futures
# import asyncio
# import numpy as np
# import requests
import json
import csv
# import time
from ftplib import FTP
from zipfile import ZipFile
import os


# Steps
# 1. import catchment_ndvi.csv file
# 2. collect streamcat data, from ftp site, and populate streamcat catchment object
# 3. iterate through catchment, by ComID from 1.
# 4. calculate cn values for each ndvi value (368), average for each time period (23)
# 5. calculate weighted CN value for catchment using updated cn values from 4.
# 6. export weighted cn for catchment (23 values)
# 7. export catchment data


def open_file(path):
    with open(path, 'r') as f:
        return f.read()


catchment_ftp_url = "newftp.epa.gov"
catchment_ftp_dir = "/EPADataCommons/ORD/NHDPlusLandscapeAttributes/StreamCat/HydroRegions/"

curvenumber_db = "curvenumber.sqlite3"

curvenumbers = json.loads(open_file("curvenumber.json"))
curvenumber_conditions = json.loads(open_file("curvenumber_conditions.json"))
curvenumber_ndvi = json.loads(open_file("curvenumber_ndvi.json"))

ndvi_data = {}
region_nlcd = {}
region_statsgo = {}


def get_db_connection():
    """
    Connect to sqlite database located at db_path
    :return: sqlite connection
    """
    conn = sqlite3.connect(curvenumber_db)
    conn.isolation_level = None
    return conn


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


def update_database():
    """
    :return: None
    """
    conn = get_db_connection()
    c = conn.cursor()

    table_check_query = "PRAGMA TABLE_INFO('PlusFlowlineVAA')"
    current_table_state = c.execute(table_check_query)
    column_names = [col[1] for col in current_table_state.fetchall()]
    if 'CurveNumber' not in column_names:
        update_query = "ALTER TABLE PlusFlowlineVAA ADD CurveNumber DECIMAL(10, 5)"
        c.execute(update_query)
        conn.commit()
    conn.close()


class Catchment:
    """
    Catchment data from epa waters watershed report
    https://watersgeo.epa.gov/watershedreport/?comid=
    """

    def __init__(self, ndvi_data, _region, i):
        self.comid = ndvi_data["ComID"]
        self.region = _region
        self.landcover = None  # NLCD landcover data for the catchment
        self.soil = None  # Statsgo soil data for the catchment
        self.hsg = None  # Hydrologic Soil Group calculated from statsgo soil data
        self.valid_catchment = True
        self.set_catchment_data()  # Function to populate landcover and soil dictionaries
        self.calculate_hsg()  # Function to calculate hydrologic soil group
        self.ndvi = {}
        self.curve_number = {}  # Catchments calculated curve number value
        self.curve_number_avg = {}
        self.set_ndvi(ndvi_data)
        self.calculate_curvenumber()  # Function to calculate curve number
        self.calculate_curvenumber_avg()
        self.update_database(i)

    def set_catchment_data(self):
        """
        Sets the data from the stream cat request to the landcover and soil attributes
        :return: None
        """
        landcover = {"11": -1, "12": -1, "21": -1, "22": -1, "23": -1, "24": -1, "31": -1, "41": -1, "42": -1, "43": -1,
                     "51": -1, "52": -1, "71": -1, "72": -1, "73": -1, "74": -1, "81": -1, "82": -1, "90": -1, "95": -1}
        soil = {'clay': -1, 'sand': -1}
        if self.comid not in region_nlcd or self.comid not in region_statsgo:
            self.valid_catchment = False
            return
        nlcd = region_nlcd[self.comid]
        statsgo = region_statsgo[self.comid]
        landcover['11'] = -1 if "NA" in nlcd["PctOw2011Cat"] else float(nlcd["PctOw2011Cat"])
        landcover['12'] = -1 if "NA" in nlcd["PctIce2011Cat"] else float(nlcd["PctIce2011Cat"])
        landcover['21'] = -1 if "NA" in nlcd["PctUrbOp2011Cat"] else float(nlcd["PctUrbOp2011Cat"])
        landcover['22'] = -1 if "NA" in nlcd["PctUrbLo2011Cat"] else float(nlcd["PctUrbLo2011Cat"])
        landcover['23'] = -1 if "NA" in nlcd["PctUrbMd2011Cat"] else float(nlcd["PctUrbMd2011Cat"])
        landcover['24'] = -1 if "NA" in nlcd["PctUrbHi2011Cat"] else float(nlcd["PctUrbHi2011Cat"])
        landcover['31'] = -1 if "NA" in nlcd["PctBl2011Cat"] else float(nlcd["PctBl2011Cat"])
        landcover['41'] = -1 if "NA" in nlcd["PctDecid2011Cat"] else float(nlcd["PctDecid2011Cat"])
        landcover['42'] = -1 if "NA" in nlcd["PctConif2011Cat"] else float(nlcd["PctConif2011Cat"])
        landcover['43'] = -1 if "NA" in nlcd["PctMxFst2011Cat"] else float(nlcd["PctMxFst2011Cat"])
        landcover['52'] = -1 if "NA" in nlcd["PctShrb2011Cat"] else float(nlcd["PctShrb2011Cat"])
        landcover['71'] = -1 if "NA" in nlcd["PctGrs2011Cat"] else float(nlcd["PctGrs2011Cat"])
        landcover['81'] = -1 if "NA" in nlcd["PctHay2011Cat"] else float(nlcd["PctHay2011Cat"])
        landcover['82'] = -1 if "NA" in nlcd["PctCrop2011Cat"] else float(nlcd["PctCrop2011Cat"])
        landcover['90'] = -1 if "NA" in nlcd["PctWdWet2011Cat"] else float(nlcd["PctWdWet2011Cat"])
        landcover['95'] = -1 if "NA" in nlcd["PctHbWet2011Cat"] else float(nlcd["PctHbWet2011Cat"])
        soil['clay'] = 0 if "NA" in statsgo["ClayCat"] else float(statsgo["ClayCat"])
        soil['sand'] = 0 if "NA" in statsgo["SandCat"] else float(statsgo["SandCat"])
        self.landcover = landcover
        self.soil = soil

    def calculate_hsg(self):
        """
        Calculate Hydorlogic Soil Group from the clay and sand composition data at streamcat
        Reference: https://daac.ornl.gov/SOILS/guides/Global_Hydrologic_Soil_Group.html
        """
        if not self.valid_catchment:
            return
        sand = self.soil['sand']
        clay = self.soil['clay']
        hsg = 'A'
        if sand > 90 and clay < 10:
            hsg = 'A'
        elif 50 < sand < 90 and 10 < clay < 20:
            hsg = 'B'
        elif sand < 50 and 20 < clay < 40:
            hsg = 'C'
        elif sand < 50 and clay > 40:
            hsg = 'D'
        self.hsg = hsg

    def set_ndvi(self, ndvi):
        _ndvi = {}
        i = 0
        for k, v in ndvi.items():
            if k is not "ComID":
                _ndvi[i] = float(v)
                i = i + 1
        self.ndvi = _ndvi

    def get_ndvi_class(self, nlcd_class, value):
        ndvi_row = curvenumber_ndvi[nlcd_class]
        if value <= float(ndvi_row["POOR"]):
            return "POOR"
        elif float(ndvi_row["POOR"]) < value < float(ndvi_row["GOOD"]):
            return "FAIR"
        else:
            return "GOOD"

    def calculate_curvenumber(self):
        """
        Calculate curve number for catchment from the populated landcover, soil and hsg values
        Reference: https://directives.sc.egov.usda.gov/OpenNonWebContent.aspx?content=41606.wba
        Reference: https://en.wikipedia.org/wiki/Runoff_curve_number for classes 41, 42, and 43
        """
        if not self.valid_catchment:
            return
        cn = {}
        k_values = ["41", "42", "43", "52", "71", "81", "82"]
        for i, ndvi in self.ndvi.items():
            cn_0 = 0
            for k, v in self.landcover.items():
                # k: nlcd class
                # v: percent value
                # v = -1: not applicable
                if v == -1:
                    cn[i] = -1
                    continue
                if k not in k_values:
                    k_cn = float(curvenumbers[k][self.hsg])
                else:
                    if ndvi == -9998:
                        cn[i] = -1
                        continue
                    k_cn = float(curvenumber_conditions[k][self.get_ndvi_class(k, ndvi)][self.hsg])
                if k_cn == -1:
                    cn[i] = -1
                    continue
                cn_0 = cn_0 + (k_cn * v / 100)
            if cn_0 == 0:
                # no valid landcover or request to streamCat produced no data
                cn_0 = -1
            elif 0 < cn_0 < 30:
                cn_0 = 30
            cn[i] = cn_0
        self.curve_number = cn

    def calculate_curvenumber_avg(self):
        if not self.valid_catchment:
            return
        cn_avg_0 = {}
        i = 0
        for k, v in self.curve_number.items():
            if i >= 23:
                i = 0
            if v == -1:
                i = i + 1
                continue
            else:
                c = cn_avg_0[i][1] + 1 if i in cn_avg_0 else 1
                cn_a = cn_avg_0[i][0] + v if i in cn_avg_0 else v
                cn_avg_0[i] = [cn_a, c]
            i = i + 1
        cn_avg = {}
        for k, v in cn_avg_0.items():
            cn = v[0] / v[1]
            cn_avg[k] = round(Decimal(cn), 4)
        self.curve_number_avg = cn_avg

    def update_database(self, j):
        if not self.valid_catchment:
            print("Invalid Catchment. Not found in streamcat data. ComID: {}".format(self.comid))
            return
        conn = get_db_connection()
        c = conn.cursor()

        c.execute("BEGIN TRANSACTION")
        for i, cn in self.curve_number.items():
            query = "INSERT INTO CurveNumberRaw (ComID, TimeStep, CN) VALUES({},{},{})".format(self.comid, i, cn)
            c.execute(query)
        query = "INSERT INTO CurveNumber (ComID) VALUES ({})".format(self.comid)
        c.execute(query)
        for i, cn in self.curve_number_avg.items():
            if i < 10:
                i = "0{}".format(i)
            query = "UPDATE CurveNumber SET CN_{}={} WHERE ComID={}".format(i, cn, self.comid)
            c.execute(query)
        c.execute("COMMIT")
        conn.close()
        print("Completed: {}, ComID: {}".format(j, self.comid))


# executor = concurrent.futures.ThreadPoolExecutor(max_workers=6)


def cn_calculation_region(region):
    """
    Calculate curve number for all catchments in database.
    :return: None
    """
    ndvi_file = "catchment_ndvi_{}.csv".format(region)
    with open(ndvi_file, newline='') as csvfile:
        print("Importing ndvi data. Region: {}, File: {}".format(region, ndvi_file))
        data = csv.DictReader(csvfile)
        json_data = {}
        for row in data:
            json_data[row["ComID"]] = row
        global ndvi_data
        ndvi_data = json_data
        print("Import complete.")
    # inputs = []
    conn = get_db_connection()
    c = conn.cursor()
    finished_catchments = []
    for comid in c.execute("SELECT ComID FROM CurveNumber"):
        finished_catchments.append(str(comid[0]))
    conn.close()
    i = 1
    for v, row in ndvi_data.items():
        if str(row["ComID"]) not in finished_catchments:
            Catchment(row, region, i)
            # inputs.append([row, region, i])
            i = i + 1
    # loop = asyncio.get_event_loop()
    # futures = [loop.run_in_executor(executor, Catchment, catchment) for catchment in inputs]
    # asyncio.gather(*futures)


def main():
    region = "09"

    files = {
        "Data/NLCD2011_Region{}.csv".format(region): "NLCD2011_Region{}.zip".format(region),
        "Data/STATSGO_Set1_Region{}.csv".format(region): "STATSGO_Set1_Region{}.zip".format(region)}
    get_streamcat_data(files)

    cn_calculation_region(region)


if __name__ == "__main__":
    main()

from decimal import Decimal
import sqlite3
import concurrent.futures
import asyncio
import requests
import json
import csv
import time
import os


def get_db_connection():
    """
    Connect to sqlite database located at db_path
    :return: sqlite connection
    """
    db_path = os.getenv('HMS_DB_PATH')
    conn = sqlite3.connect(db_path)
    conn.isolation_level = None
    return conn


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


def import_mapping():
    """
    Imports the nlcd 2011 curve number mapping csv located at mapping_file.
    :return: mapping dictionary for curve number values based on class and hydrologic soil group
    """
    mapping_file = "nlcd2011_curvenumber_mapping.csv"
    nlcd_cn = {}
    with open(mapping_file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            class_cn = {'A': row['A'], 'B': row['B'], 'C': row['C'], 'D': row['D']}
            nlcd_cn[row['class']] = class_cn
        return nlcd_cn


mapping = import_mapping()


class Catchment:
    """
    Catchment data from epa waters watershed report
    https://watersgeo.epa.gov/watershedreport/?comid=
    """
    def __init__(self, _comid):
        self._comid = _comid
        self.data = self.get_streamcat_data()

    def get_streamcat_data(self):
        """
        Makes request to streamcat for catchment data.
        :return: json object of the requested catchment data
        """
        # all_area_of_interest = ["Catchment%2FWatershed", "Riparian%20Buffer%20(100m)"]
        # all_metric_types = ["Agriculture", "Climate", "Disturbance", "Hydrology", "Infrastructure", "Land%20Cover",
        #               "Lithology", "Mines", "Pollution", "Riparian", "Soils", "Topography", "Urban", "Wetness"]
        area_of_interest = ["Catchment%2FWatershed"]
        metric_types = ["Agriculture", "Hydrology", "Land%20Cover", "Soils", "Urban"]
        request_url = "https://ofmpub.epa.gov/waters10/streamcat.jsonv25?pcomid={}&pAreaOfInterest={}&" \
                      "pLandscapeMetricType={}&pLandscapeMetricClass=Disturbance;Natural&" \
                      "pFilenameOverride=AUTO".format(self._comid, ';'.join(area_of_interest), ';'.join(metric_types))
        try:
            request_data = requests.get(request_url)
        except requests.exceptions.HTTPError as e:
            print("Data Request Error: {}".format(e))
            return ""
        try:
            return json.loads(request_data.text)
        except json.JSONDecodeError as e:
            print("Data Load Error: {}".format(e))
            return ""


class CurveNumber:
    """
    Calculate the curve number for a specified comid catchment
    Reference landcover from nlcd 2011 data: https://www.mrlc.gov/nlcd11_leg.php
    """
    def __init__(self, _comid):
        self.catchment = Catchment(_comid)  # Catchment object
        self.landcover = None               # NLCD landcover data for the catchment
        self.soil = None                    # Statsgo soil data for the catchment
        self.hsg = None                     # Hydrologic Soil Group calculated from statsgo soil data
        self.set_catchment_data()           # Function to populate landcover and soil dictionaries
        self.calculate_hsg()                # Function to calculate hydrologic soil group
        self.curve_number = None            # Catchments calculated curve number value
        self.calculate_curvenumber()        # Function to calculate curve number
        self.add_to_database()              # Add curve number to database for catchment

    def set_catchment_data(self):
        """
        Sets the data from the stream cat request to the landcover and soil attributes
        :return: None
        """
        landcover = {"11": -1, "12": -1, "21": -1, "22": -1, "23": -1, "24": -1, "31": -1, "41": -1, "42": -1, "43": -1,
                     "51": -1, "52": -1, "71": -1, "72": -1, "73": -1, "74": -1, "81": -1, "82": -1, "90": -1, "95": -1}
        soil = {'clay': -1, 'sand': -1}
        if self.catchment.data['output'] is not None:
            for i in self.catchment.data['output']['metrics']:
                if i['id'] == "pctow2011cat":               # Open Water Land Cover Percentage; class 11
                    landcover['11'] = i['metric_value']
                elif i['id'] == "pctice2011cat":            # Ice/Snow Cover Percentage; class 12
                    landcover['12'] = i['metric_value']
                elif i['id'] == "pcturbop2011cat":          # Developed, Open Space Land Use Percentage; class: 21
                    landcover['21'] = i['metric_value']
                elif i['id'] == "pcturblo2011cat":          # Developed, Low Intensity Land Use Percentage; class: 22
                    landcover['22'] = i['metric_value']
                elif i['id'] == "pcturbmd2011cat":          # Developed, Medium Intensity Land Use Percentage; class: 23
                    landcover['23'] = i['metric_value']
                elif i['id'] == "pcturbhi2011cat":          # Developed, High Intensity Land Use Percentage; class: 24
                    landcover['24'] = i['metric_value']
                elif i['id'] == "pctbl2011cat":             # Bedrock and Similar Earthen Material Percentage; class: 31
                    landcover['31'] = i['metric_value']
                elif i['id'] == "pctdecid2011cat":          # Deciduous Forest Land Cover Percentage; class: 41
                    landcover['41'] = i['metric_value']
                elif i['id'] == "pctconif2011cat":          # Evergreen Forest Land Cover Percentage; class: 42
                    landcover['42'] = i['metric_value']
                elif i['id'] == "pctmxfst2011cat":          # Mixed Deciduous/Evergreen Forest Land Cover Percentage; class 43
                    landcover['43'] = i['metric_value']
                # Class 51 Dwarf Scrub (Alaska only)
                elif i['id'] == "pctshrb2011cat":           # Shrub/Scrub Land Cover Percentage; class 52
                    landcover['52'] = i['metric_value']
                elif i['id'] == "pctgrs2011cat":            # Grassland/Herbaceous Land Cover Percentage; class: 71
                    landcover['71'] = i['metric_value']
                # Class 72 Sedge/Herbaceous (Alaska only)
                # Class 73 Lichens (Alaska only)
                # Class 74 Moss (Alaska only)
                elif i['id'] == "pcthay2011cat":            # Pasture Hay Land Use Percentage; class: 81
                    landcover['81'] = i['metric_value']
                elif i['id'] == "pctcrop2011cat":           # Row Crop Land Use Percentage; class 82
                    landcover['82'] = i['metric_value']
                elif i['id'] == "pctwdwet2011cat":          # Woody Wetland Land Cover Percentage; class 90
                    landcover['90'] = i['metric_value']
                elif i['id'] == "pcthbwet2011cat":          # Herbaceous Wetland Land Cover Percentage; class: 95
                    landcover['95'] = i['metric_value']
                elif i['id'] == "claycat":                  # Statsgo Catchment Clay Mean
                    soil['clay'] = i['metric_value']
                elif i['id'] == "sandcat":                  # Statsgo Catchment Sand Mean
                    soil['sand'] = i['metric_value']
        self.landcover = landcover
        self.soil = soil

    def calculate_hsg(self):
        """
        Calculate Hydorlogic Soil Group from the clay and sand composition data at streamcat
        Reference: https://daac.ornl.gov/SOILS/guides/Global_Hydrologic_Soil_Group.html
        """
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

    def calculate_curvenumber(self):
        """
        Calculate curve number for catchment from the populated landcover, soil and hsg values
        Reference: https://directives.sc.egov.usda.gov/OpenNonWebContent.aspx?content=41606.wba
        Reference: https://en.wikipedia.org/wiki/Runoff_curve_number for classes 41, 42, and 43
        """
        cn = 0
        for k, v in self.landcover.items():
            # k: nlcd class
            # v: percent value
            # v = -1: not applicable
            if v == -1:
                continue
            k_cn = float(mapping[k][self.hsg])
            if k_cn == -1:
                continue
            cn = cn + (k_cn * v/100)
        if cn == 0:
            # no valid landcover or request to streamCat produced no data
            cn = -1
        elif 0 < cn < 30:
            cn = 30
        self.curve_number = round(Decimal(cn), 4)

    def add_to_database(self):
        """
        Adds calculated curve number to database table
        :return:
        """
        _conn = get_db_connection()
        update_query = "UPDATE PlusFlowlineVAA SET CurveNumber = {} WHERE ComID = {}"\
            .format(self.curve_number, self.catchment._comid)
        with _conn:
            _conn.execute(update_query)
        _conn.close()
        time.sleep(2)
        print("COMID: {}; CN: {}".format(self.catchment._comid, self.curve_number))


def cn_calculation_catchment(comid):
    """
    Calculate curve number for single catchment
    :param comid: Catchment comid
    :return: None
    """
    start_t = time.time()
    c = CurveNumber(comid)
    end_t = time.time()
    print("Total Computation Time: {} sec".format(round(end_t - start_t, 3)))


executor = concurrent.futures.ThreadPoolExecutor(max_workers=6)


def cn_calculation_conus():
    """
    Calculate curve number for all catchments in database.
    :return: None
    """
    update_database()
    conn = get_db_connection()
    c = conn.cursor()
    comid_query = "SELECT ComID, CurveNumber FROM PlusFlowlineVAA WHERE CurveNumber IS NULL"
    comid_inputs = []
    for comid in c.execute(comid_query):
        comid_inputs.append(comid[0])
    loop = asyncio.get_event_loop()
    futures = [loop.run_in_executor(executor, CurveNumber, cid) for cid in comid_inputs]
    asyncio.gather(*futures)
    conn.close()


def main():
    cn_calculation_conus()


if __name__ == "__main__":
    main()

import sqlite3
import csv
import pandas as pd
import multiprocessing as mp

results = []
ndvi_missing = []
cn_missing = []

class HUCData:
    def __init__(self, huc, file_path, ndvi_file):
        self.years = [y for y in range(2001, 2018)]
        self.database = "curvenumber.sqlite3"
        self.huc = huc
        self.file_path = file_path
        self.ndvi_file = ndvi_file
        self.comids = []
        self.columns = None
        self.data_total = 0
        self.ndvi_data = None
        self.load_comids()
        self.data = None

    def connect_to_db(self):
        db_conn = sqlite3.connect(self.database)
        db_conn.isolation_level = None
        return db_conn

    def initialize(self):
        columns = ["COMID", "Year"]
        for i in range(0, 23):
            v = "0{}".format(i) if i < 10 else "{}".format(i)
            c = "CN{}".format(v)
            columns.append(c)
        for i in range(0, 23):
            v = "0{}".format(i) if i < 10 else "{}".format(i)
            n = "NDVI{}".format(v)
            columns.append(n)
        self.columns = columns
        self.data = pd.DataFrame(columns=columns)
        df_full = None
        for f in self.ndvi_file:
            df = pd.read_csv(f)
            if df_full is None:
                df_full = df
            else:
                df_full = df_full.append(df)
        self.ndvi_data = df_full

    def load_comids(self):
        self.initialize()
        print("Loading catchments for huc: {}".format(self.huc))
        with open(self.file_path, newline='') as f:
            rf = csv.DictReader(f)
            for row in rf:
                self.comids.append(row['COMID'])
        print("Loading catchment data...")
        self.iterate_comids()
        print("Writing to csv file...")
        global results
        for d in results:
            self.data = self.data.append(d)
        print("DataFrame (rows/columns): {}".format(self.data.shape))
        self.write_metafile()
        self.data.to_csv("huc_data\\{}_cn_ndvi_data.csv".format(self.huc), index=None, header=True)
        print("HUC: {} Completed.".format(self.huc))

    def write_metafile(self):
        global cn_missing
        global ndvi_missing
        with open("huc_data\\{}_metadata.txt".format(self.huc), "w") as f:
            f.write("HUC: {}\n".format(self.huc))
            f.write("DataFrame (rows/columns): {}\n".format(self.data.shape))
            f.write("\nCN Missing:\n")
            f.writelines(cn_missing)
            f.write("\nNDVI Missing:\n")
            f.writelines(ndvi_missing)

    def iterate_comids(self):
        self.data_total = len(self.comids)
        #print("COMID N: {}".format(self.data_total))
        #for c in self.comids:
        #    self.get_catchment_data(c)
        print("CPU Count: {}".format(mp.cpu_count()))
        pool = mp.Pool(mp.cpu_count())
        global results
        results = pool.map_async(self.get_catchment_data, [c for c in self.comids]).get()
        pool.close()
        pool.join()

    def get_catchment_data(self, comid):
        cn = self.query_cn(comid)
        ndvi = self.query_ndvi(comid)
        if ndvi.size == 0 and len(cn) == 0:
            print("NOT FOUND IN BOTH CN and NDVI DB: COMID {}".format(comid))
        elif ndvi.size == 0:
            print("NDVI-NOT-FOUND: COMID {}".format(comid))
        elif len(cn) == 0:
            print("CN-NOT-FOUND: COMID {}".format(comid))
        rows = {}
        for c in self.columns:
            rows[c] = []
        for i in range(0, ndvi.size - 1):
            year_i = int(i / 23)
            mod_i = i % 23
            if i == 0 or mod_i == 0:
                rows["COMID"].append(comid)
                rows["Year"].append(self.years[year_i])
            mi = "0{}".format(mod_i) if mod_i < 10 else "{}".format(mod_i)
            cn_title = "CN{}".format(mi)
            ndvi_title = "NDVI{}".format(mi)
            cn_value = cn[i][2]
            ndvi_value = ndvi.iloc[0, i]
            rows[cn_title].append(cn_value)
            rows[ndvi_title].append(ndvi_value)
        df = pd.DataFrame(rows, columns=self.columns, index=None)
        print("COMID: {}".format(comid))
        return df

    def query_cn(self, comid):
        conn = self.connect_to_db()
        # query = "SELECT Count(Distinct ComID) FROM CurveNumberRaw"
        query = "SELECT ComID, TimeStep, CN FROM CurveNumberRaw WHERE ComID={}".format(comid)
        c = conn.cursor()
        c.execute(query)
        values = c.fetchall()
        conn.close()
        return values

    def query_ndvi(self, comid):
        query = "ComID == {}".format(comid)
        values = self.ndvi_data.query(query)
        return values


# step 1: load text file into panadas dataframe
# step 2: iterate through comids in list
# step 3: collect data from tables CurveNumberRaw and NDVI for that comid (all dates)
# step 4: dump data into a dataframe
# step 5: repeat for all comids in list
# step 6: dump data into csv
# step 7: repeat for all files

if __name__ == "__main__":
    hucs = {
        # "03050105": ["huc_data\\03050105_COMID_Area.txt", ["catchment_ndvi_03N.csv"]],
        # "10250017": ["huc_data\\10250017_COMID_Area.txt", ["catchment_ndvi_10L_1.csv", "catchment_ndvi_10L_2.csv"]],
        # "10250017": ["huc_data\\10250017_COMID_Area.txt", ["catchment_ndvi_10L_1.csv"]],
        # "15020018": ["huc_data\\15020018_COMID_Area.txt", ["catchment_ndvi_15.csv"]],
        # "16060014": ["huc_data\\16060014_COMID_Area.txt", ["catchment_ndvi_16.csv"]]
        # "18090205": ["huc_data\\18090205_COMID_Area.txt", ["catchment_ndvi_18.csv"]]
        "15020018": ["huc_data\\15020018_COMID_Area.txt", ["catchment_ndvi_15.csv"]]
    }

    for k, v in hucs.items():
        h = HUCData(k, v[0], v[1])

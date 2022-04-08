import pandas as pd
from tqdm import tqdm

class DBExplorer(object):
    """Class that takes a connection to a database and provides methods
    to get a quick and dirty exploratory view of what the db is looking 
    like.
    
    Args: 
        conn(pyobdc.connection): The connection to the database.
    """
    def __init__(self, conn, indexing=True):
        self.conn = conn
        self.cursor = self.conn.cursor()
        self.create_tname_lists()

        # Check for table shapes.
        if indexing:
            try:
                self.shape_df = pd.read_csv("./resources/generated/table_shapes.csv")
                print("SUCCESS: Table shape files loaded!")
            except FileNotFoundError:
                print("HEADSUP: Database shape file not found. File will now be generated.")
                print("HEADSUP: This operation may take some time, depending on the size of the db. Grab a coffee." )
                self.shape_df = self.make_table_shape_table()

            # Check for view shapes.
            try:
                self.views_df = pd.read_csv("./resources/generated/views_shapes.csv")
                print("SUCCESS: Views Table shape files loaded!")
            except FileNotFoundError:
                print("HEADSUP: Database views shape file not found. File will now be generated.")
                print("HEADSUP: This operation may take some time, depending on the size of the db. Grab another coffee." )
                self.views_shape_df = self.make_table_shape_table(views=True)



    def create_tname_lists(self):
        """Sets self.base_table_names and self.view_table_names.
        These two lists act as an inventory of the different tables in
        the db that this calss is connected to."""

        print("Indexing table names...")

        QUERY = """SELECT *
                    from information_schema.tables;
                """

        self.cursor.execute(QUERY)

        self.base_table_names = []
        self.view_table_names = []
        for table in self.cursor.fetchall():
            if table[3] == "BASE TABLE":
                self.base_table_names.append(table[2])
            else:
                self.view_table_names.append(table[2])


        print("Table names indexed...")


    def make_table_shape_table(self, views=False):
        """Creates a MetaData File Called: table_shapes.csv which
        contains all the information about the number of fields
        and the number of entries in the db.
        
        Args:
            optional(views): Perform this operation for the views in the db, rather than the tables.
        Returns:
            out(pd.DataFrame): The shape dataframe (is set as either self.shape_df or self.view_shape_df)
        """

        if views:
            tlist = self.view_table_names
            out_fname = "table_shapes.csv"
        else:
            tlist = self.base_table_names
            out_fname = "views_table_shapes.csv"

        names = []
        entries = []
        headers = []
        for tname in tqdm(tlist):
            QUERY = f"""SELECT * from {tname}"""
            df = pd.read_sql(QUERY, self.conn)
            names.append(tname)
            entries.append(df.shape[0])
            headers.append(df.shape[1])

        df_dict = {"names": names, 
                   "entries": entries,
                   "headers": headers}

        out = pd.DataFrame(df_dict)
        out.to_csv(f"./resources/generated/{out_fname}")
        print("HEADSUP: table_shapes.csv written to folder: './resources/generated/'.")
        return out


    def make_key_data_table(self):
        """Creates a MetaData File Called: key_info.csv which
        contains all of the information about the fields in the db,
        how often those fields appear (if in multiple tables),
        and how many entries correspond to those keys.
        
        NOTE: Only does this wich actual tables, not views.
        """

        key_info_dict = {
                         "name": [],
                         "appears_in_multiple": [],
                         "appears_in_tables": [],
                         "appears_in_count": [],
                         "total_entires": [],
                         "dtype": []
        }

        for tname in tqdm(self.base_table_names):
            QUERY = f"""SELECT * from {tname}"""
            df = pd.read_sql(QUERY, self.conn)
            for col in df.columns:
                if col not in key_info_dict["name"]:
                    key_info_dict["name"].append(col)
                    key_info_dict["appears_in_multiple"].append(False)
                    key_info_dict["appears_in_tables"].append([tname])
                    key_info_dict["appears_in_count"].append(1)
                    key_info_dict["total_entires"].append(df.shape[0])
                    key_info_dict["dtype"].append(df.dtypes[col])
                else:
                    indx = key_info_dict["name"].index(col)
                    key_info_dict["appears_in_multiple"][indx] = True
                    key_info_dict["appears_in_tables"][indx].append(tname)
                    key_info_dict["appears_in_count"][indx] += 1
                    key_info_dict["total_entires"][indx] += df.shape[0]


        out = pd.DataFrame(key_info_dict)
        out.to_csv("./resources/generated/key_info.csv")
        print("HEADSUP: key_info.csv written to folder: './resources/generated/'.")
        return out


    def read_table_as_dataframe(self, tname, verbose=True):
        """Reads a table in the database in as a pandas DataFrame
        
        Args:
            tname(str): The name of the table to pull.
        Returns:
            df(pandas.DataFrame): A Dataframe object of the sql table.
        """

        if verbose:
            print(f"Pulling table: {tname}... ...")

        if tname in self.base_table_names:
            QUERY = f"""SELECT * from {tname}"""
            df = pd.read_sql(QUERY, self.conn)
            if verbose:
                print(f"Table {tname} pulled!")
            return df

        if verbose:
            print(f"Table name specificed: {tname} is not currently within the database.. ..")
            
        # Return empty df if table unable to be pulled.
        return pd.DataFrame()


    def check_for_join_options(self, tname1, tname2):
        """Checks if two databases have overlapping keys and returns the keys that the
        tables could be joined on.
        
        Args:
            tname1(str): The name of first table to check.
            tname2(str): The name of the second table to check.
        Returns:
            join_keys(list): A list of keys to perform the join on (empty if there are none).
        """

        df1 = self.read_table_as_dataframe(tname1)
        df2 = self.read_table_as_dataframe(tname2)

        if not df1.empty and not df2.empty:
            return [elm for elm in df1.columns if elm in df2.columns]
        print("One or more of the tables specified is not present in the db. Join keys could not be generated.")

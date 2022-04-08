import pandas as pd

# My imports
from sql_connector import SQLConnector
from sql_querier import SQLQuerier

# Now is when I tell you you need to be on the Syracuse network to run this whole thing.
def main():

    # Define list of table names you want to pull from tables_to_pull.txt
    TABLE_FPATH = "./tables_to_pull.txt"
    with open (TABLE_FPATH) as in_file:
        tnames = [l.strip("\n") for l in in_file.readlines()]

    # Store host ip, db name, user name, and user pwd in a separate file.
    DB_INFORMATION_FPATH = "./db_info.txt"
    with open (DB_INFORMATION_FPATH) as in_file:
        params = [l.strip("\n") for l in in_file.readlines()]

    # Create Connection
    sql = SQLConnector(params[0], params[1], params[2], params[3])
    # Create querier.
    querier = SQLQuerier(sql.get_connection())

    # Create a dictionary of all of the tables that looks like:
    # {"name_of_table": pd.DataFrame(table_data)}
    table_dict = {tname: querier.df_from_query(f"SELECT * FROM {tname}") for tname in tnames}
    # Print out a little data to ensure it worked.
    print(table_dict["Permit"].head(5))



if __name__ == "__main__":
    main()
# Syr-IPSPyODBCWrapper

Simple Python Wrapper to Pull Tables by name from IPS Database

# Using This Wrapper.

This is nothing fancy. Currently it just pulls whole tables, stores it in a dictionary, and does not put it anywhere. It can be edited to place it in a db elsewhere, or store the table as a flat file with relative ease. There is additional code for indexing here that is not used in the `main.py` file. (See code in `db_explorer.py`)

1. Clone or Download this Repo.
2. Create a virtual enviornment to store libraries. `python3 -m venv venv`
3. Activate the environment. `venv\Scripts\activate`. Or, if Linux / Mac `source venv/bin/activate`.
4. Install requirements. `pip install -r requirements.txt`
5. Create a file called: `db_info.txt` and put it in the root directory of this respoitory (more on this). `touch db_info.txt`
6. Edit `tables_to_pull.txt` to reflect the tables you want to pull.
7. Run `main.py`

# `db_info.txt` 

This file is just a simple text file that should be four lines long. Each line contains a piece of information to access the database.

Line One: Ip address of the server the db is located on.
Line two: Database name.
Line three: User name.
Line Four: User Password.

For this reason this file is not stored in this repository (as it is currently public).

# Example `db_info.txt`

```
xxx.xxx.xxx.xxx
my_database
my_user
my_password
```

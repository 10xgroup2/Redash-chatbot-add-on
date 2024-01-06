import pandas as pd
import os
import glob
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2

# Load environment variables from .env file
load_dotenv()


def load_csv_files_into_dataframes(parent_directory):
    """_summary_
    creates a dictionary where keys are in the format "parent_folder_file_name"
    and values are corresponding DataFrames
        Args:
            parent_directory (_type_): path to the csv files

        Returns:
            _type_: dictionary
    """
    # Use glob to get a list of all CSV files in subdirectories
    csv_files_path = os.path.join(parent_directory, "**/*.csv")
    csv_files = glob.glob(csv_files_path, recursive=True)

    # Create an empty dictionary to store DataFrames
    dataframes = {}

    # Iterate through each CSV file and load it into a DataFrame
    for csv_file in csv_files:
        # Extract the parent folder and file name without extension
        parent_folder = os.path.basename(os.path.dirname(csv_file))
        file_name = os.path.splitext(os.path.basename(csv_file))[0]

        # Create a DataFrame name as "parent_folder_file_name"
        dataframe_name = f"{parent_folder}_{file_name}"

        # Load CSV into a DataFrame
        df = pd.read_csv(csv_file)

        # Store the DataFrame in the dictionary
        dataframes[dataframe_name] = df

    return dataframes


# to store the name of the created tables by the write_to_sql
created_tables = []


def write_to_sql(df, table_name):
    global created_tables  # Reference the global list

    # Read connection parameters from environment variables
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database_name = os.getenv("DB_NAME")

    # Create the database engine
    connection_params = {"host": host, "user": user,
                         "password": password, "port": port, "database": database_name}
    engine = create_engine(
        f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
    )

    # Write data to a new table
    df.to_sql(name=table_name, con=engine, index=False, if_exists="replace")

    # Add the successfully created table name to the list
    created_tables.append(table_name)


def load_sql_data(table_name):
    # Read connection parameters from environment variables
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    database_name = os.getenv("DB_NAME")

    # Create the database engine
    connection_params = {"host": host, "user": user,
                         "password": password, "port": port, "database": database_name}
    engine = create_engine(
        f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
    )

    # SQL query to select all columns from the specified table
    sql_query = f"SELECT * FROM {table_name}"

    # Read data into a DataFrame
    df = pd.read_sql(sql_query, con=engine)

    return df


def missing_values_table(df):
    # Total missing values
    mis_val = df.isnull().sum()

    # Percentage of missing values
    mis_val_percent = 100 * df.isnull().sum() / len(df)

    # dtype of missing values
    mis_val_dtype = df.dtypes

    # Make a table with the results
    mis_val_table = pd.concat(
        [mis_val, mis_val_percent, mis_val_dtype], axis=1)

    # Rename the columns
    mis_val_table_ren_columns = mis_val_table.rename(
        columns={0: "Missing Values", 1: "% of Total Values", 2: "Dtype"})

    # Sort the table by percentage of missing descending
    mis_val_table_ren_columns = (
        mis_val_table_ren_columns[mis_val_table_ren_columns.iloc[:, 1] != 0]
        .sort_values("% of Total Values", ascending=False)
        .round(1)
    )

    # Print some summary information
    print(
        "Your selected dataframe has " + str(df.shape[1]) + " columns.\n"
        "There are " +
        str(mis_val_table_ren_columns.shape[0]) +
        " columns that have missing values."
    )

    # Return the dataframe with missing information
    return mis_val_table_ren_columns

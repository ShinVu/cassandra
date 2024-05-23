# Import libraries
import glob
import pandas as pd

# Get CSV files list from a folder
path = "../event_data"
csv_files = glob.glob(path + "/*.csv")

# Read each CSV file into DataFrame
# This creates a list of dataframes
df_list = (pd.read_csv(file, sep=',', header = 0) for file in csv_files)
# Concatenate all DataFrames
big_df   = pd.concat(df_list, ignore_index=True)

# Write DataFrames to csv file

big_df.to_csv("../data/event_data_merged.csv",index=False)
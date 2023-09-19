# -*- coding: utf-8 -*-
"""
Created on Thu May  4 16:59:41 2023

@author: nlanjewar
"""

# Importing standard python packages and supporting packages

import configparser
from time import time
import os
import pandas as pd
from calculations.data_preprocessing import DataPreprocessor
from calculations.visualizations import DataPlotter
import plotly.graph_objects as go
from plotly.offline import plot
import plotly
from packages.sql_logger import MySQLlogger
from packages.utilities import (
    check_files,
    create_logger,
    db_conn,
    read_input,
)

class NoDataException(Exception):
    """Raises a No Data Exception when data for next timestamp
    is unavailable"""

# Create a logger instance with default configs. For more configurations
# go to packages/utilities.py.
logging = create_logger()

logging.info("ING_BLC_SCRIPT started")

# Reading basic configurations
parser = configparser.ConfigParser()
parser.read(filenames=["config/config.ini"])
status = parser.get("DEFAULT", "status")
exec_mode = parser.get("DEFAULT", "mode")
interval = parser.get("DEFAULT", "interval")
file_list = ["config/config.ini", "config/taglist.csv", "config/EDA_config.xlsx"]
logger_config = dict(parser["LOGGER"].items())

# Creating SQL Logger instance in case of logging directly to SQL (For error logs in ICAP)
logger = MySQLlogger(
    logger_config["host"],
    logger_config["user"],
    logger_config["pass"],
    logger_config["schema"],
    logger_config["table"],
)
logger.initialize(logger_config["status"])
logger.formatter("message,timestamp,type,component")
# logger.error(f"'Script Started','{last_run_time}','BLC', 'ICAP_BLCXYZ'")

start = time() 

check_files(file_list)
# reading taglist file
taglist = pd.read_csv("config/taglist.csv")
alias_tags = tuple(taglist["alias"])

# Read Database configurations for DB connection
db_config = dict(parser["DB"].items())

#Establishing DB_Connection..
if exec_mode == "noconnect":
    db_connection = db_conn(
        db_config["host"],
        db_config["user"],
        db_config["pass"],
        db_config["schema"],
    )
    print("Database connection successful")
    logging.info("Database connection successful")
else:
    db_connection = None
    print("Reading CSV file")
    logging.info("Reading CSV file")
    
    

################################# Logic -Defined ############################

#####Reading Data from taglist.csv
tag_data = read_input(exec_mode,alias_tags,db_config["alias_table"],db_connection)

tag_data.to_excel("output/tag_output.xlsx")

def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    mapping_file = os.path.join(current_dir, "output", "tag_mapping.xlsx")
    output_file = os.path.join(current_dir, "output", "tag_output.xlsx")

    preprocessor = DataPreprocessor(mapping_file, output_file)
    preprocessor.process_data()
    plotter = DataPlotter(preprocessor.tag_output_df, preprocessor.tag_output_new, preprocessor.tag_names)
    # Read the configuration file
    config_data = pd.read_excel(r"config/EDA_config.xlsx")

    # Iterate over the configuration settings
    for _, row in config_data.iterrows():
        technique = row["Technique"]
        execute_technique = row["Value"]

        if execute_technique == "Yes":
            # Create a directory for the technique
            EDA_result_path = "output/EDA_techniques_output"
            os.makedirs(EDA_result_path, exist_ok=True)

            # Execute the selected technique
            if technique == "handle_duplicates":
                preprocessor.handle_duplicates()
                print("Executing handle_duplicates technique")
            elif technique == "clean_data":
                cleaned_data = preprocessor.clean_data()
                print("Executing clean_data technique")
                output_file = os.path.join(EDA_result_path, "clean_data_result.csv")
                cleaned_data.to_csv(output_file, index=False)

            elif technique == "impute_data":
                imputed_data = preprocessor.impute_data()
                print("Executing impute_data technique")
                output_file = os.path.join(EDA_result_path, "impute_data_result.csv")
                # Save the result to the output file
                imputed_data.to_csv(output_file, index=False)

            elif technique == "remove_outliers_user_defined":
                remove_outliers_ud = preprocessor.remove_outliers_user_defined()
                print("Executing remove_outliers_user_defined technique")
                output_file = os.path.join(EDA_result_path, "remove_outliers_user_defined_result.csv")
                remove_outliers_ud.to_csv(output_file, index=False)
                
            elif technique == "remove_outliers_iqr":
                iqr_result_data = preprocessor.remove_outliers_iqr()
                print("Executing remove_outliers_iqr technique")
                output_file = os.path.join(EDA_result_path, "remove_outliers_iqr_result.csv")
                iqr_result_data.to_csv(output_file, index=False)
                
            elif technique == "remove_outliers_percentile":
                percentile_result_data = preprocessor.remove_outliers_percentile()
                print("Executing remove_outliers_percentile technique")
                output_file = os.path.join(EDA_result_path, "remove_outliers_percentile_result.csv")
                percentile_result_data.to_csv(output_file, index=False)

            elif technique == "remove_outliers_z_scores":
                z_scores_result_data = preprocessor.remove_outliers_z_scores()
                print("Executing remove_outliers_z_scores technique")
                output_file = os.path.join(EDA_result_path, "remove_outliers_z_scores_result.csv")
                z_scores_result_data.to_csv(output_file, index=False)
            
            elif technique == "plot_histogram":
                plot_histogram_fig = plotter.plot_histogram()
                print("Executing plot_histogram technique")
                output_file = os.path.join(EDA_result_path, f"{technique}_result.html")  # Change the file extension to .html
                # Save the result to the output file
                plot_histogram_fig.write_html(output_file)    
            
            elif technique == "plot_bar_chart":
                bar_chart_fig = plotter.plot_bar_chart()
                print("Executing plot_bar_chart technique")
                output_file = os.path.join(EDA_result_path, f"{technique}_result.html")  # Change the file extension to .html
                # Save the result to the output file
                bar_chart_fig.write_html(output_file)

            elif technique == "plot_box_plot":
                box_plot_fig = plotter.plot_box_plot()
                print("Executing plot_box_plot technique")
                output_file = os.path.join(EDA_result_path, f"{technique}_result.html")
                # Save the result to the output file
                box_plot_fig.write_html(output_file)
                
            elif technique == "plot_scatter_plot":
                scatter_plot_fig = plotter.plot_scatter_plot()
                print("Executing plot_scatter_plot technique")
                output_file = os.path.join(EDA_result_path, f"{technique}_result.html")
                # Save the result to the output file
                scatter_plot_fig.write_html(output_file)
                
            elif technique == "plot_line_plots":
                line_plot_fig = plotter.plot_line_plots()
                print("Executing plot_line_plots technique")
                output_file = os.path.join(EDA_result_path, f"{technique}_result.html")
                # Save the result to the output file
                line_plot_fig.write_html(output_file)
                
            elif technique == "plot_heatmap":
                heat_map_fig = plotter.plot_heatmap()
                print("Executing plot_heatmap technique")
                output_file = os.path.join(EDA_result_path, f"{technique}_result.html")
                # Save the result to the output file
                heat_map_fig.write_html(output_file)
            else:
                print(f"Invalid technique: {technique}")
                continue
            # # Save the result to the output file
            # result_data.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()

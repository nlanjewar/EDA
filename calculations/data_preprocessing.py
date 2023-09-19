import pandas as pd

class DataPreprocessor:
    def __init__(self, mapping_file, output_file):
        self.mapping_file = mapping_file
        self.output_file = output_file
        self.tag_mapping = None
        self.tag_output_df = None
        self.tag_output_new = None
        self.tag_names = None
    
    def read_mapping_file(self):
        mapping_df = pd.read_excel(self.mapping_file)
        self.tag_mapping = dict(zip(mapping_df['tag_id'], mapping_df['alias_name']))

    def read_output_file(self):
        self.tag_output_df = pd.read_excel(self.output_file)

    def rename_columns(self):
        self.tag_output_df.rename(columns=self.tag_mapping, inplace=True)

    def extract_timestamps(self):
        self.tag_output_df['timestamp'] = pd.to_datetime(self.tag_output_df['timestamp'])

    def remove_timestamp_column(self):
        self.tag_output_new = self.tag_output_df.drop(self.tag_output_df.columns[0], axis=1)

    def get_tag_names(self):
        self.tag_names = self.tag_output_new.columns[1:]
    
    def process_data(self):    
        self.read_mapping_file()
        self.read_output_file()
        self.rename_columns()
        self.extract_timestamps()
        self.remove_timestamp_column()
        self.get_tag_names()
        
    def handle_duplicates(self):
        for tag_name in self.tag_names:
            column_values = self.tag_output_new[tag_name]
            distinct_values = column_values.unique()
        if len(distinct_values) != len(column_values):
            print("Duplicate values found in column:", tag_name)
            duplicated_values = column_values[column_values.duplicated()]
            print("Duplicated data values:")
            print(duplicated_values)
            choice = input("Do you want to remove the duplicate data? (y/n): ")
            if choice.lower() == "y":
                self.tag_output_new = self.tag_output_new.drop_duplicates(subset=[tag_name], keep='first')
                print("Duplicate data removed.")
            else:
                print("Duplicate data not removed.")
        return self.tag_output_new  # Return the updated DataFrame

                    
    def clean_data(self):
        self.handle_duplicates()
        # Perform additional data cleaning operations as needed
        cleaned_data = self.tag_output_new.copy()
        cleaned_data.dropna(inplace=True)  # Remove rows with missing values
        cleaned_data.reset_index(drop=True, inplace=True)  # Reset the index
        return cleaned_data
        
    def impute_data(self):
        config_df = pd.read_csv(r"config/taglist.csv")
        for tag_name in self.tag_names:
            impute_method = config_df.loc[config_df['alias'] == tag_name, 'impute_method'].values[0]
            if impute_method == 'mean':
                self.tag_output_df[tag_name].fillna(self.tag_output_df[tag_name].mean(), inplace=True)
            elif impute_method == 'median':
                self.tag_output_df[tag_name].fillna(self.tag_output_df[tag_name].median(), inplace=True)
            elif impute_method == 'mode':
                self.tag_output_df[tag_name].fillna(self.tag_output_df[tag_name].mode().iloc[0], inplace=True)
            else:
                print(f"Invalid imputation method for tag '{tag_name}'. Please choose 'mean', 'median', or 'mode'.")
        return self.tag_output_df

    def remove_outliers_user_defined(self):
        config_df = pd.read_csv(r"config/taglist.csv")
        for tag_name in self.tag_names:
            min_value = config_df.loc[config_df['alias'] == tag_name, 'min_value'].values[0]
            max_value = config_df.loc[config_df['alias'] == tag_name, 'max_value'].values[0]
            filtered_data = self.tag_output_new.loc[(self.tag_output_new[tag_name] >= min_value) &
                                                (self.tag_output_new[tag_name] <= max_value)]
            removed_data = self.tag_output_new.loc[(self.tag_output_new[tag_name] < min_value) |
                                               (self.tag_output_new[tag_name] > max_value)]
            print("Tag:", tag_name)
            print("Removed Data:")
            print(removed_data)
            print("Filtered Data:")
            print(filtered_data)
            self.tag_output_new = filtered_data
            return filtered_data
    def remove_outliers_iqr(self):
        fixed_multiplier = 1.5  # Specify the fixed multiplier value
        for tag_name in self.tag_names:
            q1 = self.tag_output_new[tag_name].quantile(0.25)
            q3 = self.tag_output_new[tag_name].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - fixed_multiplier * iqr
            upper_bound = q3 + fixed_multiplier * iqr
            filtered_data = self.tag_output_new.loc[(self.tag_output_new[tag_name] >= lower_bound) &
                                                    (self.tag_output_new[tag_name] <= upper_bound)]
            removed_data = self.tag_output_new.loc[(self.tag_output_new[tag_name] < lower_bound) |
                                               (self.tag_output_new[tag_name] > upper_bound)]
            print("Tag:", tag_name)
            print("Removed Data:")
            print(removed_data)
            print("Filtered Data:")
            print(filtered_data)
            self.tag_output_new = filtered_data
            return filtered_data

    def remove_outliers_percentile(self):
        config_df = pd.read_csv(r"config/taglist.csv")
        for tag_name in self.tag_names:
            min_percentile = config_df.loc[config_df['alias'] == tag_name, 'min_percentile'].values[0]
            max_percentile = config_df.loc[config_df['alias'] == tag_name, 'max_percentile'].values[0]
            min_value = self.tag_output_new[tag_name].quantile(min_percentile)
            max_value = self.tag_output_new[tag_name].quantile(max_percentile)
            filtered_data = self.tag_output_new.loc[(self.tag_output_new[tag_name] >= min_value) &
                                                    (self.tag_output_new[tag_name] <= max_value)]
            removed_data = self.tag_output_new.loc[(self.tag_output_new[tag_name] < min_value) |
                                               (self.tag_output_new[tag_name] > max_value)]
            print("Tag:", tag_name)
            print("Removed Data:")
            print(removed_data)
            print("Filtered Data:")
            print(filtered_data)
            self.tag_output_new = filtered_data
            return filtered_data
    
    def remove_outliers_z_scores(self):
        config_df = pd.read_csv("config/taglist.csv")
        for tag_name in self.tag_names:
            std_dev = config_df.loc[config_df['alias'] == tag_name, 'std_dev'].values[0]
            z_scores = (self.tag_output_new[tag_name] - self.tag_output_new[tag_name].mean()) / self.tag_output_new[tag_name].std()
            filtered_data = self.tag_output_new.loc[abs(z_scores) <= std_dev]
            removed_data = self.tag_output_new.loc[abs(z_scores) > std_dev]
            print("Tag:", tag_name)
            print("Removed Data:")
            print(removed_data)
            print("Filtered Data:")
            print(filtered_data)
            self.tag_output_new = filtered_data
            return filtered_data
            
    
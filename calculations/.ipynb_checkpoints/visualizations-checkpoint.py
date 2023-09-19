import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

# Read the data from the Excel file, skipping the first column
tag_output_df = pd.read_excel(r"output/tag_output.xlsx")
tag_output_df.head()

tag_output_new = tag_output_df.drop(tag_output_df.columns[0], axis=1)
# tag_output_new = tag_output_df.drop(columns=['timestamp'])

print(tag_output_new)

# tag_output_new.info()

# Configure the histogram plot
tag_output_new.plot.hist(bins=5, alpha=0.5, edgecolor='black')

# Customize the plot
plt.xlabel("Value")
plt.ylabel("Frequency")
plt.title("Histogram")

# Display the plot
plt.show()












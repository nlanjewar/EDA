import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly.offline import plot

class DataPlotter:
    def __init__(self, tag_output_df, tag_output_new, tag_names):
        self.tag_output_df = tag_output_df
        self.tag_output_new = tag_output_new
        self.tag_names = tag_names
   
    def plot_histogram(self):
        # Read the config_data from the Excel file
        config_data = pd.read_excel("config/EDA_config.xlsx")

        # Find the first non-null value in the 'tag_name' column
        alias = None
        for _, row in config_data.iterrows():
            if pd.notnull(row['alias']):
                alias = row['alias']
                break

        # Check if a valid tag_name is found
        if alias is None:
            print("No valid alias found in the config_data.")
            return None

        # Get the data for the specified tag_name
        data = self.tag_output_df[alias]
        
        # Create a subplot with a single histogram
        fig = go.Figure()
        hist = go.Histogram(x=data, nbinsx=10, name='Histogram')
        fig.add_trace(hist)

        # Update layout
        fig.update_layout(
            xaxis=dict(title="Value"),
            yaxis=dict(title="Frequency"),
            title_text="Histogram for {}".format(alias)
            )
        return fig

    def plot_bar_chart(self):
        # Calculate the average values for each tag_name
        averages = self.tag_output_df[self.tag_names].mean()

        # Create the bar chart
        bar_chart = go.Bar(
            x=self.tag_names,  # X-axis labels
            y=averages,  # Y-axis values
        )

        # Create the layout
        layout = go.Layout(
            title="Average Values for tag_name columns",
            xaxis=dict(title="tag_name"),
            yaxis=dict(title="Average Value"),
        )

        # Create the figure
        fig = go.Figure(data=bar_chart, layout=layout)
        return fig

    def plot_box_plot(self):
        # Create a list to store box traces
        box_traces = []

        # Iterate over each tag_name column
        for tag_name in self.tag_names:
            # Create a box trace for the current tag_name column
            box_trace = go.Box(y=self.tag_output_new[tag_name], name=tag_name)
            box_traces.append(box_trace)

        # Create the layout
        layout = go.Layout(
            yaxis=dict(title="Value"),
            title="Box Plot for tag_name columns"
        )

        # Create the figure
        fig = go.Figure(data=box_traces, layout=layout)
        return fig

    def plot_scatter_plot(self):
        # Create a list to store scatter traces
        scatter_traces = []

        # Iterate over each tag_name column
        for tag_name in self.tag_names:
            # Create a scatter trace for the current tag_name column
            scatter_trace = go.Scatter(
                x=self.tag_output_new['timestamp'],
                y=self.tag_output_new[tag_name],
                mode='markers',
                name=tag_name
            )
            scatter_traces.append(scatter_trace)

        # Create the layout
        layout = go.Layout(
            xaxis=dict(title="Timestamp"),
            yaxis=dict(title="Value"),
            title="Scatter Plot for tag_name columns"
        )

        # Create the figure
        fig = go.Figure(data=scatter_traces, layout=layout)
        return fig

    def plot_line_plots(self):
        # Create a subplot with multiple line plots
        fig = make_subplots(rows=1, cols=1)

        # Iterate over the tag_names and plot the line charts
        for tag_name in self.tag_names[:2]:
            # Line plot for the current tag_name column
            line_trace = go.Scatter(
                x=self.tag_output_df['timestamp'],
                y=self.tag_output_df[tag_name],
                mode='lines',
                name=tag_name
            )
            fig.add_trace(line_trace)

        # Update layout
        fig.update_layout(
            xaxis=dict(title="Timestamp"),
            yaxis=dict(title="Value"),
            title="Comparison of Line Plots for tag_name columns"
        )
        return fig
        
    def plot_heatmap(self):
        # Calculate the correlation matrix
        correlation_matrix = self.tag_output_df[self.tag_names].corr()

        # Create the heatmap
        heatmap = go.Heatmap(
            z=correlation_matrix.values,  # Values for the heatmap
            x=self.tag_names,  # X-axis labels
            y=self.tag_names,  # Y-axis labels
            colorscale='Viridis',  # Colorscale for the heatmap
        )

        # Create the layout
        layout = go.Layout(
        title="Correlation Heatmap for tag_name columns",
        xaxis=dict(title="tag_name"),
        yaxis=dict(title="tag_name"),
        )

        # Create the figure
        fig = go.Figure(data=heatmap, layout=layout)
        return fig



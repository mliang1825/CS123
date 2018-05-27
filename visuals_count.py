from bokeh.plotting import figure, show, output_file
from bokeh.layouts import gridplot, row
from bokeh.models import ColumnDataSource
import pandas as pd
import numpy as np

'''
This file will do some basic visualizations with the output of pairing.py (called matchings.csv).
'''

df = pd.read_csv('matchings.csv')
df['category'] = df['categories'].apply(lambda x: str(x.strip('[').strip(']').split(',')[0]).strip('""'))
df['threshold'] = df['categories'].apply(lambda x: int(x.strip('[').strip(']').split(',')[1]))
df = df.sort_values(by='threshold')

df_asin = df[df['category'] == "asin"]
differences = np.array(df_asin['counts'][1:]) - np.array(df_asin['counts'][0:-1])
df_reviewer_id = df[df['category'] == "reviewerid"]

differences2 = np.array(df_reviewer_id['counts'][1:]) - np.array(df_reviewer_id['counts'][0:-1])

source_asin = ColumnDataSource(df_asin)
source_reviewer_id = ColumnDataSource(df_reviewer_id)
p1 = figure(title = 'Count of Same Product ID Below Each Threshold', x_axis_label = 'Threshold', y_axis_label = 'Counts')
p1.vbar(x = 'threshold',top = 'counts', source = source_asin, width = 0.5)

p2 = figure(title = 'Count of Same Reviewer ID Below Each Threshold', x_axis_label = 'Threshold', y_axis_label = 'Counts')
p2.vbar(x = 'threshold',top = 'counts', source = source_reviewer_id, width = 0.5, fill_color = 'red')

p3 = figure(title = 'Difference in Counts Between Thresholds Product ID', y_axis_label = 'Difference')
p3.vbar(x = [n for n in range(1,21)], top = differences, width = 0.5)

p4 = figure(title = 'Difference in Counts Between Thresholds Reviewer ID', y_axis_label = 'Difference')
p4.vbar(x = [n for n in range(1,21)], top = differences2, width = 0.5, fill_color = 'red')
grid = gridplot([[p1,p2],
                 [p3,p4]])

output_file('counts_viz.html', title = 'Threshold Counts')
show(grid)

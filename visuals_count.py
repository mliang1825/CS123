from bokeh.plotting import figure, show, output_file
from bokeh.layouts import gridplot, row
from bokeh.models import ColumnDataSource
import pandas as pd
import numpy as np

'''
This file will do some basic visualizations with the output of pairing.py (called matchings.csv).
'''

df_total_counts = pd.read_csv('total_thresh_counts.csv', names = ['threshold', 'counts'])
df_total_counts = df_total_counts.sort_values(by = 'threshold')

df = pd.read_csv('matchings.csv')
df['category'] = df['categories'].apply(lambda x: str(x.strip('[').strip(']').split(',')[0]).strip('""'))
df['threshold'] = df['categories'].apply(lambda x: int(x.strip('[').strip(']').split(',')[1]))
df = df.sort_values(by='threshold')

df_asin = df[df['category'] == "asin"]
df_reviewer_id = df[df['category'] == "reviewerid"]

differences = np.array(df_asin['counts'][1:]) - np.array(df_asin['counts'][0:-1])
differences2 = np.array(df_reviewer_id['counts'][1:]) - np.array(df_reviewer_id['counts'][0:-1])
differences3 = np.array(df_total_counts['counts'][1:]) - np.array(df_total_counts['counts'][0:-1])

source_asin = ColumnDataSource(df_asin)
source_reviewer_id = ColumnDataSource(df_reviewer_id)
source_total = ColumnDataSource(df_total_counts)

p1 = figure(title = 'Count of Same Product ID Below Each Threshold', x_axis_label = 'Threshold', y_axis_label = 'Counts')
p1.vbar(x = 'threshold',top = 'counts', source = source_asin, width = 0.5)

p2 = figure(title = 'Count of Same Reviewer ID Below Each Threshold', x_axis_label = 'Threshold', y_axis_label = 'Counts')
p2.vbar(x = 'threshold',top = 'counts', source = source_reviewer_id, width = 0.5, fill_color = 'red')

p3 = figure(title = 'Difference in Counts Between Thresholds Product ID', y_axis_label = 'Difference')
p3.vbar(x = [n for n in range(1,21)], top = differences, width = 0.5)

p4 = figure(title = 'Difference in Counts Between Thresholds Reviewer ID', y_axis_label = 'Difference')
p4.vbar(x = [n for n in range(1,21)], top = differences2, width = 0.5, fill_color = 'red')

p5 = figure(title = 'Count of Pairs Below Threshold', x_axis_label = 'Threshold', y_axis_label = 'Counts')
p5.vbar(x = 'threshold',top = 'counts', source = source_total, width = 0.5, fill_color = 'green')

p6 = figure(title = 'Difference in Counts Between Thresholds', y_axis_label = 'Difference')
p6.vbar(x = [n for n in range(1,21)], top = differences3, width = 0.5, fill_color = 'green')


grid = gridplot([[p5,p1,p2],
                 [p6,p3,p4]])

output_file('counts_viz.html', title = 'Threshold Counts')
show(grid)

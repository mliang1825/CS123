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
df_total_counts = df_total_counts[df_total_counts.threshold != 2].set_index('threshold').round(10)

df = pd.read_csv('matchings.csv')
df['category'] = df['categories'].apply(lambda x: str(x.strip('[').strip(']').split(',')[0]).strip('""'))
df['threshold'] = df['categories'].apply(lambda x: int(x.strip('[').strip(']').split(',')[1]))
df = df.sort_values(by='threshold')

df_asin = df[df['category'] == "asin"][['counts','threshold']].set_index('threshold').round(10)
df_reviewer_id = df[df['category'] == "reviewerid"][['counts','threshold']].set_index('threshold').round(10)

df_asin_p = df_asin.join(df_total_counts,lsuffix= '_asin', rsuffix = '_total')
df_reviewer_id_p = df_reviewer_id.join(df_total_counts,lsuffix = '_reviewer_id',rsuffix = '_total')

df_asin_p['percents'] = (df_asin_p['counts_asin'] / df_asin_p['counts_total'])
df_asin_p['threshold'] = df_asin_p.index
df_reviewer_id_p['percents'] = (df_reviewer_id_p['counts_reviewer_id'] / df_reviewer_id_p['counts_total'])
df_reviewer_id_p['threshold'] = df_reviewer_id_p.index

source_asin = ColumnDataSource(df_asin_p)
source_reviewer_id = ColumnDataSource(df_reviewer_id_p)

p1 = figure(title = 'Percentage of Pairs Below Thresholds with Same Product ID', x_axis_label = 'Threshold', y_axis_label = 'Percent')
p1.vbar(x = df_asin_p.threshold, top = df_asin_p.percents, width = 0.5)

p2 = figure(title = 'Percentage of Pairs Below Thresholds with Same Reviewer ID', x_axis_label = 'Threshold', y_axis_label = 'Percents')
p2.vbar(x = df_reviewer_id_p.threshold,top = df_reviewer_id_p.percents, width = 0.5, fill_color = 'red')

grid = gridplot([[p1,p2]])

output_file('percents_viz.html', title = 'Percetanges of Pairs with Matching Product ID or Reviewer ID')
show(grid)

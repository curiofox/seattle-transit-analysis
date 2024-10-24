

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mpldates
import seaborn as sns
import plotly as py
import dill
import os

#######################################
# Import from GTFS CSVs
#######################################

df_GTFS = ["agency",
              "calendar",
              "calendar_dates",
              "fare_attributes",
              "fare_rules",
              "routes",
              "shapes",
              "stop_times",
              "stops",
              "trips",
             ]


GTFS_path_location = "King_County_GTFS_feed/"
for df in df_GTFS:
    exec(f"{df} = pd.read_csv('../{GTFS_path_location}{df}.txt')")


calendar["start_date"] = pd.to_datetime(calendar["start_date"], format = "%Y%m%d")
calendar["end_date"] = pd.to_datetime(calendar["end_date"], format = "%Y%m%d")
calendar_dates["date"] = pd.to_datetime(calendar_dates["date"], format = "%Y%m%d")


import GTFS_Functions as my


#######################################
# Combine Calendar dataframe
#######################################

calendar_full = my.create_calendar_full(calendar, calendar_dates)
calendar_full = calendar_full.pipe(my.add_calendar, calendar = calendar) \
    .pipe(my.add_calendar_dates, calendar_dates = calendar_dates)

calendar_week = my.create_calendar_week(calendar_full, binary = True)
calendar_week_mean = my.create_calendar_week(calendar_full, binary = False)

#######################################
# Add route time delta
#######################################
#
# store time deltas in the 'stop_times'
# using stop_time to datetime AND create_time_delta
#
# Time delta is
# for each stop,
# for each day of week
# per route,
# per direction
# from trip to next trip
#

# Do I want a more generalized tool for time delta?
# A tool for how often a trip is available between two stops?
#
# schedule overlay??

# merge data together
stop_freq = my.create_stop_freq(trips, stop_times, routes, calendar_week)
# Convert 'arrival_time' and 'departure_time' from string to datetime
stop_freq['arrival_time'] = pd.to_timedelta(stop_freq['arrival_time'])
stop_freq['departure_time'] = pd.to_timedelta(stop_freq['departure_time'])

#stop_freq = stop_freq.loc[stop_freq['stop_id'] == 67655, :]
#stop_freq = stop_freq.iloc[slice(0,100_000), :].copy()

#'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
    smaller = stop_freq.loc[stop_freq[day] == 1, ['stop_id', 'route_id', 'direction_id', 'arrival_time']].copy()
    grouped = smaller.sort_values('arrival_time') \
        .groupby(['stop_id', 'route_id', 'direction_id'], sort=False, as_index=False)

    output = grouped.apply(my.create_time_df)
    output.columns = [f'time_f_{day}']

    output.index = output.index.get_level_values(1)

    stop_freq = pd.merge(left=stop_freq, right=output, how='left', left_index=True,
                         right_index=True)  # .sort_values(['stop_id', 'route_id','direction_id','arrival_time']).loc[:,["arrival_time","time_d",'stop_id','route_id','direction_id']]
    #stop_freq.loc[:,[f'time_f_{day}']].fillna(value = 0, inplace = True)


#######################################
# Send everything into pickles
#######################################

df_pickles = df_GTFS + ["calendar_full",
                        "calendar_week",
                        "calendar_week_mean",
                        "stop_freq"
                       ]

for df in df_pickles:
    locals()[df].to_pickle(f"./pickles/{df}.pkl")
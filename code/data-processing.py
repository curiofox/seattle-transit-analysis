

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mpldates
import seaborn as sns
import plotly as py
import dill
import os



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

calendar_full = my.create_calendar_full(calendar, calendar_dates)
calendar_full = calendar_full.pipe(my.add_calendar, calendar = calendar) \
    .pipe(my.add_calendar_dates, calendar_dates = calendar_dates)

calendar_week = my.create_calendar_week(calendar_full, binary = True)
calendar_week_mean = my.create_calendar_week(calendar_full, binary = False)



df_pickles = df_GTFS + ["calendar_full",
                        "calendar_week",
                        "calendar_week_mean"
                       ]

for df in df_pickles:
    locals()[df].to_pickle(f"./pickles/{df}.pkl")
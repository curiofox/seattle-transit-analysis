import pandas as pd


##############################################################
# Calendar construction functions
#
##############################################################


def create_calendar_full(calendar, calendar_dates):
    """
    creates a dataframe with:
    Columns:
        [0] service_id
        [1:-1] date-time objects, each date from earliest date to latest date in Calendar
    Index:
        integers from 0
    Values:
        - service ID is filled with all unique values from calendar and calendar_dates
        - all other values are NaN

    """
    # Create variable calendar_full_columns
    # Is a series with every date (in date time) from the first date available in calendar to the last
    # Calendar should have the full range of dates covered by the database
    cal_range_max = calendar.loc[:, ["start_date", "end_date"]].max().max()
    cal_range_min = calendar.loc[:, ["start_date", "end_date"]].min().min()
    cal_range = pd.date_range(start=cal_range_min, end=cal_range_max, inclusive="both")
    calendar_full_columns = pd.Series(["service_id"] + list(cal_range))

    # Create variable service_id_list
    # Is a series with every service id in calendar and calendar_dates
    # filtered for only unique
    service_id_list = pd.merge(calendar.loc[:, ["service_id"]], calendar_dates.loc[:, ["service_id"]], how="outer")
    service_id_list.drop_duplicates(inplace=True)
    service_id_list.reset_index(inplace=True, drop=True)

    # Create dataframe
    calendar_full = pd.DataFrame(data=service_id_list, columns=calendar_full_columns)

    return calendar_full


def add_calendar(calendar_full, calendar):
    """
    assumes calendar_full's index is intergers indexed by 1
    assumes calendar_full's columns are as such:
      [0] service_id
      [1:-1] date-time objects

    """
    # iterate through entire calendar_full, sans service_id column
    # where i is index number, and j is column number
    for i in range(0, calendar_full.index.max() + 1):
        for j in range(1, len(calendar_full.columns)):
            # Target in Calendar, based on numerical index/column of calendar_full
            target_cal_index = calendar["service_id"] == calendar_full["service_id"][i]
            target_cal_column = calendar_full.columns[j].day_name().lower()
            calendar_full.iloc[i, j] = calendar.loc[target_cal_index, target_cal_column]

    return calendar_full


def add_calendar_dates(calendar_full, calendar_dates):
    """
    assumes calendar_full's index is intergers indexed by 1
    assumes calendar_full's columns are as such:
      [0] service_id
      [1:-1] date-time objects

    assumes calendar_dates only has unique combinations or service_id + date.
    this function will ignore non-unique combinations

    """
    # iterate through entire calendar_full, sans service_id column
    # where i is index number, and j is column number
    for i in range(0, calendar_full.index.max() + 1):
        for j in range(1, len(calendar_full.columns)):
            # find index in calendar_dates that matches a particular cell in calendar_full
            # target index is a series of booleans
            target_service_id = calendar_dates["service_id"] == calendar_full.iloc[i, 0]
            target_date = calendar_dates["date"] == calendar_full.columns[j]
            target_index = target_service_id & target_date
            # check to make sure there is a target (target_index has one True value)
            if target_index.sum() == 1:
                # exception is value of exception type (as per data dictionary, is only 1 or 2)
                exception = calendar_dates.loc[target_index, "exception_type"].iloc[0]
                if exception == 1:
                    # service added
                    calendar_full.iloc[i, j] = 1
                elif exception == 2:
                    # service removed
                    calendar_full.iloc[i, j] = 0
    return calendar_full


def cal_full_alter_axis(x):
    """
    Pull service id out of the columns and use it as the index
    """
    x = x.set_index("service_id").copy()
    return x


def create_calendar_week(calendar_full, binary=False):
    """
    create DF calendar_week.
    It's in the same format as "calendar"
    values are averages

    If binary is True a field of binary values are returned.
    Where the values are the averages round to 0 or 1
    """
    calendar_week = pd.DataFrame(data=calendar_full["service_id"],
                                 columns=["service_id"]
                                 )

    weekday_list = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    testy_test = 1
    testy_test = testy_test+1


    for day in weekday_list:
        # And then add the two edge dates
        filter_values = [True] + [True if x.day_name().lower() == day else False for x in calendar_full.columns[1:]]
        # create DF of serviceid plus only days of specified week
        filtered_cal = calendar_full.loc[:, filter_values]
        # average day of week
        add_day = filtered_cal.iloc[:, 1:].T.groupby(lambda x: True).mean().T
        if binary == True:
            add_day = add_day.map(lambda x: round(x))
        add_day.columns = [day]
        calendar_week[day] = add_day

    calendar_week["start_date"] = [calendar_full.columns[1]] * calendar_week.shape[0]
    calendar_week["end_date"] = [calendar_full.columns[-1]] * calendar_week.shape[0]

    return calendar_week




##############################################################
# Trip Analysis Functions
#
##############################################################

def trips_merged(trips, stop_times, routes, calendar_week):
    """

    """
    # pull stop times
    stop_times["index_og"] = stop_times.index
    stop_times_index = stop_times.groupby(["trip_id"])[["arrival_time", "index_og"]].min("arrival_time")["index_og"]
    trip_times = stop_times.loc[stop_times_index, ["trip_id", "arrival_time"]]

    # pull route names
    trip_routes = routes.loc[:, ["route_id", "route_short_name", "route_desc"]]

    # pull service_weekdays
    # just drop it in

    trips_alt = pd.merge(left=trips, right=trip_times, how="inner", on="trip_id")

    trips_alt = pd.merge(left=trips_alt, right=trip_routes, how="inner", on="route_id")

    trips_alt = pd.merge(left=trips_alt, right=calendar_week, how="inner", on="service_id")

    return trips_alt


def routes_on_a_day(trips, weekday, route, direction):
    return trips.loc[((trips["route_short_name"] == route) &
                      (trips[weekday] == 1) &
                      (trips["direction_id"] == direction)),
           :].sort_values("arrival_time")


def stop_time_to_datetime(value):
    time = value
    hour = int(time[0:2])
    if hour > 23:
        day = "1970-01-02"
        hour = hour - 24
        time = str(hour) + time[2:-1]
    else:
        day = "1970-01-01"
    return pd.to_datetime((day + " " + time))


def create_time_delta(df, time_col, delta_col):
    """
    outputs the same df, with a new column, delt_col

    the timedelta for each point is the time elapsed BEFORE the point.
    delta = Tn - T(n-1)

    df: dataframe with the datetime to build from
        dataframe the delta time will be added to

    time_col: name of the column with datetime information.
              must be in timestamp format

    delta_col: the name of the new column
    """
    one_day = pd.to_timedelta(1, unit='d')

    delta_df = pd.DataFrame(index=df.index, columns=[delta_col])
    delta_df[delta_col] = df[time_col].diff()
    delta_df.iloc[0, 0] = df[time_col].iloc[0] - (df[time_col].iloc[-1] - one_day)
    return delta_df


    # # Create a column of the differences
    # df[delta_col] = df[time_col].diff()
    #
    # # create a timedelta object equal to one day
    # one_day = pd.to_timedelta(1, unit='d')
    #
    # # calculate the time differential between the last and the first value.
    # df.loc[df.index[0], delta_col] = \
    #     df[time_col].iloc[0] - \
    #     (df[time_col].iloc[-1] - one_day)
    #
    # return df


def create_time_df(df):
    one_day = pd.to_timedelta(1, unit='d')

    delta_df = df.diff()
    delta_df.iloc[0, 0] = df.iloc[0,0] - (df.iloc[-1,0] - one_day)
    return delta_df


##############################################################
# Route Frequency Analysis
#
##############################################################

def create_stop_freq(trips, stop_times, routes, calendar_week):
    routes_filtered = routes.loc[:, ['route_id', 'route_short_name', 'route_desc', 'route_url']]
    calendar_week_filtered = calendar_week.drop(labels=['start_date', 'end_date'], axis=1)
    trips_filtered = trips.loc[:, ['trip_id',
                                   'route_id',
                                   'service_id',
                                   'trip_headsign',
                                   'direction_id',
                                   'shape_id']]
    trips_filtered = pd.merge(left=trips_filtered, right=routes_filtered, how="inner", on="route_id")
    trips_filtered = pd.merge(left=trips_filtered, right=calendar_week_filtered, how='inner', on='service_id')
    stop_freq = pd.merge(left=stop_times, right=trips_filtered, how="inner", on="trip_id")
    return stop_freq
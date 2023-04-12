from check import check_installed
from merge_processes import *


node_exist, libv_exist, win_exist = check_installed()
len_node, len_lib = give_len()
# get time_limit
# prometheus can't go over 11000 data using request for longer time periods time period must be divided
time = int(step[0:-1])
if step[len(step)-1] == "m":
    time *= 60
if step[len(step)-1] == "h":
    time *= 60*60

temp1, temp2, temp3, temp4, time_limit = time_div_step(day, hour, minute,second_in, time)
del temp1, temp2, temp3, temp4

# make day 0 to prevent clutter
hold_day = 0
hold_hour = hour+24*day
hold_minute = minute
# day w day-hold1
print(len_node)
now = datetime.now()
# 0,1,2,3...,divider-1, runs divider times to run for different servers
for counted_time_divs in range(len_node):
    crap_bool = True
    # make day 0 to prevent clutter
    hold_day = 0
    hold_hour = hour + 24 * day
    hold_minute = minute
    hold_sec = second_in
    start = True
    for count_time in range(time_limit):
        # get divided time periods and go back as them, give date as it
        day1, hour1, minute1, sec1, time_div = time_div_step(hold_day, hold_hour, hold_minute, hold_sec, time)
        # get start, end dates of time interval divisions
        print(day1, hour1, minute1, sec1, hold_sec)
        hold_small_step = time
        if start==True:
            hold_small_step = 0
            start = False
        print(hold_small_step, time)
        start, end = give_default_dates(now, day_back=hold_day, hour_back=hold_hour, min_back=hold_minute,
                                        sec_back=(hold_sec - hold_small_step), end_recent_day=hold_day,
                                        end_recent_hour=hold_hour - hour1, end_recent_min=hold_minute - minute1,
                                        end_recent_sec=hold_sec-sec1)
        print(start,end)
        node_data,titles_node = prepare_node(start,end,step,step_func,counted_time_divs)
        node_df = pd.DataFrame(node_data,columns=titles_node)
        print(node_df)
        # node_df = pd.DataFrame(node_data)
        node_df["time_stamp"] = node_df.apply(lambda x: datetime.fromtimestamp(int(x["time_stamp"])), axis=1)
        if crap_bool:
            hold_data = node_df
            crap_bool = False

        else:
            hold_data = pd.concat((hold_data,node_df),axis=0,ignore_index=True)

        hold_day = day1
        hold_hour = hold_hour - hour1
        hold_minute = hold_minute - minute1
        hold_sec -= sec1
    try:
        print(hold_data.shape)
        hold_data.to_csv(f"out/node_data_{counted_time_divs}th.csv")
    except:

        pass

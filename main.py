import pandas as pd
from organizer import *
import json
import requests as rq
import numpy as np
from check import check_installed


node_exist, libv_exist, win_exist = check_installed()
len_node, len_lib = give_len()

day = 0
hour = 1
minute = 0
# define default step and query function step
step = "20s"
step_func = "30s"

def main(start,end,step,step_func,server,node_exist,lib_exist,win_exist):

    # load csv data into dataframes
    #df = pd.read_csv('all_queries.csv')
    df_nodes = pd.read_csv("node_queries.csv")
    # create a list to contain log infos
    log = []
    # create a list to store the successfully reached data's query names
    columns_node = ["time stamp"]
    # create a list to store query names of data who had buffer error
    columns_node_buffer_error = []
    # crap booleans to store initial value in loop
    execute_once = True
    execute_once_buffer_err = True

    # iterate node queries
    for name, col in df_nodes.iterrows():

        # get query names and queries
        query_name = col["query_name"]
        query = col["query"]
        # get instance to run curly organizer function
        instance = "{instance=" + f'{return_instance("node", st_num=server)}' + "}"

        # run curly organizer
        url = curly_organizer(query, instance, step_func)

        # organize url(request) to prevent clutter
        url = organize_url(url, start, end, step)

        # get data using requests modul
        metric_data_node = rq.get(url).json()
        # check if any error happen
        try:
            # check if there is node metric data
            if len(metric_data_node['data']['result']) == 0:
                log.append(str(datetime.now()) + "\tnode -> " + query_name)
                continue

        except:

            print("Potential time error. Please check if start and end times relevant.")
            log.append("an error occured:\tERROR IN MAIN LOOP! ")
            continue

        # load metric data into a numpy array
        metric_data_node = np.array(metric_data_node['data']['result'][0]['values'])

        # select metric and time stamp data and get them ready for applying numpy's concatenate module
        metric_node = metric_data_node[:, 1][np.newaxis]
        time_stamp = metric_data_node[:, 0][np.newaxis]

        # hold first data and to merge with metric data
        if execute_once:
            collected_node_data = np.concatenate((time_stamp.T, metric_node.T), axis=1)
            execute_once = False
            columns_node.append(query_name)

        else:
            # try to merge new metric data and old hold data
            try:
                collected_node_data = np.concatenate((collected_node_data, metric_node.T), axis=1)
            # if shape of arrays are different(when buffer latency) there is an error
            except:
                # sometimes for some queries a few data can't be collected, here it is fixed by doing similar things
                columns_node_buffer_error.append(query_name)
                if execute_once_buffer_err:
                    collected_node_data_buffer_err = metric_node.T
                    execute_once_buffer_err = False
                else:
                    collected_node_data_buffer_err = np.concatenate((collected_node_data_buffer_err, metric_node.T), axis=1)

    # get libvirt virtual machines
    devices = reach_device(start, end)
    # get(load into a df) queries and query names of csv file
    df_libvs = pd.read_csv("libvirt_queries.csv")
    # iterate libvirt queries
    columns_libv = []
    execute_for_initial_col = True
    execute_once_initial_block = True
    
    if libv_exist:
        for name, col in df_libvs.iterrows():

            # get query names and queries
            query_name = col["query_name"]
            query = col["query"]
            # a counter for loop for VMs(devices in instance)
            in_count = 1

            # processes of devices taken by a function
            for device in devices:
                # assign instance to run curly organizer function
                instance = "{instance=" + f'{return_instance("libvirt",st_num=server)}'+ ",domain=" + f'"{device}"'+"}"
                # run curly organizer delete instance and replace it
                url = curly_organizer(query, instance, step_func)
                # get data after organizing the url
                url = organize_url(url, start, end, step)

                # get data using requests modul
                metric_data_libv = rq.get(url).json()

                try:
                    if len(metric_data_libv['data']['result']) == 0:
                        now = str(datetime.now())

                    else:
                        data = metric_data_libv['data']['result'][0]['values']
                        # load data into a numpy array
                        data = np.array(data)
                        # get metric values
                        metric_libv = data[:, 1][np.newaxis]
                        # get time stamp values
                        time_stamp_libv = data[:, 0][np.newaxis]
                        # for executing for 4 times

                        if execute_for_initial_col:
                            # to create following structure:
                            """ time stamp: ts, metric = m
                            ts (connect) m = a                          a
                                                                    (connect)                   
                            ts (connect) m = b                          b        =  PART
                                                                    (connect)
                            ts (connect) m = c                          c
                            """
                            if in_count == len(devices):
                                execute_for_initial_col = False
                                # 3 vertical, 4 horizontal connections done
                            # horizontally connect
                            temp_data = np.concatenate((time_stamp_libv.T, metric_libv.T), axis=1)

                            # execute for once for first element to connect second element
                            if execute_once_initial_block:
                                libv_processed_data = temp_data
                                execute_once_initial_block = False
                                columns_libv.append(query)
                            # vertically connect horizontally connected elements
                            else:
                                saves = np.concatenate((libv_processed_data, temp_data), axis=0)
                            # if devices' loops' end is reached, add connected metrics
                            if in_count == len(devices):
                                libv_processed_data = np.concatenate((libv_processed_data, saves), axis=1)
                                # append queries into titles
                                columns_libv.append(query_name)

                # catch if anything goes with time
                except:
                    print("Potential time error. Please check if start and end time relevant. ")
                    log.append("an error occured: \t" + str(datetime.now()) + "\t ERROR IN MAIN LOOP!")
                # if there is data, go on

                # increment at the end of devices loop
                in_count += 1

    return collected_node_data, libv_processed_data, columns_node

# get time_limit
# prometheus can't go over 11000 data using request for longer time periods time period must be divided
time = int(step[0:-1])
temp1, temp2, temp3, temp4, time_limit = time_div_step(day, hour, minute, time)
del temp1, temp2, temp3, temp4

# make day 0 to prevent clutter
hold_day = 0
hold_hour = hour+24*day
hold_minute = minute
# day w day-hold1

# 0,1,2,3...,divider-1, runs divider times
for counted_time_divs in range(len_node):
    crap_bool = True
    # make day 0 to prevent clutter
    hold_day = 0
    hold_hour = hour + 24 * day
    hold_minute = minute
    for count_time in range(time_limit):
        # get divided time periods and go back as them, give date as it
        day, hour, minute, sec, time_div = time_div_step(hold_day, hold_hour, hold_minute, time)
        # get start, end dates of time interval divisions
        start, end = give_default_dates(day_back=hold_day, hour_back=hold_hour, min_back=hold_minute,
                                        end_recent_day=hold_day,
                                        end_recent_hour=hold_hour - hour, end_recent_min=hold_minute - minute)

        node_data,libv_data,titles_node = main(start,end,step,step_func,counted_time_divs,node_exist,libv_exist,win_exist)
        node_df = pd.DataFrame(node_data,columns=titles_node)
        # node_df = pd.DataFrame(node_data)
        node_df["time_stamp"] = node_df.apply(lambda x: datetime.fromtimestamp(int(x["time_stamp"])), axis=1)

        libv_df = pd.DataFrame(libv_data)

        if crap_bool:
            hold_data = node_df
            crap_bool = False
            hold_libv = libv_df

        else:

            hold_libv = pd.concat((hold_libv, libv_df), axis=0, ignore_index=True)
            hold_data = pd.concat((hold_data,node_df),axis=0,ignore_index=True)

        hold_day = day
        hold_hour = hold_hour - hour
        hold_minute = hold_minute - minute

    try:
        hold_data.to_csv(f"out/node_data_{counted_time_divs}th.csv")
    except:

        pass

from reach_time import *


def check_installed():

    node_query_exist = False
    libvirt_query_exist = False
    win_query_exist = False

    query_node = "node_load1"
    query_libvirt = "libvirt_domain_block_stats_allocation"
    query_win = "windows_cs_hostname"

    url_node = f"http://localhost:9090/api/v1/query?query={query_node}"
    url_libvirt = f"http://localhost:9090/api/v1/query?query={query_libvirt}"
    url_windows = f"http://localhost:9090/api/v1/query?query={query_win}"

    try:
        data_node = rq.get(url_node).json()
        data_libvirt = rq.get(url_libvirt).json()
        data_win = rq.get(url_windows).json()

        if data_node['status'] == 'success':
            node_query_exist = True

        if data_libvirt['status'] == 'success':
            libvirt_query_exist = True

        if data_win['status'] == 'success':
            win_query_exist = True

    except:
        pass

    return node_query_exist, libvirt_query_exist, win_query_exist

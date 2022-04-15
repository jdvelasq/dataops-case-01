import os
import random
from datetime import datetime

import pandas as pd


def process_next_week():

    rdbms_requests_table = load_rdbms_requests_table()
    historical_requests_table = load_historial_requests_table()
    last_procesed_date = rdbms_requests_table.open_date.tail(1).values[0]

    batch_data = historical_requests_table[
        historical_requests_table.open_date > last_procesed_date
    ]
    batch_data = select_next_week(batch_data)
    batch_data = assign_last_modified_field(batch_data)

    rdbms_requests_table = pd.concat([rdbms_requests_table, batch_data])
    rdbms_requests_table = process_rdbms_request_table(rdbms_requests_table)
    overwrite_rdbms_requests_table(rdbms_requests_table)

    print(rdbms_requests_table.loc[batch_data.index, :])


def select_next_week(batch_data):
    batch_data = batch_data.copy()
    current_date = batch_data.open_date.head(1).values[0]
    next_date = pd.to_datetime(current_date) + pd.Timedelta(days=7)
    next_day = next_date.strftime("%A").lower()
    next_date = next_date.strftime("%Y-%m-%d")
    while next_day != "monday":
        next_date = compute_next_day(next_date)
        next_day = pd.to_datetime(next_date).strftime("%A").lower()
    batch_data = batch_data[batch_data.open_date < next_date]
    return batch_data


def restart():
    historial_requests_table = load_historial_requests_table()
    requests_table = select_initial_request_table(historial_requests_table)
    requests_table = process_rdbms_request_table(requests_table)
    overwrite_rdbms_requests_table(requests_table)
    print(requests_table)


def process_rdbms_request_table(table):
    current_date = get_init_business_date(table)
    last_date = table.open_date.tail(1).values[0]
    while current_date <= last_date:
        table = process_current_date(table, current_date)
        current_date = compute_next_day(current_date)
    return table


def process_current_date(table, current_date):

    batch_data = table[table.status != "closed"].copy()
    batch_data = batch_data[batch_data.open_date <= current_date]

    daily_assign_capacity = random.randint(int(80), int(100))
    max_in_progress_capacity = random.randint(8 * int(80), 8 * int(100))

    #
    # open ---> assigned (current day)
    #
    open_tasks = batch_data[batch_data.status == "open"]
    n = min(daily_assign_capacity, len(open_tasks))
    indexes = open_tasks.index.values[:n]
    batch_data.loc[indexes, "status"] = "assigned"
    batch_data.loc[indexes, "assigned_date"] = current_date

    #
    # in progress ---> closed (current day)
    #
    in_progress_tasks = batch_data[batch_data.status == "in progress"]
    batch_data.loc[in_progress_tasks.index, "age"] -= 1
    batch_data.loc[batch_data.age == 0, "status"] = "closed"
    batch_data.loc[batch_data.age == 0, "closed_date"] = current_date

    #
    # assigned ---> in progress (current day)
    #
    in_progress_tasks = batch_data[batch_data.status == "in progress"]
    n = min(max_in_progress_capacity - len(in_progress_tasks), len(in_progress_tasks))
    assigned_tasks = batch_data[batch_data.status == "assigned"]
    n = max(n, len(assigned_tasks))
    indexes = assigned_tasks.index.values[:n]
    batch_data.loc[indexes, "status"] = "in progress"
    batch_data.loc[indexes, "in_progress_date"] = current_date

    #
    # copy data
    #
    table.loc[batch_data.index, "status"] = batch_data.status.values
    table.loc[batch_data.index, "assigned_date"] = batch_data.assigned_date.values
    table.loc[batch_data.index, "in_progress_date"] = batch_data.in_progress_date.values
    table.loc[batch_data.index, "closed_date"] = batch_data.closed_date.values
    table.loc[batch_data.index, "age"] = batch_data.age.values

    return table


def get_init_business_date(table):
    table = table.copy()
    assigned_date = table.assigned_date.dropna()
    if len(assigned_date) == 0:
        assigned_date = table.open_date.head().values[0]
        assigned_date = repair_business_day(assigned_date)
    else:
        assigned_date = assigned_date.tail(1).values[0]
        assigned_date = compute_next_day(assigned_date)
    return assigned_date


def repair_business_day(date):
    date = pd.to_datetime(date)
    day_name = date.strftime("%A").lower()
    if day_name == "saturday":
        date = date + pd.Timedelta(days=2)
    elif day_name == "sunday":
        date = date + pd.Timedelta(days=1)
    date = date.strftime("%Y-%m-%d")
    return date


def compute_next_day(date):
    date = pd.to_datetime(date) + pd.Timedelta(days=1)
    date = repair_business_day(date)
    return date


def load_rdbms_requests_table():
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "../operational_rdbms/requests_table.csv")
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    data = pd.read_csv(filename, sep=",")
    return data


def overwrite_rdbms_requests_table(data):
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "../operational_rdbms/requests_table.csv")
    data.to_csv(filename, sep=",", index=False)


def select_initial_request_table(historical_request_table):
    historical_request_table = historical_request_table.copy()
    rdbms_request_table = historical_request_table[
        historical_request_table.open_date <= "2017-09-14"
    ]
    rdbms_request_table = assign_last_modified_field(rdbms_request_table)
    return rdbms_request_table


def assign_last_modified_field(table):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    table = table.assign(last_modified=now)
    return table


def load_historial_requests_table():
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, "historical_requests_table.csv")
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    data = pd.read_csv(filename, sep=",")
    return data

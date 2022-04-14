import os
from datetime import datetime

import pandas as pd


def main():
    simulate("letter")
    simulate("web")


def simulate(channel):
    historical_requests = get_historical_requests(channel)
    rdbms_table = get_rdbms_table(channel)
    batch_data = get_batch_data(historical_requests, rdbms_table)
    rdbms_table = pd.concat([rdbms_table, batch_data])
    rdbms_table = update_status(rdbms_table)
    save_rdbms_table(rdbms_table, channel)

    print(f"{channel} simulation finished")
    print(rdbms_table.tail(5))
    print()


def save_rdbms_table(table, channel):
    module_path = os.path.dirname(__file__)
    filename = os.path.join(
        module_path, f"../operational_rdbms/rdbms_{channel}_table.csv"
    )
    table.to_csv(filename, index=False)


def update_status(rdbms_table):
    rdbms_table = rdbms_table.copy()
    current_date = rdbms_table.in_date.tail(1).values[0]
    rdbms_table["status"] = rdbms_table["out_date"].map(
        lambda x: "closed" if x <= current_date else "open"
    )
    return rdbms_table


def get_batch_data(historical_requests, rdbms_table):

    data = historical_requests[
        historical_requests.in_date > rdbms_table.in_date.tail(1).values[0]
    ]

    init_index = data.index[0]
    end_index = init_index
    is_first_monday = True
    for index in data.index:
        if data.day_name[index] == "Monday":
            if is_first_monday is True:
                is_first_monday = False
            else:
                break
        end_index = index

    batch_data = data.loc[init_index:end_index, :]

    return batch_data


def get_rdbms_table(channel):
    module_path = os.path.dirname(__file__)
    rdbms_table_file = os.path.join(
        module_path, f"../operational_rdbms/rdbms_{channel}_table.csv"
    )
    if not os.path.exists(rdbms_table_file):
        raise FileNotFoundError(f"File {rdbms_table_file} not found")
    rdbms_table = pd.read_csv(rdbms_table_file, sep=",")
    rdbms_table = rdbms_table.fillna("NULL")
    return rdbms_table


def get_historical_requests(channel):
    module_path = os.path.dirname(__file__)
    historical_requests_file = os.path.join(
        module_path, f"historical_requests_{channel}.csv"
    )
    if not os.path.exists(historical_requests_file):
        raise FileNotFoundError(f"File {historical_requests_file} not found")
    historical_requests = pd.read_csv(historical_requests_file, sep=",")
    historical_requests = historical_requests.fillna("NULL")
    return historical_requests


if __name__ == "__main__":
    main()

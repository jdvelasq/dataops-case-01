import os
from datetime import datetime

import pandas as pd


def main():
    process_channel("letter")
    process_channel("web")


def process_channel(channel):
    data = load_requests_file(channel)
    data = select_data(data)
    save_to_rdbms_table(data, channel)


def select_data(data):
    data = data.copy()
    data = data[data.in_date <= "2017-09-14"]
    data["status"] = data["out_date"].map(
        lambda x: "closed" if x <= "2017-09-14" else "open"
    )
    return data


def save_to_rdbms_table(data, channel):
    module_path = os.path.dirname(__file__)
    filename = os.path.join(
        module_path, f"../operational_rdbms/rdbms_{channel}_table.csv"
    )
    data.to_csv(filename, index=False)


def load_requests_file(channel):
    module_path = os.path.dirname(__file__)
    filename = os.path.join(module_path, f"historical_requests_{channel}.csv")
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    data = pd.read_csv(filename, sep=",")
    data["out_date"] = data["out_date"].fillna("NULL")
    return data


if __name__ == "__main__":
    main()

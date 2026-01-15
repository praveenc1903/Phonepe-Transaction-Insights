import os
import json
import pandas as pd

BASE_PATH = "pulse-data/data"


# ===================== COMMON CLEANING =====================
def clean_states(df):
    df["States"] = df["States"].str.replace("andaman-&-nicobar-islands", "Andaman & Nicobar")
    df["States"] = df["States"].str.replace("-", " ")
    df["States"] = df["States"].str.title()
    df["States"] = df["States"].str.replace(
        "Dadra & Nagar Haveli & Daman & Diu",
        "Dadra and Nagar Haveli and Daman and Diu"
    )
    return df


# ===================== AGGREGATED TRANSACTION =====================
agg_trans = {
    "States": [], "Years": [], "Quarter": [],
    "Transaction_type": [], "Transaction_count": [], "Transaction_amount": []
}

path = f"{BASE_PATH}/aggregated/transaction/country/india/state/"
for state in os.listdir(path):
    for year in os.listdir(f"{path}{state}"):
        for file in os.listdir(f"{path}{state}/{year}"):
            with open(f"{path}{state}/{year}/{file}") as f:
                data = json.load(f)
                for i in data["data"]["transactionData"]:
                    agg_trans["States"].append(state)
                    agg_trans["Years"].append(year)
                    agg_trans["Quarter"].append(int(file.strip(".json")))
                    agg_trans["Transaction_type"].append(i["name"])
                    agg_trans["Transaction_count"].append(i["paymentInstruments"][0]["count"])
                    agg_trans["Transaction_amount"].append(i["paymentInstruments"][0]["amount"])

Agg_trans = clean_states(pd.DataFrame(agg_trans))


# ===================== AGGREGATED USER =====================
agg_user = {
    "States": [], "Years": [], "Quarter": [],
    "Brands": [], "Transaction_count": [], "Percentage": []
}

path = f"{BASE_PATH}/aggregated/user/country/india/state/"
for state in os.listdir(path):
    for year in os.listdir(f"{path}{state}"):
        for file in os.listdir(f"{path}{state}/{year}"):
            with open(f"{path}{state}/{year}/{file}") as f:
                data = json.load(f)
                try:
                    for i in data["data"]["usersByDevice"]:
                        agg_user["States"].append(state)
                        agg_user["Years"].append(year)
                        agg_user["Quarter"].append(int(file.strip(".json")))
                        agg_user["Brands"].append(i["brand"])
                        agg_user["Transaction_count"].append(i["count"])
                        agg_user["Percentage"].append(i["percentage"])
                except:
                    pass

Agg_user = clean_states(pd.DataFrame(agg_user))


# ===================== AGGREGATED INSURANCE =====================
agg_ins = {
    "States": [], "Years": [], "Quarter": [],
    "Insurance_type": [], "Insurance_count": [], "Insurance_amount": []
}

path = f"{BASE_PATH}/aggregated/insurance/country/india/state/"
for state in os.listdir(path):
    for year in os.listdir(f"{path}{state}"):
        for file in os.listdir(f"{path}{state}/{year}"):
            with open(f"{path}{state}/{year}/{file}") as f:
                data = json.load(f)
                for i in data["data"]["transactionData"]:
                    agg_ins["States"].append(state)
                    agg_ins["Years"].append(year)
                    agg_ins["Quarter"].append(int(file.strip(".json")))
                    agg_ins["Insurance_type"].append(i["name"])
                    agg_ins["Insurance_count"].append(i["paymentInstruments"][0]["count"])
                    agg_ins["Insurance_amount"].append(i["paymentInstruments"][0]["amount"])

Agg_insur = clean_states(pd.DataFrame(agg_ins))


# ===================== MAP TRANSACTION =====================
map_trans = {
    "States": [], "Years": [], "Quarter": [],
    "District": [], "Transaction_count": [], "Transaction_amount": []
}

path = f"{BASE_PATH}/map/transaction/hover/country/india/state/"
for state in os.listdir(path):
    for year in os.listdir(f"{path}{state}"):
        for file in os.listdir(f"{path}{state}/{year}"):
            with open(f"{path}{state}/{year}/{file}") as f:
                data = json.load(f)
                for i in data["data"]["hoverDataList"]:
                    map_trans["States"].append(state)
                    map_trans["Years"].append(year)
                    map_trans["Quarter"].append(int(file.strip(".json")))
                    map_trans["District"].append(i["name"])
                    map_trans["Transaction_count"].append(i["metric"][0]["count"])
                    map_trans["Transaction_amount"].append(i["metric"][0]["amount"])

Map_trans = clean_states(pd.DataFrame(map_trans))


# ===================== MAP USER =====================
map_user = {
    "States": [], "Years": [], "Quarter": [],
    "District": [], "RegisteredUser": [], "AppOpens": []
}

path = f"{BASE_PATH}/map/user/hover/country/india/state/"
for state in os.listdir(path):
    for year in os.listdir(f"{path}{state}"):
        for file in os.listdir(f"{path}{state}/{year}"):
            with open(f"{path}{state}/{year}/{file}") as f:
                data = json.load(f)
                for k, v in data["data"]["hoverData"].items():
                    map_user["States"].append(state)
                    map_user["Years"].append(year)
                    map_user["Quarter"].append(int(file.strip(".json")))
                    map_user["District"].append(k)
                    map_user["RegisteredUser"].append(v["registeredUsers"])
                    map_user["AppOpens"].append(v["appOpens"])

Map_users = clean_states(pd.DataFrame(map_user))


# ===================== MAP INSURANCE =====================
map_ins = {
    "States": [], "Years": [], "Quarter": [],
    "District": [], "Transaction_count": [], "Transaction_amount": []
}

path = f"{BASE_PATH}/map/insurance/hover/country/india/state/"
for state in os.listdir(path):
    for year in os.listdir(f"{path}{state}"):
        for file in os.listdir(f"{path}{state}/{year}"):
            with open(f"{path}{state}/{year}/{file}") as f:
                data = json.load(f)
                for i in data["data"]["hoverDataList"]:
                    map_ins["States"].append(state)
                    map_ins["Years"].append(year)
                    map_ins["Quarter"].append(int(file.strip(".json")))
                    map_ins["District"].append(i["name"])
                    map_ins["Transaction_count"].append(i["metric"][0]["count"])
                    map_ins["Transaction_amount"].append(i["metric"][0]["amount"])

Map_insur = clean_states(pd.DataFrame(map_ins))


# ===================== TOP TRANSACTION =====================
top_trans = {
    "States": [], "Years": [], "Quarter": [],
    "Pincodes": [], "Transaction_count": [], "Transaction_amount": []
}

path = f"{BASE_PATH}/top/transaction/country/india/state/"
for state in os.listdir(path):
    for year in os.listdir(f"{path}{state}"):
        for file in os.listdir(f"{path}{state}/{year}"):
            with open(f"{path}{state}/{year}/{file}") as f:
                data = json.load(f)
                for i in data["data"]["pincodes"]:
                    top_trans["States"].append(state)
                    top_trans["Years"].append(year)
                    top_trans["Quarter"].append(int(file.strip(".json")))
                    top_trans["Pincodes"].append(i["entityName"])
                    top_trans["Transaction_count"].append(i["metric"]["count"])
                    top_trans["Transaction_amount"].append(i["metric"]["amount"])

Top_trans = clean_states(pd.DataFrame(top_trans))


# ===================== TOP USER =====================
top_user = {
    "States": [], "Years": [], "Quarter": [],
    "Pincodes": [], "RegisteredUser": []
}

path = f"{BASE_PATH}/top/user/country/india/state/"
for state in os.listdir(path):
    for year in os.listdir(f"{path}{state}"):
        for file in os.listdir(f"{path}{state}/{year}"):
            with open(f"{path}{state}/{year}/{file}") as f:
                data = json.load(f)
                for i in data["data"]["pincodes"]:
                    top_user["States"].append(state)
                    top_user["Years"].append(year)
                    top_user["Quarter"].append(int(file.strip(".json")))
                    top_user["Pincodes"].append(i["name"])
                    top_user["RegisteredUser"].append(i["registeredUsers"])

Top_user = clean_states(pd.DataFrame(top_user))


# ===================== TOP INSURANCE =====================
top_ins = {
    "States": [], "Years": [], "Quarter": [],
    "Pincodes": [], "Transaction_count": [], "Transaction_amount": []
}

path = f"{BASE_PATH}/top/insurance/country/india/state/"
for state in os.listdir(path):
    for year in os.listdir(f"{path}{state}"):
        for file in os.listdir(f"{path}{state}/{year}"):
            with open(f"{path}{state}/{year}/{file}") as f:
                data = json.load(f)
                for i in data["data"]["pincodes"]:
                    top_ins["States"].append(state)
                    top_ins["Years"].append(year)
                    top_ins["Quarter"].append(int(file.strip(".json")))
                    top_ins["Pincodes"].append(i["entityName"])
                    top_ins["Transaction_count"].append(i["metric"]["count"])
                    top_ins["Transaction_amount"].append(i["metric"]["amount"])
Top_insur = clean_states(pd.DataFrame(top_ins))
Top_insur["Pincodes"] = Top_insur["Pincodes"].fillna("UNKNOWN")

# Now all dataframes are ready
import sys
from pathlib import Path
import requests
import json
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))
from ingest.extract_data import (
    Agg_trans,
    Agg_user,
    Agg_insur,
    Map_trans,
    Map_users,
    Map_insur,
    Top_trans,
    Top_user,
    Top_insur
)
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

import plotly.express as px

# ===================== DATABASE =====================
# ===================== DATA HANDLING =====================
import pandas as pd
import uuid

# ===================== STREAMLIT =====================
import streamlit as st
from streamlit_option_menu import option_menu


# ===================== DATABASE =====================
from utils.database import get_connection


def load_df(query, columns):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = columns
    return df
Aggre_transaction = Agg_trans
Aggre_user = Agg_user
Aggre_insurance = Agg_insur

Map_transaction = Map_trans
Map_user = Map_users
Map_insurance = Map_insur

Top_transaction = Top_trans
Top_user = Top_user
Top_insurance = Top_insur
#'''
#Aggre_transaction = load_df(
#    "SELECT states, years, quarter, transaction_type, transaction_count, transaction_amount FROM aggregated_transaction",
#    ["States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"]
#)
#
#Aggre_user = load_df(
#    "SELECT states, years, quarter, brands, transaction_count, percentage FROM aggregated_user",
#    ["States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage"]
#)
#
#Map_insurance = load_df(
#    "SELECT states, years, quarter, district, transaction_count, transaction_amount FROM map_insurance",
#    ["States", "Years", "Quarter", "District", "Transaction_count", "Transaction_amount"]
#)
#
#Map_transaction = load_df(
#    "SELECT states, years, quarter, district, transaction_count, transaction_amount FROM map_transaction",
#    ["States", "Years", "Quarter", "District", "Transaction_count", "Transaction_amount"]
#)
#
#Map_user = load_df(
#    "SELECT states, years, quarter, district, registered_user, app_opens FROM map_user",
#    ["States", "Years", "Quarter", "District", "RegisteredUser", "AppOpens"]
#)
#
#
#Top_insurance = load_df(
#    "SELECT states, years, quarter, pincode, transaction_count, transaction_amount FROM top_insurance",
#    ["States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"]
#)
#
#Top_transaction = load_df(
#    "SELECT states, years, quarter, pincode, transaction_count, transaction_amount FROM top_transaction",
#    ["States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"]
#)
#
#Top_user = load_df(
#    "SELECT states, years, quarter, pincode, registered_user FROM top_user",
#    ["States", "Years", "Quarter", "Pincodes", "RegisteredUser"]
#)
#
#
#Aggre_insurance = load_df(
#    """
#    SELECT
#        states,
#        years,
#        quarter,
#        insurance_type,
#        insurance_count,
#        insurance_amount
#    FROM aggregated_insurance
#    """,
#    [
#        "States",
#        "Years",
#        "Quarter",
#        "Insurance_type",
#        "Insurance_count",
#        "Insurance_amount"
#    ]
#)
#-------------Main------------------

import uuid

def Aggre_transaction_Y(df, year, context_id=None):
    if context_id is None:
        context_id = uuid.uuid4().hex[:6]

    # Filter by year
    aty = df[df["Years"] == year].copy()
    aty.reset_index(drop=True, inplace=True)

    # Group by state
    atyg = (
        aty
        .groupby("States")[["Transaction_count", "Transaction_amount"]]
        .sum()
        .reset_index()
    )

    # ---------------- BAR CHARTS ----------------
    col1, col2 = st.columns(2)

    with col1:
        fig_amount = px.bar(
            atyg,
            x="States",
            y="Transaction_amount",
            title=f"{year} TRANSACTION AMOUNT",
            height=600,
            color_discrete_sequence=px.colors.sequential.Aggrnyl
        )
        st.plotly_chart(fig_amount, width="stretch",
                        key=f"bar_tran_amt_{year}_{context_id}")

    with col2:
        fig_count = px.bar(
            atyg,
            x="States",
            y="Transaction_count",
            title=f"{year} TRANSACTION COUNT",
            height=600,
            color_discrete_sequence=px.colors.sequential.Bluered_r
        )
        st.plotly_chart(fig_count, width="stretch",
                        key=f"bar_tran_cnt_{year}_{context_id}")

    # ---------------- MAPS ----------------
    geo_url = (
        "https://gist.githubusercontent.com/jbrobst/"
        "56c13bbbf9d97d187fea01ca62ea5112/raw/"
        "e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    )
    geojson = json.loads(requests.get(geo_url).content)

    col1, col2 = st.columns(2)

    with col1:
        fig_map_amt = px.choropleth(
            atyg,
            geojson=geojson,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Transaction_amount",
            title=f"{year} TRANSACTION AMOUNT",
            color_continuous_scale="Cividis",
            fitbounds="locations"
        )
        fig_map_amt.update_geos(visible=False)
        st.plotly_chart(fig_map_amt, width="stretch",
                        key=f"map_tran_amt_{year}_{context_id}")

    with col2:
        fig_map_cnt = px.choropleth(
            atyg,
            geojson=geojson,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Transaction_count",
            title=f"{year} TRANSACTION COUNT",
            color_continuous_scale="Cividis",
            fitbounds="locations"
        )
        fig_map_cnt.update_geos(visible=False)
        st.plotly_chart(fig_map_cnt, width="stretch",
                        key=f"map_tran_cnt_{year}_{context_id}")

    return aty

def Aggre_transaction_Y_Q(df, quarter, context_id=None):
    if context_id is None:
        context_id = uuid.uuid4().hex[:6]

    # Filter quarter
    atyq = df[df["Quarter"] == quarter].copy()

    # Group transaction data
    atyqg = (
        atyq
        .groupby("States")[["Transaction_count", "Transaction_amount"]]
        .sum()
        .reset_index()
    )

    # ---------------- BAR CHARTS ----------------
    col1, col2 = st.columns(2)

    with col1:
        fig_q_amount = px.bar(
            atyqg,
            x="States",
            y="Transaction_amount",
            title=f"{atyq['Years'].min()}  Q{quarter} TRANSACTION AMOUNT",
            height=600,
            color_discrete_sequence=px.colors.sequential.Burg_r
        )
        st.plotly_chart(
            fig_q_amount,
            width="stretch",
            key=f"q_tran_amt_{quarter}_{context_id}"
        )

    with col2:
        fig_q_count = px.bar(
            atyqg,
            x="States",
            y="Transaction_count",
            title=f"{atyq['Years'].min()}  Q{quarter} TRANSACTION COUNT",
            height=600,
            color_discrete_sequence=px.colors.sequential.Cividis_r
        )
        st.plotly_chart(
            fig_q_count,
            width="stretch",
            key=f"q_tran_cnt_{quarter}_{context_id}"
        )

    # ---------------- MAPS ----------------
    geo_url = (
        "https://gist.githubusercontent.com/jbrobst/"
        "56c13bbbf9d97d187fea01ca62ea5112/raw/"
        "e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    )
    geojson = json.loads(requests.get(geo_url).content)

    col1, col2 = st.columns(2)

    with col1:
        fig_map_amt = px.choropleth(
            atyqg,
            geojson=geojson,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Transaction_amount",
            color_continuous_scale="Cividis",
            range_color=(
                atyqg["Transaction_amount"].min(),
                atyqg["Transaction_amount"].max()
            ),
            hover_name="States",
            title=f"{atyq['Years'].min()}  Q{quarter} TRANSACTION AMOUNT",
            fitbounds="locations"
        )
        fig_map_amt.update_geos(visible=False)
        st.plotly_chart(
            fig_map_amt,
            width="stretch",
            key=f"q_map_tran_amt_{quarter}_{context_id}"
        )

    with col2:
        fig_map_cnt = px.choropleth(
            atyqg,
            geojson=geojson,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Transaction_count",
            color_continuous_scale="Cividis",
            range_color=(
                atyqg["Transaction_count"].min(),
                atyqg["Transaction_count"].max()
            ),
            hover_name="States",
            title=f"{atyq['Years'].min()}  Q{quarter} TRANSACTION COUNT",
            fitbounds="locations"
        )
        fig_map_cnt.update_geos(visible=False)
        st.plotly_chart(
            fig_map_cnt,
            width="stretch",
            key=f"q_map_tran_cnt_{quarter}_{context_id}"
        )

    return atyq

def Aggre_insurance_Y(df, year, context_id=None):
    if context_id is None:
        context_id = uuid.uuid4().hex[:6]

    # Filter year
    aiy = df[df["Years"] == year].copy()

    # Group insurance data
    aiyg = (
        aiy
        .groupby("States")[["Insurance_count", "Insurance_amount"]]
        .sum()
        .reset_index()
    )

    # ---------------- BAR CHARTS ----------------
    col1, col2 = st.columns(2)

    with col1:
        fig_amount = px.bar(
            aiyg,
            x="States",
            y="Insurance_amount",
            title=f"{year} INSURANCE AMOUNT",
            height=600,
            color_discrete_sequence=px.colors.sequential.Aggrnyl
        )
        st.plotly_chart(
            fig_amount,
            width="stretch",
            key=f"bar_ins_amt_{year}_{context_id}"
        )

    with col2:
        fig_count = px.bar(
            aiyg,
            x="States",
            y="Insurance_count",
            title=f"{year} INSURANCE COUNT",
            height=600,
            color_discrete_sequence=px.colors.sequential.Bluered_r
        )
        st.plotly_chart(
            fig_count,
            width="stretch",
            key=f"bar_ins_cnt_{year}_{context_id}"
        )

    # ---------------- MAPS ----------------
    geo_url = (
        "https://gist.githubusercontent.com/jbrobst/"
        "56c13bbbf9d97d187fea01ca62ea5112/raw/"
        "e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    )
    geojson = json.loads(requests.get(geo_url).content)

    col1, col2 = st.columns(2)

    with col1:
        fig_map_amount = px.choropleth(
            aiyg,
            geojson=geojson,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Insurance_amount",
            color_continuous_scale="Cividis",
            range_color=(
                aiyg["Insurance_amount"].min(),
                aiyg["Insurance_amount"].max()
            ),
            hover_name="States",
            title=f"{year} INSURANCE AMOUNT",
            fitbounds="locations"
        )
        fig_map_amount.update_geos(visible=False)
        st.plotly_chart(
            fig_map_amount,
            width="stretch",
            key=f"map_ins_amt_{year}_{context_id}"
        )

    with col2:
        fig_map_count = px.choropleth(
            aiyg,
            geojson=geojson,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Insurance_count",   # ✅ FIXED
            color_continuous_scale="Cividis",
            range_color=(
                aiyg["Insurance_count"].min(),
                aiyg["Insurance_count"].max()
            ),
            hover_name="States",
            title=f"{year} INSURANCE COUNT",  # ✅ FIXED
            fitbounds="locations"
        )
        fig_map_count.update_geos(visible=False)
        st.plotly_chart(
            fig_map_count,
            width="stretch",
            key=f"map_ins_cnt_{year}_{context_id}"
        )

    return aiy

def Aggre_insurance_Y_Q(df, quarter, context_id=None):
    if context_id is None:
        context_id = uuid.uuid4().hex[:6]

    # Filter quarter
    aiyq = df[df["Quarter"] == quarter].copy()

    # Group insurance data
    aiyqg = (
        aiyq
        .groupby("States")[["Insurance_count", "Insurance_amount"]]
        .sum()
        .reset_index()
    )

    # ---------------- BAR CHARTS ----------------
    col1, col2 = st.columns(2)

    with col1:
        fig_q_amount = px.bar(
            aiyqg,
            x="States",
            y="Insurance_amount",
            title=f"{aiyq['Years'].min()}  Q{quarter} INSURANCE AMOUNT",
            height=600,
            color_discrete_sequence=px.colors.sequential.Burg_r
        )
        st.plotly_chart(
            fig_q_amount,
            width="stretch",
            key=f"q_ins_amt_{quarter}_{context_id}"
        )

    with col2:
        fig_q_count = px.bar(
            aiyqg,
            x="States",
            y="Insurance_count",
            title=f"{aiyq['Years'].min()}  Q{quarter} INSURANCE COUNT",
            height=600,
            color_discrete_sequence=px.colors.sequential.Cividis_r
        )
        st.plotly_chart(
            fig_q_count,
            width="stretch",
            key=f"q_ins_cnt_{quarter}_{context_id}"
        )

    # ---------------- MAPS ----------------
    geo_url = (
        "https://gist.githubusercontent.com/jbrobst/"
        "56c13bbbf9d97d187fea01ca62ea5112/raw/"
        "e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    )
    geojson = json.loads(requests.get(geo_url).content)

    col1, col2 = st.columns(2)

    with col1:
        fig_map_amt = px.choropleth(
            aiyqg,
            geojson=geojson,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Insurance_amount",
            color_continuous_scale="Cividis",
            range_color=(
                aiyqg["Insurance_amount"].min(),
                aiyqg["Insurance_amount"].max()
            ),
            hover_name="States",
            title=f"{aiyq['Years'].min()}  Q{quarter} INSURANCE AMOUNT",
            fitbounds="locations"
        )
        fig_map_amt.update_geos(visible=False)
        st.plotly_chart(
            fig_map_amt,
            width="stretch",
            key=f"q_map_ins_amt_{quarter}_{context_id}"
        )

    with col2:
        fig_map_cnt = px.choropleth(
            aiyqg,
            geojson=geojson,
            locations="States",
            featureidkey="properties.ST_NM",
            color="Insurance_count",
            color_continuous_scale="Cividis",
            range_color=(
                aiyqg["Insurance_count"].min(),
                aiyqg["Insurance_count"].max()
            ),
            hover_name="States",
            title=f"{aiyq['Years'].min()}  Q{quarter} INSURANCE COUNT",
            fitbounds="locations"
        )
        fig_map_cnt.update_geos(visible=False)
        st.plotly_chart(
            fig_map_cnt,
            width="stretch",
            key=f"q_map_ins_cnt_{quarter}_{context_id}"
        )

    return aiyq


    
def Aggre_Transaction_type(df, state):
    df_state= df[df["States"] == state]
    df_state.reset_index(drop= True, inplace= True)

    agttg= df_state.groupby("Transaction_type")[["Transaction_count", "Transaction_amount"]].sum()
    agttg.reset_index(inplace= True)

    col1,col2= st.columns(2)
    with col1:

        fig_hbar_1= px.bar(agttg, x= "Transaction_count", y= "Transaction_type", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, width= 400, 
                        title= f"{state.upper()} TRANSACTION TYPES AND TRANSACTION COUNT",height= 500)
        st.plotly_chart(fig_hbar_1)

    with col2:

        fig_hbar_2= px.bar(agttg, x= "Transaction_amount", y= "Transaction_type", orientation="h",
                        color_discrete_sequence=px.colors.sequential.Greens_r, width= 400,
                        title= f"{state.upper()} TRANSACTION TYPES AND TRANSACTION AMOUNT", height= 500)
        st.plotly_chart(fig_hbar_2)

def Aggre_user_plot_1(df,year):
    aguy= df[df["Years"] == year]
    aguy.reset_index(drop= True, inplace= True)
    
    aguyg= pd.DataFrame(aguy.groupby("Brands")["Transaction_count"].sum())
    aguyg.reset_index(inplace= True)

    fig_line_1= px.bar(aguyg, x="Brands",y= "Transaction_count", title=f"{year} BRANDS AND TRANSACTION COUNT",
                    width=800,color_discrete_sequence=px.colors.sequential.Magenta_r)
    st.plotly_chart(fig_line_1)

    return aguy

def Aggre_user_plot_2(df,quarter):
    auqs= df[df["Quarter"] == quarter]
    auqs.reset_index(drop= True, inplace= True)

    fig_pie_1= px.pie(data_frame=auqs, names= "Brands", values="Transaction_count", hover_data= "Percentage",
                      width=800,title=f"{quarter} QUARTER TRANSACTION COUNT PERCENTAGE",hole=0.5, color_discrete_sequence= px.colors.sequential.Magenta_r)
    st.plotly_chart(fig_pie_1)

    return auqs

def Aggre_user_plot_3(df,state):
    aguqy= df[df["States"] == state]
    aguqy.reset_index(drop= True, inplace= True)

    aguqyg= pd.DataFrame(aguqy.groupby("Brands")["Transaction_count"].sum())
    aguqyg.reset_index(inplace= True)

    fig_scatter_1= px.line(aguqyg, x= "Brands", y= "Transaction_count", markers= True,width=800)
    st.plotly_chart(fig_scatter_1)

def map_transaction_plot_bar(df, state):
    df_state = df[df["States"] == state].copy()

    grouped = (
        df_state
        .groupby("District")[["Transaction_count", "Transaction_amount"]]
        .sum()
        .reset_index()
    )

    col1, col2 = st.columns(2)

    with col1:
        fig_amount = px.bar(
            grouped,
            x="District",
            y="Transaction_amount",
            title=f"{state.upper()} DISTRICT TRANSACTION AMOUNT",
            height=500,
            color_discrete_sequence=px.colors.sequential.Mint_r
        )
        st.plotly_chart(fig_amount, width="stretch")

    with col2:
        fig_count = px.bar(
            grouped,
            x="District",
            y="Transaction_count",
            title=f"{state.upper()} DISTRICT TRANSACTION COUNT",
            height=500,
            color_discrete_sequence=px.colors.sequential.Mint
        )
        st.plotly_chart(fig_count, width="stretch")

def map_transaction_plot_pie(df, state):
    df_state = df[df["States"] == state].copy()

    grouped = (
        df_state
        .groupby("District")[["Transaction_count", "Transaction_amount"]]
        .sum()
        .reset_index()
    )

    col1, col2 = st.columns(2)

    with col1:
        fig_amount_pie = px.pie(
            grouped,
            names="District",
            values="Transaction_amount",
            title=f"{state.upper()} DISTRICT TRANSACTION AMOUNT",
            hole=0.5,
            height=500,
            color_discrete_sequence=px.colors.sequential.Mint_r
        )
        st.plotly_chart(fig_amount_pie, width="stretch")

    with col2:
        fig_count_pie = px.pie(
            grouped,
            names="District",
            values="Transaction_count",
            title=f"{state.upper()} DISTRICT TRANSACTION COUNT",
            hole=0.5,
            height=500,
            color_discrete_sequence=px.colors.sequential.Oranges_r
        )
        st.plotly_chart(fig_count_pie, width="stretch")

def map_insurance_plot_bar(df, state):
    # Filter state
    miys = df[df["States"] == state].copy()

    # Group by district
    miysg = (
        miys
        .groupby("District")[["Insurance_count", "Insurance_amount"]]
        .sum()
        .reset_index()
    )

    col1, col2 = st.columns(2)

    # ---------- INSURANCE AMOUNT ----------
    with col1:
        fig_amount = px.bar(
            miysg,
            x="District",
            y="Insurance_amount",
            title=f"{state.upper()} DISTRICT INSURANCE AMOUNT",
            height=500,
            color_discrete_sequence=px.colors.sequential.Mint_r
        )
        fig_amount.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(
            fig_amount,
            width="stretch",
            key=f"ins_bar_amt_{state}"
        )

    # ---------- INSURANCE COUNT ----------
    with col2:
        fig_count = px.bar(
            miysg,
            x="District",
            y="Insurance_count",
            title=f"{state.upper()} DISTRICT INSURANCE COUNT",
            height=500,
            color_discrete_sequence=px.colors.sequential.Mint
        )
        fig_count.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(
            fig_count,
            width="stretch",
            key=f"ins_bar_cnt_{state}"
        )

def map_insurance_plot_pie(df, state):
    # Filter state
    miys = df[df["States"] == state].copy()

    # Group by district
    miysg = (
        miys
        .groupby("District")[["Insurance_count", "Insurance_amount"]]
        .sum()
        .reset_index()
    )

    col1, col2 = st.columns(2)

    # ---------- INSURANCE AMOUNT ----------
    with col1:
        fig_pie_amt = px.pie(
            miysg,
            names="District",
            values="Insurance_amount",
            title=f"{state.upper()} DISTRICT INSURANCE AMOUNT",
            hole=0.45,
            height=500,
            color_discrete_sequence=px.colors.sequential.Mint_r
        )
        fig_pie_amt.update_traces(
            textinfo="percent+label",
            hovertemplate="%{label}<br>₹%{value:,}<extra></extra>"
        )
        st.plotly_chart(
            fig_pie_amt,
            width="stretch",
            key=f"ins_pie_amt_{state}"
        )

    # ---------- INSURANCE COUNT ----------
    with col2:
        fig_pie_cnt = px.pie(
            miysg,
            names="District",
            values="Insurance_count",
            title=f"{state.upper()} DISTRICT INSURANCE COUNT",
            hole=0.45,
            height=500,
            color_discrete_sequence=px.colors.sequential.Oranges_r
        )
        fig_pie_cnt.update_traces(
            textinfo="percent+label",
            hovertemplate="%{label}<br>%{value:,} policies<extra></extra>"
        )
        st.plotly_chart(
            fig_pie_cnt,
            width="stretch",
            key=f"ins_pie_cnt_{state}"
        )

#old functions
def map_insure_plot_1(df,state):
    miys= df[df["States"] == state]
    miysg= miys.groupby("District")[["Transaction_count","Transaction_amount"]].sum()
    miysg.reset_index(inplace= True)

    col1, col2 = st.columns(2)

    with col1:
        fig_amount = px.bar(
            miysg, x="District", y="Transaction_amount",
            width=400, height=500,
            title=f"{state.upper()} DISTRICT TRANSACTION AMOUNT",
            color_discrete_sequence=px.colors.sequential.Mint_r
        )
        st.plotly_chart(fig_amount, key=f"{state}_amount_chart")

    with col2:
        fig_count = px.bar(
            miysg, x="District", y="Transaction_count",
            width=400, height=500,
            title=f"{state.upper()} DISTRICT TRANSACTION COUNT",
            color_discrete_sequence=px.colors.sequential.Mint
        )
        st.plotly_chart(fig_count, key=f"{state}_count_chart")
#old functions
def map_insure_plot_2(df,state):
    miys= df[df["States"] == state]
    miysg= miys.groupby("District")[["Transaction_count","Transaction_amount"]].sum()
    miysg.reset_index(inplace= True)

    col1,col2= st.columns(2)
    with col1:
        fig_map_pie_1= px.pie(miysg, names= "District", values= "Transaction_amount",
                              width=500, height=500, title= f"{state.upper()} DISTRICT TRANSACTION AMOUNT",
                              hole=0.5,color_discrete_sequence= px.colors.sequential.Mint_r)
        st.plotly_chart(fig_map_pie_1)

    with col2:
        fig_map_pie_1= px.pie(miysg, names= "District", values= "Transaction_count",
                              width=500, height= 500, title= f"{state.upper()} DISTRICT TRANSACTION COUNT",
                              hole=0.5,  color_discrete_sequence= px.colors.sequential.Oranges_r)
        
        st.plotly_chart(fig_map_pie_1)

def map_user_plot_1(df, year):
    muy = df[df["Years"] == year].copy()

    muyg = (
        muy
        .groupby("States")[["RegisteredUser", "AppOpens"]]
        .sum()
        .reset_index()
    )

    fig = px.line(
        muyg,
        x="States",
        y=["RegisteredUser", "AppOpens"],
        markers=True,
        title=f"{year} REGISTERED USERS & APP OPENS",
        color_discrete_sequence=px.colors.sequential.Bluyl
    )

    fig.update_layout(xaxis_tickangle=-45)

    st.plotly_chart(
        fig,
        width="stretch",
        key=f"user_map_year_{year}"
    )

    return muy

def map_user_plot_2(df, quarter):
    muyq = df[df["Quarter"] == quarter].copy()

    muyqg = (
        muyq
        .groupby("States")[["RegisteredUser", "AppOpens"]]
        .sum()
        .reset_index()
    )

    year_label = muyq["Years"].min()

    fig = px.line(
        muyqg,
        x="States",
        y=["RegisteredUser", "AppOpens"],
        markers=True,
        title=f"{year_label} {quarter} REGISTERED USERS & APP OPENS",
        color_discrete_sequence=px.colors.sequential.Blugrn
    )

    fig.update_layout(xaxis_tickangle=-45)

    st.plotly_chart(
        fig,
        width="stretch",
        key=f"user_map_quarter_{quarter}"
    )

    return muyq

def map_user_plot_3(df, state):
    muyqs = df[df["States"] == state].copy()

    muyqsg = (
        muyqs
        .groupby("District")[["RegisteredUser", "AppOpens"]]
        .sum()
        .reset_index()
    )

    fig = px.pie(
        muyqsg,
        names="District",
        values="RegisteredUser",
        title=f"{state.upper()} REGISTERED USERS BY DISTRICT",
        hole=0.45,
        color_discrete_sequence=px.colors.sequential.Rainbow_r
    )

    fig.update_traces(
        textinfo="percent+label",
        hovertemplate="%{label}<br>%{value:,} users<extra></extra>"
    )

    st.plotly_chart(
        fig,
        width="stretch",
        key=f"user_map_state_{state}"
    )


def top_user_plot_1(df,year):
    tuy= df[df["Years"] == year]
    tuy.reset_index(drop= True, inplace= True)

    tuyg= pd.DataFrame(tuy.groupby(["States","Quarter"])["RegisteredUser"].sum())
    tuyg.reset_index(inplace= True)

    fig_top_plot_1= px.bar(tuyg, x= "States", y= "RegisteredUser", barmode= "group", color= "Quarter",
                            width=1000, height= 800, color_continuous_scale= px.colors.sequential.Rainbow)
    st.plotly_chart(fig_top_plot_1)

    return tuy

def top_user_plot_2(df,state):
    tuys= df[df["States"] == state]
    tuys.reset_index(drop= True, inplace= True)

    tuysg= pd.DataFrame(tuys.groupby("Quarter")["RegisteredUser"].sum())
    tuysg.reset_index(inplace= True)

    fig_top_plot_2= px.bar(tuys, x= "Quarter", y= "RegisteredUser",barmode= "group",
                           width=1000, height= 800,color= "Pincodes",hover_data="Pincodes",
                            color_continuous_scale= px.colors.sequential.Magenta)
    st.plotly_chart(fig_top_plot_2)




def ques1():
    brand= Aggre_user[["Brands","Transaction_count"]]
    brand1= brand.groupby("Brands")["Transaction_count"].sum().sort_values(ascending=False)
    brand2= pd.DataFrame(brand1).reset_index()

    fig_brands= px.pie(brand2, values= "Transaction_count", names= "Brands", color_discrete_sequence=px.colors.sequential.Rainbow_r,
                       title= "Top Mobile Brands of Transaction_count")
    return st.plotly_chart(fig_brands)

def ques2():
    lt= Aggre_transaction[["States", "Transaction_amount"]]
    lt1= lt.groupby("States")["Transaction_amount"].sum().sort_values(ascending= True)
    lt2= pd.DataFrame(lt1).reset_index().head(10)

    fig_lts= px.bar(lt2, x= "States", y= "Transaction_amount",title= "LOWEST TRANSACTION AMOUNT and STATES",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)

def ques3():
    ht= Aggre_transaction[["States", "Transaction_amount"]]
    ht1= ht.groupby("States")["Transaction_amount"].sum().sort_values(ascending= False)
    ht2= pd.DataFrame(ht1).reset_index().head(10)

    fig_lts= px.bar(ht2, x= "States", y= "Transaction_amount",title= "HIGHEST TRANSACTION AMOUNT and STATES",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)


def ques4():
    htd= Map_transaction[["District", "Transaction_amount"]]
    htd1= htd.groupby("District")["Transaction_amount"].sum().sort_values(ascending=False)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.bar(htd2, x= "Transaction_amount", y= "District", title="TOP 10 DISTRICT OF HIGHEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Reds)
    return st.plotly_chart(fig_htd)

def ques5():
    htd= Map_transaction[["District", "Transaction_amount"]]
    htd1= htd.groupby("District")["Transaction_amount"].sum().sort_values(ascending=True)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.bar(htd2, x= "Transaction_amount", y= "District", title="TOP 10 DISTRICT OF LOWEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Reds)
    return st.plotly_chart(fig_htd)


def ques6():
    sa= Map_user[["States", "AppOpens"]]
    sa1= sa.groupby("States")["AppOpens"].sum().sort_values(ascending=False)
    sa2= pd.DataFrame(sa1).reset_index().head(10)

    fig_sa= px.pie(sa2, names= "States", values= "AppOpens", title="Top 10 States With Phonepe users",hole=0.5,
                color_discrete_sequence= px.colors.sequential.Rainbow_r)
    return st.plotly_chart(fig_sa)

def ques7():
    sa= Map_user[["States", "AppOpens"]]
    sa1= sa.groupby("States")["AppOpens"].sum().sort_values(ascending=True)
    sa2= pd.DataFrame(sa1).reset_index().head(10)

    fig_sa= px.pie(sa2, names= "States", values= "AppOpens", title="lowest 10 States With Phonepe users",hole=0.5,
                color_discrete_sequence= px.colors.sequential.Rainbow_r)
    return st.plotly_chart(fig_sa)

def ques8():
    stc= Aggre_transaction[["States", "Transaction_count"]]
    stc1= stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=True)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "States", y= "Transaction_count", title= "STATES WITH LOWEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Magenta_r)
    return st.plotly_chart(fig_stc)

def ques9():
    stc= Aggre_transaction[["States", "Transaction_count"]]
    stc1= stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=False)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "States", y= "Transaction_count", title= "STATES WITH HIGHEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Magenta_r)
    return st.plotly_chart(fig_stc)


def ques10():
    dt= Map_transaction[["District", "Transaction_amount"]]
    dt1= dt.groupby("District")["Transaction_amount"].sum().sort_values(ascending=False)
    dt2= pd.DataFrame(dt1).reset_index().head(50)

    fig_dt= px.bar(dt2, x= "District", y= "Transaction_amount", title= "DISTRICT WITH HIGHEST TRANSACTION AMOUNT",
                color_discrete_sequence= px.colors.sequential.Viridis)
    return st.plotly_chart(fig_dt)


#-------------------------------------------------Streamlit part---------------------------------------------------------------------->


st.set_page_config(layout="wide")


st.markdown("""
    <style>
    .title-3d {
        font-size: 3rem;
        font-weight: 900;
        color: white;
        text-shadow:
            2px 2px 0 #ff6600,
            4px 4px 0 #000000;
        margin-bottom: 1rem;
    }
    </style>

    <h1 class="title-3d">
        <span style="color: orange;">PHONEPE</span> DATA VISUALIZATION AND EXPLORATION
    </h1>
""", unsafe_allow_html=True)



# Sidebar Navigation Menu
with st.sidebar:
    select = option_menu(
        "Main Menu",
        ["Home", "Data Exploration", "Top Charts"],
        icons=["house", "bar-chart", "graph-up"],
        menu_icon="cast",
        default_index=0,
        styles={
            "container": {"padding": "5!important", "background-color": "#141414"},
            "icon": {"color": "white", "font-size": "20px"},
            "nav-link": {
                "font-size": "18px",
                "text-align": "left",
                "margin": "5px",
                "--hover-color": "#e50914",
                "color": "white"
            },
            "nav-link-selected": {
                "background-color": "#e50914",
                "font-weight": "bold",
                "color": "white"
            },
        },
    )
if select == "Home":
    st.markdown("""
    <style>
        body {
            background-color: #141414;
            color: #fff;
        }
        .header {
            text-align: center;
            padding: 2rem;
            background: linear-gradient(135deg, #e50914, #b00710);
            border-radius: 10px;
            margin-bottom: 2rem;
        }
        .header h1 {
            font-size: 3.5rem;
            font-weight: bold;
            color: #fff;
        }
        .header p {
            font-size: 1.2rem;
            color: #f5f5f1;
            margin-top: 0.5rem;
        }
        .features {
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            gap: 1rem;
        }
        .feature-card {
            background: #1f1f1f;
            border-left: 5px solid #e50914;
            border-radius: 8px;
            padding: 1rem;
            width: 48%;
            box-shadow: 0 4px 10px rgba(0,0,0,0.5);
            margin-bottom: 1rem;
        }
        .feature-card p {
            font-size: 1rem;
            color: #f5f5f1;
        }
        .download-btn {
            display: inline-block;
            background-color: #e50914;
            color: white !important;
            font-weight: bold;
            padding: 14px 30px;
            border-radius: 50px;
            text-decoration: none;
            font-size: 1.1rem;
            margin: 30px auto;
        }
        .download-btn:hover {
            background-color: #b00710;
        }
        .video-wrapper {
            text-align: center;
            margin: 2rem 0;
        }
        .video-title {
            font-size: 1.8rem;
            margin-bottom: 1rem;
            color: #ffffff;
        }
    </style>
    """, unsafe_allow_html=True)

    # ---- HEADER SECTION ----
    st.markdown("""
    <div class="header">
        <h1>PHONEPE</h1>
        <p>INDIA'S BEST TRANSACTION APP</p>
        <p>INDIA'S DIGITAL PAYMENT PLATFORM</p>
    </div>
    """, unsafe_allow_html=True)

    # ---- FEATURES SECTION ----
    st.markdown('<div class="features">', unsafe_allow_html=True)
    features = [
        "✓ 100% Secure and Lightning Fast",
        "✓ Instant Money Transfer",
        "✓ Transfer Money up to ₹1 Lakh Daily",
        "✓ Banking Services 24/7",
        "✓ UPI Payments & More",
    ]
    for f in features:
        st.markdown(f"""
        <div class="feature-card">
            <p>{f}</p>
        </div>
        """, unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # ---- DOWNLOAD BUTTON ----
    st.markdown("""
    <div style='text-align: center;'>
        <a class='download-btn' href='https://www.phonepe.com/app-download/' target='_blank'>
            DOWNLOAD THE APP NOW
        </a>
    </div>
    """, unsafe_allow_html=True)

    # ---- VIDEO SECTION ----
    st.markdown("""
    <div class='video-wrapper'>
        <div class='video-title'>📹 How PhonePe Works</div>
        <iframe width="100%" height="480" src="https://www.youtube.com/embed/xhZ82fUWJ6g"
        frameborder="0" allowfullscreen></iframe>
    </div>
    """, unsafe_allow_html=True)
#--------------------------------------------2nd---------------------------------------------------------
#changed 
if select == "Data Exploration":
    # CSS for styling tabs and radio buttons with corrected spacing
    st.markdown("""
    <style>
        /* TABS */
        .stTabs [data-baseweb="tab"] {
            font-size: 18px;
            font-weight: bold;
            color: white;
            background-color: #1f1f1f;
            border-radius: 5px 5px 0 0;
            padding: 12px 20px;
            margin-right: 6px;
        }
        .stTabs [aria-selected="true"] {
            background-color: #e50914;
            color: white;
        }

        /* RADIO GROUP CONTAINER */
        .stRadio > div {
            background-color: #1c1c1c;
            padding: 20px;
            border-radius: 12px;
            margin-top: 20px;
        }

        /* RADIO LABELS */
        label[data-testid="stRadioLabel"] {
            font-size: 1.2rem;
            font-weight: bold;
            color: white;
        }

        /* RADIO OPTIONS HOVER EFFECT */
        [data-testid="stRadio"] div[role="radiogroup"] label:hover {
            color: orange;
        }

        /* BODY BACKGROUND FIX (optional, if needed) */
        .main {
            padding-top: 1rem;
        }
    </style>
    """, unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["📊 Aggregated Analysis", "🗺️ Map Analysis"])
    with tab1:
        st.markdown("### 📊 Aggregated Analysis")

        method = st.radio(
            "**Select the Analysis Method**",
            ["Insurance Analysis", "Transaction Analysis", "User Analysis"]
        )

        # ---------------- INSURANCE ----------------
        if method == "Insurance Analysis":

            year = st.selectbox(
                "**Select Year**",
                Aggre_insurance["Years"].unique()
            )

            df_year = Aggre_insurance_Y(Aggre_insurance, year)

            quarter = st.selectbox(
                "**Select Quarter**",
                df_year["Quarter"].unique()
            )

            Aggre_insurance_Y_Q(df_year, quarter)

        # ---------------- TRANSACTION ----------------
        elif method == "Transaction Analysis":

            year = st.selectbox(
                "**Select Year**",
                Aggre_transaction["Years"].unique()
            )

            df_year = Aggre_transaction_Y(Aggre_transaction, year)

            quarter = st.selectbox(
                "**Select Quarter**",
                df_year["Quarter"].unique()
            )

            df_q = Aggre_transaction_Y_Q(df_year, quarter)

            state = st.selectbox(
                "**Select State**",
                df_q["States"].unique()
            )

            Aggre_Transaction_type(df_q, state)

        # ---------------- USER ----------------
        elif method == "User Analysis":

            year = st.selectbox(
                "**Select Year**",
                Aggre_user["Years"].unique()
            )

            df_year = Aggre_user_plot_1(Aggre_user, year)

            quarter = st.selectbox(
                "**Select Quarter**",
                df_year["Quarter"].unique()
            )

            df_q = Aggre_user_plot_2(df_year, quarter)

            state = st.selectbox(
                "**Select State**",
                df_q["States"].unique()
            )

            Aggre_user_plot_3(df_q, state)
    with tab2:
        st.markdown("### 🗺️ Map Analysis")

        method_map = st.radio(
            "**Select Map Analysis Type**",
            ["Map Insurance Analysis", "Map Transaction Analysis", "Map User Analysis"]
        )

        # ---------------- MAP INSURANCE (ACTUALLY TRANSACTION) ----------------
        if method_map == "Map Insurance Analysis":

            st.info(
                "ℹ️ Map_insurance contains transaction data at district level.\n"
                "Showing TRANSACTION visualizations."
            )

            year = st.selectbox(
                "**Select Year**",
                Map_insurance["Years"].unique(),
                key="map_insurance_year"
            )

            df_year = Aggre_transaction_Y(Map_insurance, year)

            quarter = st.selectbox(
                "**Select Quarter**",
                df_year["Quarter"].unique(),
                key="map_insurance_yea"
            )

            df_q = Aggre_transaction_Y_Q(df_year, quarter)

            # 🔵 Quarter-level PIE
            state_q = st.selectbox(
                "**Select State (Quarter view)**",
                df_q["States"].unique()
            )
            map_transaction_plot_pie(df_q, state_q)

            # 🔵 Year-level BAR
            state_y = st.selectbox(
                "**Select State (Year view)**",
                df_year["States"].unique()
            )
            map_transaction_plot_bar(df_year, state_y)

        # ---------------- MAP TRANSACTION ----------------
        elif method_map == "Map Transaction Analysis":

            year = st.selectbox(
                "**Select Year**",
                Map_transaction["Years"].unique()
            )

            df_year = Aggre_transaction_Y(Map_transaction, year)

            quarter = st.selectbox(
                "**Select Quarter**",
                df_year["Quarter"].unique()
            )

            df_q = Aggre_transaction_Y_Q(df_year, quarter)

            # 🔵 Quarter-level PIE
            state_q = st.selectbox(
                "**Select State (Quarter view)**",
                df_q["States"].unique()
            )
            map_transaction_plot_pie(df_q, state_q)

            # 🔵 Year-level BAR
            state_y = st.selectbox(
                "**Select State (Year view)**",
                df_year["States"].unique()
            )
            map_transaction_plot_bar(df_year, state_y)

        # ---------------- MAP USER ----------------
        elif method_map == "Map User Analysis":

            year = st.selectbox(
                "**Select Year**",
                Map_user["Years"].unique(),
                key="map_insurance"
            )

            df_year = map_user_plot_1(Map_user, year)

            quarter = st.selectbox(
                "**Select Quarter**",
                df_year["Quarter"].unique()
            )

            df_q = map_user_plot_2(df_year, quarter)

            state = st.selectbox(
                "**Select State**",
                df_q["States"].unique()
            )

            map_user_plot_3(df_q, state)

if select == "Top Charts":
    st.markdown("""
        <style>
        .top-charts-container {
            background-color: #1c1c1c;
            padding: 2rem;
            border-radius: 16px;
            box-shadow: 0 0 8px rgba(229, 9, 20, 0.2);
            margin-top: 1rem;
            animation: fadeIn 0.8s ease-in-out;
        }

        .top-charts-title {
            color: #ffffff;
            font-size: 2.2rem;
            font-weight: 800;
            text-shadow: 0 0 4px #e50914;
            margin-bottom: 1.5rem;
        }

        .stSelectbox > div {
            background-color: #262626 !important;
            color: #ffffff !important;
            border: none !important;
            border-radius: 10px !important;
            padding: 0.4rem !important;
            box-shadow: 0 0 0 1px rgba(229, 9, 20, 0.3);
        }

        .stSelectbox > div:hover {
            box-shadow: 0 0 6px #e50914;
        }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        </style>
    """, unsafe_allow_html=True)

    #st.markdown("<div class='top-charts-container'>", unsafe_allow_html=True)

    st.markdown("""
    <div style="
        background-color: #e50914;
        padding: 10px;
        border-radius: 20px;
        text-align: center;
        box-shadow: 0 8px 20px rgba(0,0,0,0.4);
        margin-bottom: 30px;
    ">
        <h2 style="color: white; font-weight: bold; font-size: 2rem; margin: 0;">
            🔝 Top Charts Analysis
        </h2>
    </div>
    """, unsafe_allow_html=True)
    ques = st.selectbox("**Select the Question**", (
        'Top Brands Of Mobiles Used',
        'States With Lowest Trasaction Amount',
        'States With Highest Trasaction Amount',
        'District With Highest Transaction Amount',
        'District With Lowest Transaction Amount',
        'Top 10 States With Phonepe users',
        'Least 10 States With Phonepe users',
        'States With Lowest Trasaction Count',
        'States With Highest Trasaction Count',
        'Top 50 District With Highest Transaction Amount'
    ))
    # Render the result inside the container
    if ques == "Top Brands Of Mobiles Used":
        ques1()
    elif ques == "States With Lowest Trasaction Amount":
        ques2()
    elif ques == "States With Highest Trasaction Amount":
        ques3()
    elif ques == "District With Highest Transaction Amount":
        ques4()  # Important this is inside the container
    elif ques == "District With Lowest Transaction Amount":
        ques5()
    elif ques == "Top 10 States With Phonepe users":
        ques6()
    elif ques == "Least 10 States With Phonepe users":
        ques7()
    elif ques == "States With Lowest Trasaction Count":
        ques8()
    elif ques == "States With Highest Trasaction Count":
        ques9()
    elif ques == "Top 50 District With Highest Transaction Amount":
        ques10()
    st.markdown("</div>", unsafe_allow_html=True)  # Make sure this closes the box

st.markdown("""
<style>
.footer-banner {
    background: linear-gradient(135deg, #e50914, #b0060f);
    padding: 20px;
    margin-top: 4rem;
    border-radius: 12px;
    box-shadow: 0 6px 20px rgba(0, 0, 0, 0.7);
    text-align: center;
}
.footer-banner h2 {
    color: white;
    font-size: 1.8rem;
    font-weight: 800;
    letter-spacing: 1px;
    text-shadow: 2px 2px 4px rgba(0,0,0,0.6);
    margin: 0;
}
.footer-banner p {
    margin-top: 10px;
    color: #ffe6e6;
    font-size: 1rem;
    text-shadow: 1px 1px 2px rgba(0,0,0,0.5);
}
</style>

<div class="footer-banner">
    <h2>📊 Report Submitted by Praveen</h2>
    <p>© 2025 All Rights Reserved | Designed with Streamlit</p>
</div>
""", unsafe_allow_html=True)

 
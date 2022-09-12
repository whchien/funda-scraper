import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

sold = pd.read_csv("test_data/merged/merged_sold.csv", lineterminator='\n')
sold["date_list"] = pd.to_datetime(sold["date_list"])
sold["date_sold"] = pd.to_datetime(sold["date_sold"])

rent = pd.read_csv("test_data/merged/merged_rent.csv", lineterminator='\n')
rent["date_list"] = pd.to_datetime(rent["date_list"])
rent["date_sold"] = pd.to_datetime(rent["date_sold"])

st.title("House Price Trend in Netherlands 🇳🇱")


# Sidebar
market_map = {"Buy": sold, "Rent": rent}
market = st.sidebar.selectbox("Looking for?", market_map.keys())
if market:
    df = market_map[market]
    df = df[df["year_list"] >= 2020]
    df = df[df["price"] <= 2000000]
    df = df[df["living_area"] != 0]
    df = df[df["city"].isin(["amsterdam", "utrecht", "den-haag", "rotterdam"])]

cities = st.sidebar.multiselect("City", df.city.unique().tolist())
if len(cities) != 0:
    df = df[df.city.isin(cities)]

n_rooms = st.sidebar.slider("Rooms", max_value=10, value=(0, 10))
if n_rooms:
    df = df[(df.room <= int(max(n_rooms))) & (df.room >= int(min(n_rooms)))]

cust_price = st.sidebar.slider(
    "Budget", value=(int(df.price.min()), int(df.price.max()))
)
if cust_price:
    df = df[(df.price <= int(max(cust_price))) & (df.price >= int(min(cust_price)))]

cust_area = st.sidebar.slider(
    "Living area (m2)", value=(int(df.living_area.min()), int(df.living_area.max()))
)
if cust_price:
    df = df[
        (df.living_area <= int(max(cust_area)))
        & (df.living_area >= int(min(cust_area)))
    ]

age = st.sidebar.text_input("Max house age", "50")
if age:
    df = df[(df.house_age <= int(age))]

bool_map = {
    "All": ["huis", "appartement"],
    "Appartement": ["appartement"],
    "Huis": ["huis"],
}
house_type = st.sidebar.selectbox("House type", bool_map.keys())
if len(house_type) != 0:
    df = df[df.house_type.isin(bool_map[house_type])]

bool_map = {
    "All": ["Resale property", "New property"],
    "Resale property": ["Resale property"],
    "New property": ["New property"],
}
building_type = st.sidebar.selectbox("Building type", bool_map.keys())
if len(building_type) != 0:
    df = df[df.building_type.isin(bool_map[building_type])]

bool_map = {"Does not matter": [1, 0], "Yes": [1], "No": [0]}
balcony = st.sidebar.selectbox("Balcony", bool_map.keys())
if len(balcony) != 0:
    df = df[df.has_balcony.isin(bool_map[balcony])]

garden = st.sidebar.selectbox("Garden", bool_map.keys())
if len(garden) != 0:
    df = df[df.has_garden.isin(bool_map[garden])]

label_list = list(df.energy_label.unique())
label_list.sort()
labels = st.sidebar.multiselect(
    "Energy label", label_list
)
if len(labels) != 0:
    df = df[df.energy_label.isin(labels)]


# General information
tab1, tab2, tab3 = st.tabs(["General Figures", "Data Visualization", "About"])

with tab1:
    # st.markdown("""---""")
    st.subheader("General Figures")
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric(label="Total items", value=df.shape[0])
    delta = df.date_list.max() - df.date_list.min()
    days_to_list = round(int(df.shape[0]) / max(1, int(delta.days)), 1)
    col2.metric(label="New listing per day", value=days_to_list)
    col3.metric(label="Median house price", value=int(np.median(df.price)))
    col4.metric(label="Avg day to sell", value=round(np.average(df.dropna().term_days), 1))
    col5.metric(
        label="Avg house age", value=int(np.average(df[df.house_age < 500].house_age))
    )

# Data visualization
with tab2:
    st.subheader("Data Visualization")
    filter_lists = ["house_type", "building_type", "city"]
    filter = st.selectbox("Choose one additional feature to analyze:", filter_lists)

    df["count"] = 1
    df['year_list'] = df['year_list'].astype(int)
    fig = px.histogram(
        df,
        x="year_list",
        y='count',
        color=filter,
        barmode='group',
        text_auto='.',
        title="Count of houses",
    )
    st.plotly_chart(fig, use_container_width=True)

    tmp = df.groupby(["ym_list", filter]).median().reset_index()
    fig = px.line(
        tmp,
        x="ym_list",
        y="price",
        color=filter,
        labels={"ym_list": "Listed Date",
                "price": "Total Price(€)"},
        title="Median house price",
    )
    st.plotly_chart(fig, use_container_width=True)

    fig = px.line(
        tmp,
        x="ym_list",
        y="price_m2",
        color=filter,
        labels={"ym_list": "Listed Date",
                "price_m2": "Price per m2 (€)"},
        title="Median house price per m2",
    )
    st.plotly_chart(fig, use_container_width=True)

    fig = px.scatter(
        df,
        x="date_list",
        y="price",
        trendline="ols",
        color=filter,
        labels={"date_list": "Listed Date",
                "price": "Total Price (€)"},
        title="Scatter plot of house price per m2",
    )
    st.plotly_chart(fig, use_container_width=True)

    fig = px.scatter(
        df,
        x="date_list",
        y="price_m2",
        trendline="ols",
        color=filter,
        labels={"date_list": "Listed Date",
                "price": "Total Price (€)"},
        title="Scatter plot of house price per m2",
    )
    st.plotly_chart(fig, use_container_width=True)

    fig = px.histogram(
        df,
        x="price",
        marginal="box",
        color=filter,
        nbins=25,
        hover_data=df.columns,
        labels={"price": "Total Price (€)"},
        title="Distribution of house price",
    )
    st.plotly_chart(fig, use_container_width=True)

    fig = px.density_heatmap(
        df,
        x="price",
        y="house_age",
        marginal_x="histogram",
        marginal_y="histogram",
        title="Density heatmap of house price per m2 during 2020-2022",
    )
    st.plotly_chart(fig, use_container_width=True)


with tab3:
    st.subheader("About me")
    st.markdown("Hi, my name is Will Chien, a data scientist working in Amsterdam.")
    st.markdown("I developed this dashboard based on the data i scraped from [Funda](https://https://www.funda.nl/). "
                "If you find these insights useful, please share it with your friends or colleagues. "
                "Good luck on finding your dream house!")

    st.subheader("Contact")
    st.markdown("For any feedbacks or further collaboration, "
                "please reach out via e-mail (locriginal@gmail.com) or [LinkedIn](https://https://www.funda.nl/)."
                "If you are a data scientist, you can try the code yourself [here](https://github.com/whchien)."
                "Please star the project if you found this handy."
    )
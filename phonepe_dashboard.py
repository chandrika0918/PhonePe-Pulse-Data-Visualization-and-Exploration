import streamlit as st
import json
import pandas as pd
import mysql.connector
from mysql.connector import Error
from streamlit_option_menu import option_menu
import plotly.express as px
import PIL
from PIL import Image
import requests

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='phonepe_data'
    )
mycursor = mydb.cursor() 

def ques_1():
    query_1 = '''SELECT DISTINCT States, Year, SUM(Transaction_Amount) as Total_Transaction_Amount FROM aggregated_transaction GROUP BY States,Year
                ORDER BY Total_Transaction_Amount DESC LIMIT 10'''
    mycursor.execute(query_1)
    sql_ans1 = mycursor.fetchall()
    df1 = pd.DataFrame(sql_ans1, columns=['State name','Year','Total_Transaction_Amount'])
    # Set a custom index with a name
    df1.index = pd.RangeIndex(start=1, stop=len(df1) + 1, name='S.No')
    return df1

def ques_2():
    query_2 = '''SELECT States, Year, SUM(Transaction_Amount) as Total_Transaction_Amount FROM aggregated_transaction GROUP BY States,Year
                ORDER BY Total_Transaction_Amount ASC LIMIT 10'''
    mycursor.execute(query_2)
    sql_ans2 = mycursor.fetchall()
    df2 = pd.DataFrame(sql_ans2, columns=['State name','Year','Total_Transaction_Amount'])
    # Set a custom index with a name
    df2.index = pd.RangeIndex(start=1, stop=len(df2) + 1, name='S.No')
    return df2

def ques_3():
    query_3 = '''SELECT States, District, SUM(Registered_user) as Total_Registered_User FROM map_user GROUP BY States, District
                ORDER BY States, District DESC LIMIT 10'''
    mycursor.execute(query_3)
    sql_ans3 = mycursor.fetchall()
    df3 = pd.DataFrame(sql_ans3, columns=['State name','District','Total_Registered_User'])
    # Set a custom index with a name
    df3.index = pd.RangeIndex(start=1, stop=len(df3) + 1, name='S.No')
    return df3
def ques_4():
    query_4 = '''SELECT States, District, SUM(Registered_user) as Total_Registered_User FROM map_user GROUP BY States, District
                ORDER BY States, District ASC LIMIT 10'''
    mycursor.execute(query_4)
    sql_ans4 = mycursor.fetchall()
    df4 = pd.DataFrame(sql_ans4, columns=['State name','District','Total_Registered_User'])
    # Set a custom index with a name
    df4.index = pd.RangeIndex(start=1, stop=len(df4) + 1, name='S.No')
    return df4
def ques_5():
    query_5 = '''SELECT Districts, SUM(M_Transaction_Amount) as Total_Transaction_Amount FROM map_transaction GROUP BY States
                ORDER BY Total_Transaction_Amount DESC LIMIT 10'''
    mycursor.execute(query_5)
    sql_ans5 = mycursor.fetchall()
    df5 = pd.DataFrame(sql_ans5, columns=['District','Total_Transaction_Amount'])
    # Set a custom index with a name
    df5.index = pd.RangeIndex(start=1, stop=len(df5) + 1, name='S.No')
    return df5
def ques_6():
    query_6 = '''SELECT Districts, SUM(M_Transaction_Amount) as Total_Transaction_Amount FROM map_transaction GROUP BY States
                ORDER BY Total_Transaction_Amount ASC LIMIT 10'''
    mycursor.execute(query_6)
    sql_ans6 = mycursor.fetchall()
    df6 = pd.DataFrame(sql_ans6, columns=['District','Total_Transaction_Amount'])
    # Set a custom index with a name
    df6.index = pd.RangeIndex(start=1, stop=len(df6) + 1, name='S.No')
    return df6
def ques_7():
    query_7 = '''SELECT Districts, SUM(M_Transaction_Count) as Total_Transaction_Count FROM map_transaction GROUP BY States
                ORDER BY Total_Transaction_Count DESC LIMIT 10'''
    mycursor.execute(query_7)
    sql_ans7 = mycursor.fetchall()
    df7 = pd.DataFrame(sql_ans7, columns=['District','Total_Transaction_Count'])
    # Set a custom index with a name
    df7.index = pd.RangeIndex(start=1, stop=len(df7) + 1, name='S.No')
    return df7
def ques_8():
    query_8 = '''SELECT Districts, SUM(M_Transaction_Count) as Total_Transaction_Count FROM map_transaction GROUP BY States
                ORDER BY Total_Transaction_Count ASC LIMIT 10'''
    mycursor.execute(query_8)
    sql_ans8 = mycursor.fetchall()
    df8 = pd.DataFrame(sql_ans8, columns=['District','Total_Transaction_Count'])
    # Set a custom index with a name
    df8.index = pd.RangeIndex(start=1, stop=len(df8) + 1, name='S.No')
    return df8
def ques_9():
    query_9 = '''SELECT DISTINCT Transaction_type, SUM(Transaction_Amount) as Total_Transaction_Amount FROM aggregated_transaction GROUP BY Transaction_type
                ORDER BY Total_Transaction_Amount DESC'''
    mycursor.execute(query_9)
    sql_ans9 = mycursor.fetchall()
    df9 = pd.DataFrame(sql_ans9, columns=['Transaction_Type','Total_Transaction_Amount'])
    # Set a custom index with a name
    df9.index = pd.RangeIndex(start=1, stop=len(df9) + 1, name='S.No')
    return df9
def ques_10():
    query_10 = '''SELECT DISTINCT Brands, SUM(Transaction_Count) as Total_Transaction_Count FROM aggregated_users GROUP BY Brands
                ORDER BY Total_Transaction_Count DESC LIMIT 10'''
    mycursor.execute(query_10)
    sql_ans10 = mycursor.fetchall()
    df10 = pd.DataFrame(sql_ans10, columns=['Brands','Total_Transaction_Count'])
    # Set a custom index with a name
    df10.index = pd.RangeIndex(start=1, stop=len(df10) + 1, name='S.No')
    return df10

# Streamlit App
def main():
    st.set_page_config(page_title='PhonePe Pulse', page_icon=':bar_chart:', layout="wide")
    st.title("PhonePe Pulse Data Visualization and Exploration")
    with st.sidebar:
        menu = option_menu("PhonePe Pulse APP", ["About", 'Home', 'Analysis', 'Insights'],icons=['bar-chart', 'house','toggles','at'], menu_icon="bar_chart", default_index=0)
    if menu == "About":
        st.subheader("PhonePe  is an Indian digital payments and financial technology company headQuatered in Bengaluru, Karnataka, India. PhonePe was founded in December 2015, by Sameer Nigam, Rahul Chari and Burzin Engineer. The PhonePe app, based on the Unified Payments Interface (UPI), went live in August 2016. It is owned by Flipkart, a subsidiary of Walmart.")
        st.markdown("[DOWNLOAD APP](https://www.phonepe.com/app-download/)")    
    elif menu == "Home":
        st.title(':violet[PHONEPE PULSE DATA VISUALISATION]')
        st.subheader(':violet[Phonepe Pulse]:')
        st.write('PhonePe Pulse is a feature offered by the Indian digital payments platform called PhonePe.PhonePe Pulse provides users with insights and trends related to their digital transactions and usage patterns on the PhonePe app.')
        st.subheader(':violet[Phonepe Pulse Data Visualisation]:')
        st.write('Data visualization refers to the graphical representation of data using charts, graphs, and other visual elements to facilitate understanding and analysis in a visually appealing manner.'
                    'The goal is to extract this data and process it to obtain insights and information that can be visualized in a user-friendly manner.')
        st.markdown("## :violet[Done by] : VIJAYACHANDRIKA R")
        st.markdown("[Inspired from](https://www.phonepe.com/pulse/)")
        st.markdown("[Githublink](https://github.com/chandrika0918)")
        st.markdown("[LinkedIn](https://www.linkedin.com/in/vijayachandrika-r-bb554023a/)")
        st.write("---")
    elif menu == "Analysis":
        st.title(':violet[ANALYSIS]')
        st.subheader('Analysis done on the basis of All India between 2018 and 2024')
        tab1, tab2, tab3 = st.tabs(["AGGREGATED","MAP","TOP"])
        with tab1:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                in_method = st.selectbox('**Select Method**', ('TRANSACTION', 'USER', 'INSURANCE'), key='in_method')
            with col2:
                if in_method == "TRANSACTION":
                    trans = 'aggregated_transaction'
                elif in_method == "USER":
                    trans = 'aggregated_users'
                elif in_method == "INSURANCE":
                    trans = 'aggregated_insurance'
                get_year_query  = f"SELECT DISTINCT Year FROM {trans};"
                mycursor.execute(get_year_query )
                years = mycursor.fetchall()
                get_year_options = [year[0] for year in years]
                in_trans_yr = st.selectbox('**Select Year**',get_year_options, format_func=lambda x: x, key='in_trans_yr')
            with col3:
                    in_trans_qtr = st.selectbox('**Select qtr**', ('1', '2', '3', '4'), key='in_trans_qtr')
            if in_method == "TRANSACTION":
                with col4:
                    in_trans_type = st.selectbox('**Select type**', ('Recharge & bill payments', 'Peer-to-peer payments', 'Merchant payments','Financial Services','Others'), key='in_trans_type')
                query = "SELECT States, Transaction_type, Transaction_Count, Transaction_Amount FROM aggregated_transaction WHERE Year = %s AND Quater = %s AND Transaction_type = %s"
                mycursor.execute(query, (in_trans_yr, in_trans_qtr,in_trans_type))
                rest_trans_data = mycursor.fetchall()
                df_rest_trans_data = pd.DataFrame(rest_trans_data, columns=['State','Transaction_type','Transaction_Count', 'Transaction_amount'])
                df_rest_trans_data = df_rest_trans_data.set_index(pd.Index(range(1, len(df_rest_trans_data) + 1)))
                st.write(df_rest_trans_data)
                col1, col2= st.columns(2)
                with col1:
                    fig = px.bar(df_rest_trans_data, x='State', y='Transaction_amount')
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    # Load GeoJSON file for India states
                    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                    response = requests.get(geojson_url)
                    india_states_geojson = response.json()
                    fig_map = px.choropleth(
                                    df_rest_trans_data,
                                    geojson=india_states_geojson,
                                    featureidkey='properties.ST_NM',
                                    locations='State',
                                    color='Transaction_amount',
                                    hover_name='State'
                                )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    # Show map
                    st.plotly_chart(fig_map, use_container_width=True)
            elif in_method == "USER":
                with col4:
                    get_brand_query  = f"SELECT DISTINCT Brands FROM aggregated_users;"
                    mycursor.execute(get_brand_query )
                    brands = mycursor.fetchall()
                    get_brand_options = [brand[0] for brand in brands]
                    in_brands = st.selectbox('**Select Year**',get_brand_options, format_func=lambda x: x, key='in_brands')
                query = "SELECT States,Brands,Transaction_Count,Percentage FROM aggregated_users WHERE Year = %s AND Quater = %s AND Brands = %s"
                mycursor.execute(query, (in_trans_yr, in_trans_qtr, in_brands))
                rest_user_data = mycursor.fetchall()
                df_rest_user_data = pd.DataFrame(rest_user_data, columns=['State','Brands','Transaction_Count', 'Percentage'])
                df_rest_user_data = df_rest_user_data.set_index(pd.Index(range(1, len(df_rest_user_data) + 1)))
                st.write(df_rest_user_data)
                col1, col2= st.columns(2)
                with col1:
                    fig = px.bar(df_rest_user_data, x='State', y='Transaction_Count')
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    # Load GeoJSON file for India states
                    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                    response = requests.get(geojson_url)
                    india_states_geojson = response.json()
                    fig_map = px.choropleth(
                                    df_rest_user_data,
                                    geojson=india_states_geojson,
                                    featureidkey='properties.ST_NM',
                                    locations='State',
                                    color='Transaction_Count',
                                    hover_name='State'
                                )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    # Show map
                    st.plotly_chart(fig_map, use_container_width=True)
            elif in_method == "INSURANCE":
                query = "SELECT States,Aggre_Insurance_Count,Aggre_Insurance_Amount FROM aggregated_insurance WHERE Year = %s AND Quater = %s"
                mycursor.execute(query, (in_trans_yr, in_trans_qtr))
                rest_ins_data = mycursor.fetchall()
                df_rest_ins_data = pd.DataFrame(rest_ins_data, columns=['State','Transaction_Count', 'Transaction_amount'])
                df_rest_ins_data = df_rest_ins_data.set_index(pd.Index(range(1, len(df_rest_ins_data) + 1)))
                st.write(df_rest_ins_data) 
                col1, col2= st.columns(2)
                with col1:
                    fig = px.bar(df_rest_ins_data, x='State', y='Transaction_Count')
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    # Load GeoJSON file for India states
                    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                    response = requests.get(geojson_url)
                    india_states_geojson = response.json()
                    fig_map = px.choropleth(
                                    df_rest_ins_data,
                                    geojson=india_states_geojson,
                                    featureidkey='properties.ST_NM',
                                    locations='State',
                                    color='Transaction_Count',
                                    hover_name='State'
                                )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    # Show map
                    st.plotly_chart(fig_map, use_container_width=True)
        with tab2:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                in_method_2 = st.selectbox('**Select Method**', ('TRANSACTION', 'USER', 'INSURANCE'), key='in_method_2')
            with col2:
                if in_method_2 == "TRANSACTION":
                    trans = 'map_transaction'
                elif in_method_2 == "USER":
                    trans = 'map_user'
                elif in_method_2 == "INSURANCE":
                    trans = 'map_insurance'
                get_year_query  = f"SELECT DISTINCT Year FROM {trans};"
                mycursor.execute(get_year_query )
                years = mycursor.fetchall()
                get_year_options = [year[0] for year in years]
                M_in_trans_yr = st.selectbox('**Select Year**',get_year_options, format_func=lambda x: x, key='M_in_trans_yr')
            with col3:
                M_in_trans_qtr = st.selectbox('**Select qtr**', ('1', '2', '3', '4'), key='M_in_trans_qtr')
            
            if in_method_2 == "TRANSACTION":
                query = "SELECT States, SUM(M_Transaction_Count) as Total_Transaction_Count, SUM(M_Transaction_Amount) as Total_Transaction_Amount FROM map_transaction WHERE Year = %s AND Quater = %s GROUP BY States"
                mycursor.execute(query, (M_in_trans_yr, M_in_trans_qtr))
                rest_trans_data = mycursor.fetchall()
                df_rest_trans_data = pd.DataFrame(rest_trans_data, columns=['State','Total_Transaction_Count', 'Total_Transaction_Amount'])
                df_rest_trans_data = df_rest_trans_data.set_index(pd.Index(range(1, len(df_rest_trans_data) + 1)))
                st.write(df_rest_trans_data)
                col1, col2= st.columns(2)
                with col1:
                    fig = px.bar(df_rest_trans_data, x='State', y='Total_Transaction_Amount')
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    # Load GeoJSON file for India states
                    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                    response = requests.get(geojson_url)
                    india_states_geojson = response.json()
                    fig_map = px.choropleth(
                                    df_rest_trans_data,
                                    geojson=india_states_geojson,
                                    featureidkey='properties.ST_NM',
                                    locations='State',
                                    color='Total_Transaction_Count',
                                    hover_name='State'
                                )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    # Show map
                    st.plotly_chart(fig_map, use_container_width=True)
            elif in_method_2 == "USER":
                query = "SELECT States,Registered_user,App_opens FROM map_user WHERE Year = %s AND Quater = %s"
                mycursor.execute(query, (M_in_trans_yr, M_in_trans_qtr))
                rest_user_data = mycursor.fetchall()
                df_rest_user_data = pd.DataFrame(rest_user_data, columns=['State','Registered_user', 'App_opens'])
                df_rest_user_data = df_rest_user_data.set_index(pd.Index(range(1, len(df_rest_user_data) + 1)))
                st.write(df_rest_user_data)
                col1, col2= st.columns(2)
                with col1:
                    fig = px.bar(df_rest_user_data, x='State', y='Registered_user')
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    # Load GeoJSON file for India states
                    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                    response = requests.get(geojson_url)
                    india_states_geojson = response.json()
                    fig_map = px.choropleth(
                                    df_rest_user_data,
                                    geojson=india_states_geojson,
                                    featureidkey='properties.ST_NM',
                                    locations='State',
                                    color='Registered_user',
                                    hover_name='State'
                                )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    # Show map
                    st.plotly_chart(fig_map, use_container_width=True)
            elif in_method_2 == "INSURANCE":
                query = "SELECT States,SUM(Map_Insurance_Count) as Total_Map_Insurance_Count,SUM(Map_Insurance_Amount) as Total_Map_Insurance_Amount FROM map_insurance WHERE Year = %s AND Quater = %s GROUP BY States"
                mycursor.execute(query, (M_in_trans_yr, M_in_trans_qtr))
                rest_ins_data = mycursor.fetchall()
                df_rest_ins_data = pd.DataFrame(rest_ins_data, columns=['State','Total_Map_Insurance_Count', 'Total_Map_Insurance_Amount'])
                df_rest_ins_data = df_rest_ins_data.set_index(pd.Index(range(1, len(df_rest_ins_data) + 1)))
                st.write(df_rest_ins_data) 
                col1, col2= st.columns(2)
                with col1:
                    fig = px.bar(df_rest_ins_data, x='State', y='Total_Map_Insurance_Count')
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    # Load GeoJSON file for India states
                    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                    response = requests.get(geojson_url)
                    india_states_geojson = response.json()
                    fig_map = px.choropleth(
                                    df_rest_ins_data,
                                    geojson=india_states_geojson,
                                    featureidkey='properties.ST_NM',
                                    locations='State',
                                    color='Total_Map_Insurance_Count',
                                    hover_name='State'
                                )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    # Show map
                    st.plotly_chart(fig_map, use_container_width=True)  
        with tab3:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                in_method_3 = st.selectbox('**Select Method**', ('TRANSACTION', 'USER', 'INSURANCE'), key='in_method_3')
            with col2:
                if in_method_3 == "TRANSACTION":
                    trans = 'top_transaction'
                elif in_method_3 == "USER":
                    trans = 'top_user'
                elif in_method_3 == "INSURANCE":
                    trans = 'top_insurance'
                get_year_query  = f"SELECT DISTINCT Year FROM {trans};"
                mycursor.execute(get_year_query )
                years = mycursor.fetchall()
                get_year_options = [year[0] for year in years]
                T_in_trans_yr = st.selectbox('**Select Year**',get_year_options, format_func=lambda x: x, key='T_in_trans_yr')
            with col3:
                T_in_trans_qtr = st.selectbox('**Select qtr**', ('1', '2', '3', '4'), key='T_in_trans_qtr')
            
            if in_method_3 == "TRANSACTION":
                query = "SELECT States, SUM(T_Transaction_Count) as Total_Transaction_Count, SUM(T_Transaction_Amount) as Total_Transaction_Amount FROM top_transaction WHERE Year = %s AND Quater = %s GROUP BY States"
                mycursor.execute(query, (T_in_trans_yr, T_in_trans_qtr))
                rest_trans_data = mycursor.fetchall()
                df_rest_trans_data = pd.DataFrame(rest_trans_data, columns=['State','Total_Transaction_Count', 'Total_Transaction_Amount'])
                df_rest_trans_data = df_rest_trans_data.set_index(pd.Index(range(1, len(df_rest_trans_data) + 1)))
                st.write(df_rest_trans_data)
                col1, col2= st.columns(2)
                with col1:
                    fig = px.bar(df_rest_trans_data, x='State', y='Total_Transaction_Amount')
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    # Load GeoJSON file for India states
                    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                    response = requests.get(geojson_url)
                    india_states_geojson = response.json()
                    fig_map = px.choropleth(
                                    df_rest_trans_data,
                                    geojson=india_states_geojson,
                                    featureidkey='properties.ST_NM',
                                    locations='State',
                                    color='Total_Transaction_Count',
                                    hover_name='State'
                                )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    # Show map
                    st.plotly_chart(fig_map, use_container_width=True)
            elif in_method_3 == "USER":
                query = "SELECT States,SUM(Registered_user) as Total_Registered_user,Pincodes FROM top_user WHERE Year = %s AND Quater = %s GROUP BY States"
                mycursor.execute(query, (T_in_trans_yr, T_in_trans_qtr))
                rest_user_data = mycursor.fetchall()
                df_rest_user_data = pd.DataFrame(rest_user_data, columns=['State','Total_Registered_user', 'Pincodes'])
                df_rest_user_data = df_rest_user_data.set_index(pd.Index(range(1, len(df_rest_user_data) + 1)))
                st.write(df_rest_user_data)
                col1, col2= st.columns(2)
                with col1:
                    fig = px.bar(df_rest_user_data, x='State', y='Total_Registered_user')
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    # Load GeoJSON file for India states
                    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                    response = requests.get(geojson_url)
                    india_states_geojson = response.json()
                    fig_map = px.choropleth(
                                    df_rest_user_data,
                                    geojson=india_states_geojson,
                                    featureidkey='properties.ST_NM',
                                    locations='State',
                                    color='Total_Registered_user',
                                    hover_name='State'
                                )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    # Show map
                    st.plotly_chart(fig_map, use_container_width=True)
            elif in_method_3 == "INSURANCE":
                query = "SELECT States,SUM(Top_Insurance_T_Count) as Total_Insurance_Count,SUM(Top_Insurance_T_Amount) as Total_Insurance_Amount FROM top_insurance WHERE Year = %s AND Quater = %s GROUP BY States"
                mycursor.execute(query, (T_in_trans_yr, T_in_trans_qtr))
                rest_ins_data = mycursor.fetchall()
                df_rest_ins_data = pd.DataFrame(rest_ins_data, columns=['State','Total_Insurance_Count', 'Total_Insurance_Amount'])
                df_rest_ins_data = df_rest_ins_data.set_index(pd.Index(range(1, len(df_rest_ins_data) + 1)))
                st.write(df_rest_ins_data) 
                col1, col2= st.columns(2)
                with col1:
                    fig = px.bar(df_rest_ins_data, x='State', y='Total_Map_Insurance_Count')
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    # Load GeoJSON file for India states
                    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                    response = requests.get(geojson_url)
                    india_states_geojson = response.json()
                    fig_map = px.choropleth(
                                    df_rest_ins_data,
                                    geojson=india_states_geojson,
                                    featureidkey='properties.ST_NM',
                                    locations='State',
                                    color='Total_Insurance_Count',
                                    hover_name='State'
                                )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    # Show map
                    st.plotly_chart(fig_map, use_container_width=True)  
    elif menu == "Insights":
        st.title(':violet[BASIC INSIGHTS]')
        st.subheader("The basic insights are derived from the Analysis of the Phonepe Pulse data. It provides a clear idea about the analysed data.")
        options = ["--select--",
               "1. Top 10 states based on year and amount of transaction",
               "2. Least 10 states based on year and amount of transaction",
               "3. Top 10 States and Districts based on Registered Users",
               "4. Least 10 States and Districts based on Registered Users",
               "5. Top 10 Districts based on the Transaction Amount",
               "6. Least 10 Districts based on the Transaction Amount",
               "7. Top 10 Districts based on the Transaction count",
               "8. Least 10 Districts based on the Transaction count",
               "9. Top Transaction types based on the Transaction Amount",
               "10. Top 10 Mobile Brands based on the User count of transaction"]
        select_question = st.selectbox(":violet[Select the option]",options)

        if select_question == '1. Top 10 states based on year and amount of transaction':
            q_df1 = ques_1()
            st.write(q_df1)
        elif select_question == '2. Least 10 states based on year and amount of transaction':
            st.write(ques_2())
        elif select_question == '3. Top 10 States and Districts based on Registered Users':
            st.write(ques_3())
        elif select_question == '4. Least 10 States and Districts based on Registered Users':
            st.write(ques_4())
        elif select_question == '5. Top 10 Districts based on the Transaction Amount':
            st.write(ques_5())
        elif select_question == '6. Least 10 Districts based on the Transaction Amount':
            st.write(ques_6())
        elif select_question == '7. Top 10 Districts based on the Transaction count':
            st.write(ques_7())
        elif select_question == '8. Least 10 Districts based on the Transaction count':
            st.write(ques_8())
        elif select_question == '9. Top Transaction types based on the Transaction Amount':
            st.write(ques_9())
        elif select_question == '10. Top 10 Mobile Brands based on the User count of transaction':
            st.write(ques_10())

# Call the main function at the end
if __name__ == "__main__":
    main()
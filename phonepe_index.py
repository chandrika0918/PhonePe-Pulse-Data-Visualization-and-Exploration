import git
import os
import json
import pandas as pd
import mysql.connector
from mysql.connector import Error

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database='phonepe_data'
    )
mycursor = mydb.cursor() 

# Data Collect form github

git.Repo.clone_from("https://github.com/PhonePe/pulse.git", 'phonepe_pulse_git')

# Data Extraction

# AGGREGATE DATA --> TRANSACTION

Aggre_path_1 = "G:/CHANDRIKA/data_science/projects/phonepe_pluse/pulse/data/aggregated/transaction/country/india/state/"
aggre_state_list = os.listdir(Aggre_path_1)
Aggre_clm_1 = {"States":[], "Year":[], "Quater":[], "Transaction_type":[], "Transaction_Count":[], "Transaction_Amount":[]}
for state in aggre_state_list:
    present_state = Aggre_path_1+state+"/"
    aggre_year_list = os.listdir(present_state)

    for year in aggre_year_list:
        present_year = present_state+year+"/"
        aggre_file_list = os.listdir(present_year)

        for file in aggre_file_list:
            present_file = present_year+file
            jsn_file_info = open(present_file,"r")
            A = json.load(jsn_file_info)

            for i in A['data']['transactionData']:
                Name = i['name']
                Count = i['paymentInstruments'][0]['count']
                Amount = i['paymentInstruments'][0]['amount']
                Aggre_clm_1['Transaction_type'].append(Name)
                Aggre_clm_1['Transaction_Count'].append(Count)
                Aggre_clm_1['Transaction_Amount'].append(Amount)
                Aggre_clm_1['States'].append(state)
                Aggre_clm_1['Year'].append(year)
                Aggre_clm_1['Quater'].append(int(file.strip('.json')))
Aggregrated_Transaction = pd.DataFrame(Aggre_clm_1)
Aggregrated_Transaction['States'] = Aggregrated_Transaction['States'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Aggregrated_Transaction['States'] = Aggregrated_Transaction['States'].str.replace('-',' ')
Aggregrated_Transaction['States'] = Aggregrated_Transaction['States'].str.title()
Aggregrated_Transaction['States'] = Aggregrated_Transaction['States'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

# AGGREGATE DATA --> USER

Aggre_path_2 = "G:/CHANDRIKA/data_science/projects/phonepe_pluse/pulse/data/aggregated/user/country/india/state/"
aggre_state_list = os.listdir(Aggre_path_2)
Aggre_clm_2 = {"States":[], "Year":[], "Quater":[], "Brands":[], "Transaction_Count":[], "Percentage":[]}
for state in aggre_state_list:
    present_state = Aggre_path_2+state+"/"
    aggre_year_list = os.listdir(present_state)

    for year in aggre_year_list:
        present_year = present_state+year+"/"
        aggre_file_list = os.listdir(present_year)

        for file in aggre_file_list:
            present_file = present_year+file
            jsn_file_info = open(present_file,"r")
            B = json.load(jsn_file_info)
            try:
                for i in B['data']['usersByDevice']:
                    Brand = i['brand']
                    Count = i['count']
                    Percentage = i['percentage']
                    Aggre_clm_2['Brands'].append(Brand)
                    Aggre_clm_2['Transaction_Count'].append(Count)
                    Aggre_clm_2['Percentage'].append(Percentage)
                    Aggre_clm_2['States'].append(state)
                    Aggre_clm_2['Year'].append(year)
                    Aggre_clm_2['Quater'].append(int(file.strip('.json')))
            except:
                pass
Aggregrated_User = pd.DataFrame(Aggre_clm_2)
Aggregrated_User['States'] = Aggregrated_User['States'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Aggregrated_User['States'] = Aggregrated_User['States'].str.replace('-',' ')
Aggregrated_User['States'] = Aggregrated_User['States'].str.title()
Aggregrated_User['States'] = Aggregrated_User['States'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

# AGGREGATE DATA --> INSURANCE

Aggre_path_3 = "G:/CHANDRIKA/data_science/projects/phonepe_pluse/pulse/data/aggregated/insurance/country/india/state/"
aggre_state_list = os.listdir(Aggre_path_3)
Aggre_clm_3 = {"States":[], "Year":[], "Quater":[], "Aggre_Insurance_Type":[], "Aggre_Insurance_Count":[], "Aggre_Insurance_Amount":[]}
for state in aggre_state_list:
    present_state = Aggre_path_3+state+"/"
    aggre_year_list = os.listdir(present_state)

    for year in aggre_year_list:
        present_year = present_state+year+"/"
        aggre_file_list = os.listdir(present_year)

        for file in aggre_file_list:
            present_file = present_year+file
            jsn_file_info = open(present_file,"r")
            C = json.load(jsn_file_info)
            
            for i in C['data']['transactionData']:
                name = i['name']
                count = i['paymentInstruments'][0]['count']
                amount = i['paymentInstruments'][0]['amount']
                Aggre_clm_3['Aggre_Insurance_Type'].append(name)
                Aggre_clm_3['Aggre_Insurance_Count'].append(count)
                Aggre_clm_3['Aggre_Insurance_Amount'].append(amount)
                Aggre_clm_3['States'].append(state)
                Aggre_clm_3['Year'].append(year)
                Aggre_clm_3['Quater'].append(int(file.strip('.json')))
Aggregrated_Insurance = pd.DataFrame(Aggre_clm_3)
Aggregrated_Insurance['States'] = Aggregrated_Insurance['States'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Aggregrated_Insurance['States'] = Aggregrated_Insurance['States'].str.replace('-',' ')
Aggregrated_Insurance['States'] = Aggregrated_Insurance['States'].str.title()
Aggregrated_Insurance['States'] = Aggregrated_Insurance['States'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

# MAP DATA --> TRANSACTION

Map_path_1 = "G:/CHANDRIKA/data_science/projects/phonepe_pluse/pulse/data/map/transaction/hover/country/india/state/"
map_state_list = os.listdir(Map_path_1)
Map_clm_1 = {"States":[], "Year":[], "Quater":[], "Districts":[], "M_Transaction_Count":[], "M_Transaction_Amount":[]}
for state in map_state_list:
    present_state = Map_path_1+state+"/"
    map_year_list = os.listdir(present_state)

    for year in map_year_list:
        present_year = present_state+year+"/"
        map_file_list = os.listdir(present_year)

        for file in map_file_list:
            present_file = present_year+file
            jsn_file_info = open(present_file,"r")
            D = json.load(jsn_file_info)

            for i in D['data']['hoverDataList']:
                Name = i['name']
                Amount = i['metric'][0]['amount']
                Count = i['metric'][0]['count']
                Map_clm_1['Districts'].append(Name)
                Map_clm_1['M_Transaction_Count'].append(Count)
                Map_clm_1['M_Transaction_Amount'].append(Amount)
                Map_clm_1['States'].append(state)
                Map_clm_1['Year'].append(year)
                Map_clm_1['Quater'].append(int(file.strip('.json')))
Map_Transaction = pd.DataFrame(Map_clm_1)
Map_Transaction['States'] = Map_Transaction['States'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Map_Transaction['States'] = Map_Transaction['States'].str.replace('-',' ')
Map_Transaction['States'] = Map_Transaction['States'].str.title()
Map_Transaction['States'] = Map_Transaction['States'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

# MAP DATA --> USER

Map_path_2 = "G:/CHANDRIKA/data_science/projects/phonepe_pluse/pulse/data/map/user/hover/country/india/state/"
map_state_list = os.listdir(Map_path_2)
Map_clm_2 = {"States":[], "Year":[], "Quater":[], "District":[], "Registered_user":[], "App_opens":[]}
for state in map_state_list:
    present_state = Map_path_2+state+"/"
    aggre_year_list = os.listdir(present_state)

    for year in aggre_year_list:
        present_year = present_state+year+"/"
        aggre_file_list = os.listdir(present_year)

        for file in aggre_file_list:
            present_file = present_year+file
            jsn_file_info = open(present_file,"r")
            E = json.load(jsn_file_info)

            try:
                for i in E['data']['hoverData'].items():
                    District = i[0]
                    Registered_user = i[1]['registeredUsers']
                    App_opens = i[1]['appOpens']
                    Map_clm_2['District'].append(District)
                    Map_clm_2['Registered_user'].append(Registered_user)
                    Map_clm_2['App_opens'].append(App_opens)
                    Map_clm_2['States'].append(state)
                    Map_clm_2['Year'].append(year)
                    Map_clm_2['Quater'].append(int(file.strip('.json')))
            except:
                pass
Map_User = pd.DataFrame(Map_clm_2)
Map_User['States'] = Map_User['States'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Map_User['States'] = Map_User['States'].str.replace('-',' ')
Map_User['States'] = Map_User['States'].str.title()
Map_User['States'] = Map_User['States'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

# MAP DATA --> INSURANCE

Map_path_3 = "G:/CHANDRIKA/data_science/projects/phonepe_pluse/pulse/data/map/insurance/hover/country/india/state/"
map_state_list = os.listdir(Map_path_3)
Map_clm_3 = {"States":[], "Year":[], "Quater":[], "Districts":[], "Map_Insurance_Count":[], "Map_Insurance_Amount":[]}
for state in map_state_list:
    present_state = Map_path_3+state+"/"
    aggre_year_list = os.listdir(present_state)

    for year in aggre_year_list:
        present_year = present_state+year+"/"
        aggre_file_list = os.listdir(present_year)

        for file in aggre_file_list:
            present_file = present_year+file
            jsn_file_info = open(present_file,"r")
            F = json.load(jsn_file_info)

            for i in F['data']['hoverDataList']:
                name = i['name']
                count = i['metric'][0]['count']
                amount = i['metric'][0]['amount']
                Map_clm_3['Districts'].append(name)
                Map_clm_3['Map_Insurance_Count'].append(count)
                Map_clm_3['Map_Insurance_Amount'].append(amount)
                Map_clm_3['States'].append(state)
                Map_clm_3['Year'].append(year)
                Map_clm_3['Quater'].append(int(file.strip('.json')))
Map_Insurance = pd.DataFrame(Map_clm_3)
Map_Insurance['States'] = Map_Insurance['States'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Map_Insurance['States'] = Map_Insurance['States'].str.replace('-',' ')
Map_Insurance['States'] = Map_Insurance['States'].str.title()
Map_Insurance['States'] = Map_Insurance['States'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

# TOP DATA --> TRANSACTION

Top_path_1 = "G:/CHANDRIKA/data_science/projects/phonepe_pluse/pulse/data/top/transaction/country/india/state/"
top_state_list = os.listdir(Top_path_1)
Top_clm_1 = {"States":[], "Year":[], "Quater":[], "T_Pincodes":[], "T_Transaction_Count":[], "T_Transaction_Amount":[]}
for state in top_state_list:
    present_state = Top_path_1+state+"/"
    top_year_list = os.listdir(present_state)

    for year in top_year_list:
        present_year = present_state+year+"/"
        top_file_list = os.listdir(present_year)

        for file in top_file_list:
            present_file = present_year+file
            jsn_file_info = open(present_file,"r")
            G = json.load(jsn_file_info)
            
            for i in G['data']['pincodes']:
                Name = i['entityName']  
                Amount = i['metric']['amount']
                Count = i['metric']['count']
                Top_clm_1['T_Pincodes'].append(Name)
                Top_clm_1['T_Transaction_Count'].append(Count)
                Top_clm_1['T_Transaction_Amount'].append(Amount)
                Top_clm_1['States'].append(state)
                Top_clm_1['Year'].append(year)
                Top_clm_1['Quater'].append(int(file.strip('.json')))
Top_Transaction = pd.DataFrame(Top_clm_1)
Top_Transaction['States'] = Top_Transaction['States'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Top_Transaction['States'] = Top_Transaction['States'].str.replace('-',' ')
Top_Transaction['States'] = Top_Transaction['States'].str.title()
Top_Transaction['States'] = Top_Transaction['States'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

# TOP DATA --> USER

Top_path_2 = "G:/CHANDRIKA/data_science/projects/phonepe_pluse/pulse/data/top/user/country/india/state/"
top_state_list = os.listdir(Top_path_2)
Top_clm_2 = {"States":[], "Year":[], "Quater":[], "Pincodes":[], "Registered_user":[]}
for state in top_state_list:
    present_state = Top_path_2+state+"/"
    aggre_year_list = os.listdir(present_state)

    for year in aggre_year_list:
        present_year = present_state+year+"/"
        aggre_file_list = os.listdir(present_year)

        for file in aggre_file_list:
            present_file = present_year+file
            jsn_file_info = open(present_file,"r")
            H = json.load(jsn_file_info)

            for i in H['data']['districts']:
                District = i['name']
                Registered_user = i['registeredUsers']
                Top_clm_2['Pincodes'].append(District)
                Top_clm_2['Registered_user'].append(Registered_user)
                Top_clm_2['States'].append(state)
                Top_clm_2['Year'].append(year)
                Top_clm_2['Quater'].append(int(file.strip('.json')))
Top_User = pd.DataFrame(Top_clm_2)
Top_User['States'] = Top_User['States'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Top_User['States'] = Top_User['States'].str.replace('-',' ')
Top_User['States'] = Top_User['States'].str.title()
Top_User['States'] = Top_User['States'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

# TOP DATA --> INSURANCE

Top_path_3 = "G:/CHANDRIKA/data_science/projects/phonepe_pluse/pulse/data/top/insurance/country/india/state/"
top_state_list = os.listdir(Top_path_3)
Top_clm_3 = {"States":[], "Year":[], "Quater":[], "Top_Insurance_Pincodes":[], "Top_Insurance_T_Count":[], "Top_Insurance_T_Amount":[]}
for state in top_state_list:
    present_state = Top_path_3+state+"/"
    aggre_year_list = os.listdir(present_state)

    for year in aggre_year_list:
        present_year = present_state+year+"/"
        aggre_file_list = os.listdir(present_year)

        for file in aggre_file_list:
            present_file = present_year+file
            jsn_file_info = open(present_file,"r")
            I = json.load(jsn_file_info)

            
            for i in I['data']['districts']:
                name = i['entityName']
                count = i['metric']['count']
                amount = i['metric']['amount']
                Top_clm_3['Top_Insurance_Pincodes'].append(name)
                Top_clm_3['Top_Insurance_T_Count'].append(count)
                Top_clm_3['Top_Insurance_T_Amount'].append(amount)
                Top_clm_3['States'].append(state)
                Top_clm_3['Year'].append(year)
                Top_clm_3['Quater'].append(int(file.strip('.json')))
Top_Insurance = pd.DataFrame(Top_clm_3)
Top_Insurance['States'] = Top_Insurance['States'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
Top_Insurance['States'] = Top_Insurance['States'].str.replace('-',' ')
Top_Insurance['States'] = Top_Insurance['States'].str.title()
Top_Insurance['States'] = Top_Insurance['States'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

#  =============     CONNECT SQL SERVER  /    CREAT TABLE    /    STORE DATA    ========  #
# ============ TABLE 1 ============ #
#create Table in sql db
aggre_trans_create_query = '''CREATE TABLE IF NOT EXISTS aggregated_transaction(
                                States varchar(255),Year int,Quater varchar(2),
                                Transaction_type varchar(255),Transaction_Count bigint,
                                Transaction_Amount float
                            )'''
mycursor.execute(aggre_trans_create_query)
mydb.commit()

# Insert Values to sql table 
aggre_trans_insert_query = '''INSERT INTO aggregated_transaction(States,Year,Quater,Transaction_type,
                                Transaction_Count,Transaction_Amount) VALUES(%s,%s,%s,%s,%s,%s)
                            '''
data = Aggregrated_Transaction.values.tolist()
mycursor.executemany(aggre_trans_insert_query,data)
mydb.commit()

# ============ TABLE 2 ============ #
#create Table in sql db
aggre_user_create_query = '''CREATE TABLE IF NOT EXISTS aggregated_users(States varchar(255),Year int,Quater varchar(2),
                                                                        Brands varchar(255),Transaction_Count bigint,
                                                                        Percentage float
                                                                        )'''
                                
mycursor.execute(aggre_user_create_query)
mydb.commit()

# Insert Values to sql table
aggre_user_insert_query = '''INSERT INTO aggregated_users(States,Year,Quater,Brands,
                                Transaction_Count,Percentage) VALUES(%s,%s,%s,%s,%s,%s)
                            '''
data = Aggregrated_User.values.tolist()
mycursor.executemany(aggre_user_insert_query,data)
mydb.commit()

# ============ TABLE 3 ============ #
#create Table in sql db
aggre_insurance_create_query = '''CREATE TABLE IF NOT EXISTS aggregated_insurance(States varchar(255),Year int,Quater varchar(2),
                                                                        Aggre_Insurance_Type varchar(255),Aggre_Insurance_Count bigint,
                                                                        Aggre_Insurance_Amount bigint
                                                                        )'''
                                
mycursor.execute(aggre_insurance_create_query)
mydb.commit()

# Insert Values to sql table
aggre_insurance_insert_query = '''INSERT INTO aggregated_insurance(States,Year,Quater,Aggre_Insurance_Type,
                                Aggre_Insurance_Count,Aggre_Insurance_Amount) VALUES(%s,%s,%s,%s,%s,%s)
                            '''
data = Aggregrated_Insurance.values.tolist()
mycursor.executemany(aggre_insurance_insert_query,data)
mydb.commit()

# ============ TABLE 4 ============ #
#create Table in sql db
map_transaction_create_query = '''CREATE TABLE IF NOT EXISTS map_transaction(States varchar(255),Year int,Quater varchar(2),
                                                                        Districts varchar(255),M_Transaction_Count bigint,
                                                                        M_Transaction_Amount bigint
                                                                        )'''
                                
mycursor.execute(map_transaction_create_query)
mydb.commit()

# Insert Values to sql table
map_transaction_insert_query = '''INSERT INTO map_transaction(States,Year,Quater,Districts,
                                M_Transaction_Count,M_Transaction_Amount) VALUES(%s,%s,%s,%s,%s,%s)
                            '''
data = Map_Transaction.values.tolist()
mycursor.executemany(map_transaction_insert_query,data)
mydb.commit()

# ============ TABLE 5 ============ #
#create Table in sql db
map_user_create_query = '''CREATE TABLE IF NOT EXISTS map_user(States varchar(255),Year int,Quater varchar(2),
                                                                        District varchar(255),Registered_user bigint,
                                                                        App_opens bigint
                                                                        )'''
                                
mycursor.execute(map_user_create_query)
mydb.commit()

# Insert Values to sql table
map_user_insert_query = '''INSERT INTO map_user(States,Year,Quater,District,
                                Registered_user,App_opens) VALUES(%s,%s,%s,%s,%s,%s)
                            '''
data = Map_User.values.tolist()
mycursor.executemany(map_user_insert_query,data)
mydb.commit()

# ============ TABLE 6 ============ #
#create Table in sql db
map_insurance_create_query = '''CREATE TABLE IF NOT EXISTS map_insurance(States varchar(255),Year int,Quater varchar(2),
                                                                        Districts varchar(255),Map_Insurance_Count bigint,
                                                                        Map_Insurance_Amount bigint
                                                                        )'''
                                
mycursor.execute(map_insurance_create_query)
mydb.commit()

# Insert Values to sql table
map_insurance_insert_query = '''INSERT INTO map_insurance(States,Year,Quater,Districts,
                                Map_Insurance_Count,Map_Insurance_Amount) VALUES(%s,%s,%s,%s,%s,%s)
                            '''
data = Map_Insurance.values.tolist()
mycursor.executemany(map_insurance_insert_query,data)
mydb.commit()

# ============ TABLE 7 ============ #
#create Table in sql db
top_transaction_create_query = '''CREATE TABLE IF NOT EXISTS top_transaction(States varchar(255),Year int,Quater varchar(2),
                                                                        T_Pincodes int,T_Transaction_Count bigint,
                                                                        T_Transaction_Amount bigint
                                                                        )'''
                                
mycursor.execute(top_transaction_create_query)
mydb.commit()

# Insert Values to sql table
top_transaction_insert_query = '''INSERT INTO top_transaction(States,Year,Quater,T_Pincodes,
                                T_Transaction_Count,T_Transaction_Amount) VALUES(%s,%s,%s,%s,%s,%s)
                            '''
data = Top_Transaction.values.tolist()
mycursor.executemany(top_transaction_insert_query,data)
mydb.commit()

# ============ TABLE 8 ============ #
#create Table in sql db
top_user_create_query = '''CREATE TABLE IF NOT EXISTS top_user(States varchar(255),Year int,Quater varchar(2),
                                                                        Pincodes int,Registered_user bigint
                                                                        )'''
                                
mycursor.execute(top_user_create_query)
mydb.commit()

# Insert Values to sql table
top_user_insert_query = '''INSERT INTO top_user(States,Year,Quater,Pincodes,
                                Registered_user) VALUES(%s,%s,%s,%s,%s)
                            '''
data = Top_User.values.tolist()
mycursor.executemany(top_user_insert_query,data)
mydb.commit()

# ============ TABLE 9 ============ #
#create Table in sql db
top_insurance_create_query = '''CREATE TABLE IF NOT EXISTS top_insurance(States varchar(255),Year int,Quater varchar(2),
                                                                        Top_Insurance_Pincodes int,Top_Insurance_T_Count bigint,
                                                                        Top_Insurance_T_Amount bigint
                                                                        )'''
                                
mycursor.execute(top_insurance_create_query)
mydb.commit()

# Insert Values to sql table
top_insurance_insert_query = '''INSERT INTO top_insurance(States,Year,Quater,Top_Insurance_Pincodes,
                            Top_Insurance_T_Count,Top_Insurance_T_Amount) VALUES(%s,%s,%s,%s,%s,%s)
                        '''
data = Top_Insurance.values.tolist()
mycursor.executemany(top_insurance_insert_query,data)
mydb.commit()

import streamlit as st
import numpy as np
import sharepy
import pandas as pd
import sys
import cx_Oracle as orcCon
import os
import datetime
import pandas as pd
import re


st.set_page_config(page_title='ETL', page_icon = 'logo.png', layout = 'wide', initial_sidebar_state = 'auto')
col1,col2 = st.columns([1,6])
with col1:
    st.image('logo.png')



try:
    orcCon.init_oracle_client(lib_dir=r"C:\Users\Hefatima\OneDrive - NUQUL Group\Desktop\instantclient_21_3")
    os.environ["TNS_ADMIN"] = (
        r"C:\Users\Hefatima\OneDrive - NUQUL Group\Desktop\instantclient_21_3\network\admin\Wallet_ADWPROD")
    os.environ['CLIENT_HOME'] = (r'C:\Users\Hefatima\OneDrive - NUQUL Group\Desktop\12.1.0\client_1')
    os.environ['LD_LIBRARY_PATH'] = (r'C:\Users\Hefatima\OneDrive - NUQUL Group\Desktop\12.1.0\client_1\lib')
    os.environ['PATH'] = (r'C:\Users\Hefatima\OneDrive - NUQUL Group\Desktop\12.1.0\client_1\lib')
    os.environ.get('TNS_ADMIN')
except:
    st.write("Connection to Oracle Database is done")


try:
    orcCon.init_oracle_client(
        lib_dir=r"C:\Users\GWadmin\Downloads\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3")
    os.environ["TNS_ADMIN"] = (
        r"C:\Users\GWadmin\Downloads\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3\network\admin\Wallet_ADWPROD")
    os.environ['CLIENT_HOME'] = (r'C:\Users\GWadmin\Downloads\12.1.0 - Copy\client_1')
    os.environ['LD_LIBRARY_PATH'] = (r'C:\Users\GWadmin\Downloads\12.1.0 - Copy\client_1\lib')
    os.environ['PATH'] = (r'C:\Users\GWadmin\Downloads\12.1.0 - Copy\client_1\lib')

except:
    st.write("Connection to Oracle Database is done")










st.write("Step 1 = Choose the correct database")
st.write("Step 2= Write the table")
st.write("Step 3 = Upload the file")


with col2:
    menu = ['BI_STAGE_CLOUD','BI_PROD_CLOUD']
    choice = st.sidebar.selectbox("Choose Database",options = menu)
if choice == "BI_STAGE_CLOUD":
    connection = orcCon.connect('BI_STAGE', 'WelC0me##123', 'adwprod_high')

elif choice == 'BI_PROD_CLOUD':
    connection = orcCon.connect('BI', 'WelC0me##123', 'adwprod_high')

user_input = st.text_input("Write the table name")



uploaded_file = st.file_uploader("Choose a csv file")

button = st.button('submit')
if button:
    dataframe = pd.read_csv(uploaded_file)
    dataframe = dataframe.astype(str)
    #st.write(dataframe)
    df1 = list(dataframe.columns.values.tolist())
    joined_string = ",".join(df1)
    joined_string = joined_string.replace("'", "")
    #print(joined_string)
    n = len(dataframe.columns)
    result = []
    for i in range(1, n + 1):
        result.append((i))


    def prepend(list, str):

        # Using format()
        str += '{0}'
        list = [str.format(i) for i in list]
        return (list)


    str = ':'
    number_list = prepend(result, str)
    cursor = connection.cursor()
    insert_table = "insert into  %s (%s) values (%s)" % (user_input, joined_string, number_list)
    insert_table = re.sub(r"[\[\]]", "", insert_table)
    insert_table = re.sub(r"[\'\]]", "", insert_table)
    #st.write(insert_table)

    df_list = dataframe.fillna('').values.tolist()
    #st.write(df_list)
    cursor.executemany(insert_table, df_list)


    cursor.close()
    connection.commit()
    connection.close()
    st.write("File has been uploaded")








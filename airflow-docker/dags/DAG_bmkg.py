from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

from datetime import datetime, timedelta,date
from sqlalchemy import create_engine
import pandas as pd
import requests
import json

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def Create_Table(ti):

    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    username = dag_var_bmkg["username"]
    password = dag_var_bmkg["password"]
    server = dag_var_bmkg["server"]
    port = dag_var_bmkg["port"]
    database = dag_var_bmkg["database"] 

    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)

    sql_create_tabel = engine.execute("create table if not exists gempa( "
                                            "tanggal date, "
                                            "jam time, "
                                            "date_time timestamptz, "
                                            "coordinates varchar(255), "
                                            "lintang varchar(255), "
                                            "bujur varchar(255), "
                                            "magnitude float, "
                                            "kedalaman varchar(255), "
                                            "wilayah varchar(255), "
                                            "potensi varchar(255), "
                                            "jam_wib time, "
                                            "send_email timestamp)"
                                            )

def Get_Web_Data(ti):    

    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    username = dag_var_bmkg["username"]
    password = dag_var_bmkg["password"]
    server = dag_var_bmkg["server"]
    port = dag_var_bmkg["port"]
    database = dag_var_bmkg["database"] 
    url_bmkg = dag_var_bmkg["link_bmkg"]

    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)

    df = pd.DataFrame(columns = ['Tanggal','Jam','DateTime','Coordinates','Lintang','Bujur','Magnitude','Kedalaman','Wilayah','Potensi'])

    page = requests.get(url_bmkg).text
    data = json.loads(page)

    for i in data['Infogempa']['gempa']:
        df = df.append(i,ignore_index='TRUE')    

    df['DateTime']= pd.to_datetime(df['DateTime'])    
    df['Tanggal']= pd.to_datetime(df['DateTime']).dt.date
    df['Jam WIB']= df['Jam'].str.replace(" WIB","")
    df['Jam WIB']= pd.to_datetime(df['Jam WIB']).dt.time
    df['Jam']= pd.to_datetime(df['DateTime']).dt.time
    df['Magnitude']= pd.to_numeric(df['Magnitude'])

    df.rename(columns={'DateTime':'date_time', 'Jam WIB':'jam_wib'},inplace='TRUE')

    df.columns= df.columns.str.lower()

    df.to_sql(name='temp_data',con=engine,index=False, if_exists='replace')

def Move_Data(ti):

    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    username = dag_var_bmkg["username"]
    password = dag_var_bmkg["password"]
    server = dag_var_bmkg["server"]
    port = dag_var_bmkg["port"]
    database = dag_var_bmkg["database"] 

    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)

    sql_move_data = engine.execute("insert into gempa "
                                        "(tanggal,jam,date_time,coordinates,lintang,bujur,magnitude,kedalaman,wilayah,potensi,jam_wib) "
                                        "select tanggal,jam,date_time,coordinates,lintang,bujur,magnitude,kedalaman,wilayah,potensi,jam_wib "
                                        "from temp_data where CONCAT(temp_data.date_time,temp_data.Coordinates) "
                                        "not in (select concat(gempa.date_time,gempa.coordinates) "
                                        "from gempa) "
                                        ) 

def Earthquake_status(ti):

    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    username = dag_var_bmkg["username"]
    password = dag_var_bmkg["password"]
    server = dag_var_bmkg["server"]
    port = dag_var_bmkg["port"]
    database = dag_var_bmkg["database"] 

    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)

    '''
    df = pd.read_sql_query("select COUNT(*) as status from gempa "
                            "where tanggal = current_date and send_email is null"
                            ,con=engine)
    '''
    df = pd.read_sql_query("select COUNT(*) as status from gempa "
                            "where tanggal = '2022-05-27' and send_email is null"
                            ,con=engine)

    if int(df['status']) >= 1:
        return ("Earthquake")
    else:
        return ("No_Earthquake")

def Earthquake(ti):

    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    username = dag_var_bmkg["username"]
    password = dag_var_bmkg["password"]
    server = dag_var_bmkg["server"]
    port = dag_var_bmkg["port"]
    database = dag_var_bmkg["database"] 
    path_ekspor = dag_var_bmkg["temp_csv"] 

    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)
    
    '''
    df = pd.read_sql_query("select * from gempa "
                            "where tanggal = current_date and send_email is null"
                            ,con=engine)
    '''

    df = pd.read_sql_query("select * from gempa "
                            "where tanggal = '2022-05-27' and send_email is null"
                            ,con=engine)

    df.to_csv (path_ekspor, index = False, header=True)

def No_Earthquake():
    print("No Gempa this day")

def Send_Email(ti):

    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    path_ekspor = dag_var_bmkg["temp_csv"] 
    sender_address = dag_var_bmkg["sender_address"] 
    sender_pass = dag_var_bmkg["sender_pass"] 
    receiver_address = dag_var_bmkg["receiver_address"] 

    df_gempa_now = pd.read_csv(path_ekspor)
    mail_content = ""

    for index, row in df_gempa_now.iterrows():
        mail_content = ("\n Tanggal : " + str(row['tanggal']) +"\n Jam :" + str(row['jam_wib']).replace("0 days","") + " WIB" + "\n Coordinate : " + str(row['coordinates']) + "\n Magnitude : " + str(row['magnitude'])) + "\n Kedalaman : " + str(row['kedalaman']) + "\n Wilayah : " + str(row['wilayah']) + "\n Potensi : " + str(row['potensi']) + "\n\n Sumber data : https://data.bmkg.go.id "

    mail_content = "Perhatian, telah terjadi gempa dengan data sebagai berikut:" + mail_content

    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Peringatan Gempa ' + str(date.today())  #The subject line
    #The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, sender_pass) #login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()

def Update_DB_Send_Email(ti):

    dag_var_bmkg = Variable.get("var_bmkg",deserialize_json=True)
    username = dag_var_bmkg["username"]
    password = dag_var_bmkg["password"]
    server = dag_var_bmkg["server"]
    port = dag_var_bmkg["port"]
    database = dag_var_bmkg["database"] 
    path_ekspor = dag_var_bmkg["temp_csv"] 

    engine = create_engine('postgresql://' + username + ':' + password + '@' + server + ':' + port + '/' + database)

    df_gempa_now = pd.read_csv(path_ekspor)

    for index, row in df_gempa_now.iterrows():
        sql_move_data = engine.execute("update gempa "
                                            "set send_email =  CURRENT_TIMESTAMP "
                                            "where date_time ='" + str(row['date_time']) + "' and  "
                                            "coordinates = '" + str(row['coordinates']) + "'"
                                      ) 


with DAG("DAG_bmkg", start_date=datetime(2022, 5, 17),
    schedule_interval="0 19 * * *", catchup=False) as dag:

        Create_Table = PythonOperator(
            task_id="Create_Table",
            python_callable= Create_Table
        )       

        Get_Web_Data = PythonOperator(
            task_id="Get_Web_Data",
            python_callable= Get_Web_Data
        )       

        Move_Data = PythonOperator(
            task_id="Move_Data",
            python_callable= Move_Data
        )       

        Earthquake_status = BranchPythonOperator(
            task_id='Earthquake_status',
            python_callable=Earthquake_status
        )

        No_Earthquake = PythonOperator(
            task_id='No_Earthquake',
            python_callable=No_Earthquake
        )

        Earthquake = PythonOperator(
            task_id='Earthquake',
            python_callable=Earthquake
        )

        Send_Email = PythonOperator(
            task_id='Send_Email',
            python_callable=Send_Email
        )

        Update_DB_Send_Email = PythonOperator(
            task_id='Update_DB_Send_Email',
            python_callable=Update_DB_Send_Email
        )

        Create_Table >> Get_Web_Data >> Move_Data >> Earthquake_status >> No_Earthquake
        Create_Table >> Get_Web_Data >> Move_Data >> Earthquake_status >> Earthquake >> Send_Email>>Update_DB_Send_Email
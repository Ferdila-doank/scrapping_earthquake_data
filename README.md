# ETL for Earthquake Data & Send Email (/Days) Using Python + Airflow

![image](https://user-images.githubusercontent.com/55681442/170829793-09501a7a-34ff-4046-a30c-7ab1f3caf774.png)

This project contain ETL to get eartquake data from website data.bmkg.go.id and store data in postgre. If in the day data get have new earthquake data will sending data to email (in this case using gmail).

This project using airflow, python and postgre. in Airflow have 7 task to run this ETL. First task (Create_Table) is to create table in postgre for store data from bmkg website. Second task(Get_Web_Data) is for scrapping data from website bmkg, that data always update everyday. 3th(Move_Data) task is for move data to postgree (if have new data). 4th(Earthquake_status) task is for compare data in postgre and website, if have new data from website will update that data to postgre. 5th(No_Earthquake) and 6th(Earthquake) task is for get only new data based on days the etl running. 7th(Send_Email) task is to sending data to email(suing gmail).8th (Update_DB_send_email) task is to update field send_email in table gempa for confrim the data has been send to email.




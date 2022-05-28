# ETL for Earthquake Data & Send Email (/Days) Using Python + Airflow

![image](https://user-images.githubusercontent.com/55681442/170829793-09501a7a-34ff-4046-a30c-7ab1f3caf774.png)

This project contain ETL to get eartquake data from website data.bmkg.go.id and store data in postgre. If in the day data get have new earthquake data will sending data to email (in this case using gmail).

This project using airflow, python and postgre. in Airflow have 7 task to run this ETL. First task (Create_Table) is to create table in postgre for store data from bmkg website. Second task() is for scrapping data from website bmkg, that data always update everyday. 3th() task is for move data to postgree (if have new data). 4th() task is for compare data in postgre and website, if have new data from website will update that data to postgre. 5th() and 6th() task is for get only new data based on days the etl running. 7th() task is to sending data to email(suing gmail).




# **ETL for Earthquake Data & Send Email (/Days) Using Python + Airflow**

![image](https://user-images.githubusercontent.com/55681442/170854762-c16f55e2-fc6f-4b85-b643-92a8887f64d2.png)

This project contain ETL to get eartquake data from website data.bmkg.go.id and store data in postgre. If in the day data get have new earthquake data will sending data to email (in this case using gmail). Source data get from [https://data.bmkg.go.id/gempabumi/](https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.xml)

This project using airflow, python and postgre. in Airflow have 7 task to run this ETL. First task (Create_Table) is to create table in postgre for store data from bmkg website. Second task(Get_Web_Data) is for scrapping data from website bmkg, that data always update everyday. 3th(Move_Data) task is for move data to postgree (if have new data). 4th(Earthquake_status) task is for compare data in postgre and website, if have new data from website will update that data to postgre. 5th(No_Earthquake) and 6th(Earthquake) task is for get only new data based on days the etl running. 7th(Send_Email) task is to sending data to email(using gmail).8th (Update_DB_send_email) task is to update field send_email in table gempa for confrim the data has been send to email.

## 1. Installation Instruction (this step for ubuntu OS)

a. Please install docker first before doing next step, this is step installation docker for ubuntu https://linuxhint.com/install_configure_docker_ubuntu/

b. Copy file airflow-docker.zip and extract to docker path for windows or linux copy in airflow-docker

c. With terminal go to forlder airflow-docker and type docker-compose up airflow-init

d. Please create database bmkg in your postgre server 

![image](https://user-images.githubusercontent.com/55681442/170855032-7d1005c7-5b36-4303-86c0-e2223096a2fd.png)

e. Open Airflow website in ip 0.0.0.0 using username airflow and password airflow
![image](https://user-images.githubusercontent.com/55681442/170854176-b6f25d26-30f5-476e-8d2e-dce550912de6.png)

f. in tab admin -> variabel import file var_bmkg.json but before you upload this file please edit server ip (sync with ip postgre in your airflow), sender_address to email you want to send data bmkg, sender_pass is sender_address password. And receiver_address to email you want get data.

![image](https://user-images.githubusercontent.com/55681442/170854605-d4140e47-0a89-4dd4-b5f7-4ccd59edbafc.png)

![image](https://user-images.githubusercontent.com/55681442/170854613-441df54c-dea5-40a5-b45c-c8e3ab8bbfdd.png)

g. Configure your email sender (@gmail.com) in setting less secure app setting

![image](https://user-images.githubusercontent.com/55681442/170855260-97b79706-c45b-4fa6-8b63-23ce67594ef0.png)

## 2. Running and Testing DAG

a. For trying DAG go to DAG tab search DAG_BMKG and click action run (Don't forget to unpause this DAG)

![image](https://user-images.githubusercontent.com/55681442/170854670-9a42c20f-d46c-4c59-b8d3-2b058c77c0a6.png)

b. If the DAG running well in graph view you will see all task in green border 

![image](https://user-images.githubusercontent.com/55681442/170854762-c16f55e2-fc6f-4b85-b643-92a8887f64d2.png)

c. If this DAG is running well you will see postgre database in table gempa will update and if you have new data you will get email (receiver_address) detail of eartquake 

![image](https://user-images.githubusercontent.com/55681442/170855101-889df435-d87c-4c66-84ea-846dbc704c58.png)

![image](https://user-images.githubusercontent.com/55681442/170855175-b2f9c9de-07c8-41f0-b9eb-6926abcdf807.png)









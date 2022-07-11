FROM mcr.microsoft.com/azure-functions/python:3.0-python3.8

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

COPY ./requirements.txt /code/requirements.txt

RUN pip install --upgrade pip

RUN apt-get update && apt-get -y install libpq-dev gcc && pip install psycopg2
 
RUN pip install -r /code/requirements.txt 

COPY ./* /home/site/wwwroot
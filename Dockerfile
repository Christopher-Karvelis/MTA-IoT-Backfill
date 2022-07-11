FROM mcr.microsoft.com/azure-functions/python:3.0-python3.8

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

COPY ./requirements.txt /code/requirements.txt

RUN pip install --upgrade pip

RUN apt-get update
RUN apt-get upgrade -y

# this is here to make sure we can compile c++ when using pip install
RUN apt-get install gcc python3-dev python3-pip libxml2-dev libxslt1-dev zlib1g-dev g++ -y

# cannot install pyodbc without this installed
RUN apt-get install unixodbc-dev -y
 
RUN pip install -r /code/requirements.txt 

COPY . /home/site/wwwroot
# base image
FROM python:3.7-slim

# install important programs
RUN apt-get update
RUN apt-get install -y gnupg
RUN apt-get install -y g++
RUN apt-get install -y curl

# download package
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update

# install driver
ENV ACCEPT_EULA=Y
RUN apt-get install -y msodbcsql17

# install packages to make pyodbc working
RUN apt-get install -y unixodbc-dev


WORKDIR /DownstreamSender
COPY requirements.txt downstream.py config.py run.py ./
RUN mkdir log
RUN pip install -r requirements.txt


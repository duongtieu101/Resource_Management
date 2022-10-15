FROM apache/airflow:2.4.1-python3.8
WORKDIR /opt/airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . /opt/airflow

USER root
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools
RUN chown -R airflow /opt/airflow

USER airflow

# RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
# CMD ["python", "./main.py"]
# CMD ["python", "./main.py"]
# CMD ["echo", "Hello"]
# ENTRYPOINT ["tail", "-f", "/dev/null"]



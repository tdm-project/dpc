FROM tdmproject/tdmq-client:dev-379

ENV DPC_HOME /opt/dpc

WORKDIR $DPC_HOME
COPY requirements.txt $DPC_HOME
RUN pip3 install -r requirements.txt
COPY ingest.py $DPC_HOME
COPY sources $DPC_HOME/sources


ENTRYPOINT ["python3", "ingest.py"]

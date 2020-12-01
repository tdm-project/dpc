FROM tdmproject/tdmq-client:dev-308

ENV DPC_HOME /opt/dpc

WORKDIR $DPC_HOME
COPY requirements.txt $DPC_HOME
RUN pip3 install -r requirements.txt
COPY ingest.py $DPC_HOME
COPY sources $DPC_HOME/sources


ENTRYPOINT ["/entrypoint.sh", "fake_user.sh", "python3", "ingest.py"]

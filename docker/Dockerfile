FROM tdmproject/tdmqc
ENV DPC_HOME /opt/dpc

WORKDIR $DPC_HOME
COPY dpc/requirements.txt $DPC_HOME
RUN pip3 install -r requirements.txt
COPY dpc/ingest.py $DPC_HOME
COPY dpc/sources $DPC_HOME/sources


ENTRYPOINT ["/entrypoint.sh", "fake_user.sh", "python3", "ingest.py"]

FROM tdmproject/tdmq-client:dev-410

ENV DPC_HOME /opt/dpc

WORKDIR $DPC_HOME
COPY requirements.txt "${DPC_HOME}"
RUN pip3 install --no-cache-dir -r requirements.txt
COPY dpc.py "${DPC_HOME}"
RUN chmod a+rx "${DPC_HOME}/dpc.py"

RUN useradd -m -s /bin/bash -d "${DPC_HOME}" dpc
USER dpc

ENTRYPOINT ["python3", "./dpc.py"]

FROM tdmproject/tdmq-client:master-652

ENV DPC_HOME /opt/dpc

USER root
RUN useradd -m -s /bin/bash -d "${DPC_HOME}" dpc

COPY dpc.py "${DPC_HOME}"
COPY requirements.txt "${DPC_HOME}"

RUN chmod a+rx "${DPC_HOME}/dpc.py" && chown -R dpc:dpc "${DPC_HOME}"

USER dpc
WORKDIR $DPC_HOME
RUN pip3 install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "./dpc.py"]

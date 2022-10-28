FROM python:3
RUN pip install kafka-python
RUN pip install pandas

WORKDIR /usr/src/app
COPY data_producer_2.py .
COPY gen2_Heizungsdaten.csv .

#CMD ["python", "./data_producer_2.py"]
CMD python ./data_producer_2.py > logfile.log

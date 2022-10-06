FROM python:3
RUN pip install kafka-python
RUN pip install pandas

WORKDIR /usr/src/app
COPY data_consumer2a.py ../../../part1
COPY consum_Heizungsdaten.csv ../../../data

CMD ["python", "./data_consumer2a.py"]
FROM python:3
RUN pip install kafka-python
RUN pip install pandas

WORKDIR /usr/src/app
COPY data_producer_1a.py .
COPY gen1_sinus_data.csv .

CMD ["python", "./data_producer_1a.py"]
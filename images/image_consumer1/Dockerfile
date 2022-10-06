FROM python:3
RUN pip install kafka-python
RUN pip install pandas

WORKDIR /usr/src/app
COPY data_consumer1.py ../../../part1
COPY consum_sinus_data.csv ../../../data

CMD ["python", "./data_consumer1.py"]
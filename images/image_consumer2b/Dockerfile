FROM python:3
RUN pip install kafka-python
RUN pip install pandas

WORKDIR /usr/src/app
COPY data_consumer2b.py .

CMD ["python", "./data_consumer2b.py"]
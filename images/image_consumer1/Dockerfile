FROM python:3
RUN pip install kafka-python
RUN pip install pandas

WORKDIR /usr/src/app
COPY data_consumer1.py .
COPY consum_sinus_data.csv .

CMD ["python", "./data_consumer1.py"]

# Error Handling
#CMD echo heyyyy && python ./data_consumer1.py > log.txt && echo ende
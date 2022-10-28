FROM jupyter/scipy-notebook:${JUPYTER_VERSION:-latest}
RUN pip install kafka-python
RUN pip install pandas
COPY consum_performance.ipynb /home/jovyan

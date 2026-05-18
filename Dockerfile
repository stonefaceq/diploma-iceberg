FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

RUN pip install --no-cache-dir kafka-python

USER jovyan
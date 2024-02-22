# official lightweight python from docker
FROM python:3.12.2

#push log to Knative logs
ENV PYTHONUNBUFFERED True

#local code into image
RUN mkdir ./airtable
WORKDIR ./airtable
COPY ./ ./

RUN pip install -r requirements.txt
RUN python setup.py install 
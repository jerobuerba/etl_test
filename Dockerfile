FROM python:3.10

WORKDIR /

COPY job.py /
COPY base_google.py /
COPY parameters.py /
COPY requirements.txt /

COPY creds /creds

RUN pip install -r requirements.txt
EXPOSE 8080

CMD ["python", "job.py"]
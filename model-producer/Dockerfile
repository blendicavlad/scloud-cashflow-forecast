# syntax=docker/dockerfile:1

FROM python:3.9
RUN  apt-get update
RUN  apt-get -y install libxml2-dev libxmlsec1-dev libxmlsec1-openssl

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN pip install python-dotenv

COPY . .

CMD ["python3", "app.py"]

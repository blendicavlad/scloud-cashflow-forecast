# syntax=docker/dockerfile:1

FROM python:3.9

EXPOSE 5000

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN pip install python-dotenv

COPY . .

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]

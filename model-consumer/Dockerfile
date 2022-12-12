# syntax=docker/dockerfile:1

FROM public.ecr.aws/lambda/python:3.8

WORKDIR ${LAMBDA_TASK_ROOT}
COPY requirements.txt requirements.txt

RUN /var/lang/bin/python3.8 -m pip install --upgrade pip
RUN /var/lang/bin/python3.8 -m pip install -r requirements.txt
RUN /var/lang/bin/python3.8 -m pip install python-dotenv

COPY . .

CMD ["app.handler"]

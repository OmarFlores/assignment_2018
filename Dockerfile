FROM python:2.7-slim

WORKDIR /app

COPY . /app

COPY requirements.txt /app

ADD scripts/crypto_analysis.py /app

RUN pip install -r requirements.txt

RUN mkdir -p /app/temp_records

ENV FOLDER_NAME temp_records

ENV API_KEY I3OKCLU7HBEW63X1

CMD [ "python", "./crypto_analysis.py" ]

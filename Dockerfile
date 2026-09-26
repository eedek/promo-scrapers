FROM python:3.12

WORKDIR /app
COPY src/requirements.txt ./
RUN pip install -r requirements.txt
RUN playwright install --with-deps chromium

COPY src .


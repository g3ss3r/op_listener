FROM python:3.8 AS builder
COPY requirements.txt .

RUN pip install --user -r requirements.txt

FROM python:3.8
WORKDIR /code

COPY --from=builder /root/.local /root/.local
COPY ./ .

ENV PATH=/root/.local:$PATH;PYTHONUNBUFFERED=1

CMD ["python", "-u", "-v", "./main.py"]
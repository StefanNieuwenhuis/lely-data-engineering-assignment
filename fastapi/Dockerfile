FROM python:3.12-slim

WORKDIR opt/fastapi/app

COPY ./fastapi/requirements.txt .

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./fastapi/app .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
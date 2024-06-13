FROM python:3.12-slim

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

CMD ["streamlit", "run", "--server.address", "0.0.0.0", "Home.py"]
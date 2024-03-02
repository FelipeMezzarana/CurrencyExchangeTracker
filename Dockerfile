FROM python:3.10

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy everything in
COPY src /src 
WORKDIR /

CMD python3 -m src.main
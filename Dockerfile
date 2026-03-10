FROM python:3.10.11

ENV TZ=Asia/Jakarta

WORKDIR /Excel Uploader

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*


COPY . .

RUN pip3 install -r requirements.txt

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

CMD [ "python", "main.py", "--server.port=8501", "--server.address=0.0.0.0" ]
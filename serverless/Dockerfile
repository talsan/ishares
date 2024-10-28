FROM public.ecr.aws/lambda/python:3.9
COPY etf_downloader.py /var/task
COPY queue_etf_downloader.py /var/task
COPY /utils /var/task
COPY config.py /var/task
COPY .env /var/task
COPY ishares-etf-index.csv /var/task
COPY requirements.txt /var/task
COPY start.sh /var/task
RUN pip install -r requirements.txt
ENTRYPOINT ['/var/task/start.sh']



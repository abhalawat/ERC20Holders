FROM python:3.8.10
LABEL Maintainer="Aditi"
WORKDIR /home/deq/ERC20_token

COPY requirements.txt requirements.txt
RUN pip install -Ur requirements.txt
COPY contractAdresss.py ./

#RUN ray start --head

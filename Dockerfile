#Deriving the latest base image
FROM python:3.8.10
#FROM rayproject/ray
#RUN pip install -U ray
#RUN sudo apt update
#RUN sudo apt install python3
#Labels as key value pair
LABEL Maintainer="Aditi"


# Any working directory can be chosen as per choice like '/' or '/home' etc
# i have chosen /usr/app/src
WORKDIR /ERC20_token

COPY requirements.txt requirements.txt
RUN pip install -Ur requirements.txt
#to COPY the remote file at working directory in container
COPY app.py ./

# Now the structure looks like this '/usr/app/src/test.py'

RUN ray start --head
#CMD instruction should be used to run the software
#contained by your image, along with any arguments.
#CMD ["ray start", "--head"]
CMD [ "python3", "app.py"]
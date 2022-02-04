#Deriving the latest base image
FROM python:latest


#Labels as key value pair
LABEL Maintainer="Aditi"


# Any working directory can be chosen as per choice like '/' or '/home' etc
# i have chosen /usr/app/src
WORKDIR /ERC20_token/ERC20Address

#to COPY the remote file at working directory in container
COPY erc20Address.py ./
# Now the structure looks like this '/usr/app/src/test.py'


#CMD instruction should be used to run the software
#contained by your image, along with any arguments.

CMD [ "python3", "./erc20Address.py"]
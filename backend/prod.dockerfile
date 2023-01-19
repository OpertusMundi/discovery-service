# vim: set syntax=dockerfile:
FROM python:3.7.12-slim-buster

# Install deps
RUN apt-get update --fix-missing && apt-get install -y gcc g++ wget git
# Need to do this since JRE install is bugged on slim-buster: https://github.com/debuerreotype/docker-debian-artifacts/issues/24
RUN mkdir -p /usr/share/man/man1 
RUN apt-get install -y default-jre

# Install python deps
COPY requirements.txt /
RUN pip3 install -r /requirements.txt

COPY . ./backend

FROM python:3.7.12-slim-buster

# Install deps
RUN apt-get clean
RUN apt-get update --fix-missing
RUN apt-get install -y screen
RUN apt-get install -y gcc
RUN apt-get install -y g++
RUN apt-get install -y wget
RUN apt-get install -y git
# Need to do this since JRE install is bugged on slim-buster: https://github.com/debuerreotype/docker-debian-artifacts/issues/24
RUN mkdir -p /usr/share/man/man1 
RUN apt-get install -y default-jre

# Add docker-compose-wait tool -------------------
ENV WAIT_VERSION 2.7.2
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/$WAIT_VERSION/wait ./wait
RUN chmod +x ./wait

# Install minio client - using an older version, because one of the new versions completely broke within docker
RUN wget https://dl.min.io/client/mc/release/linux-amd64/archive/mc.RELEASE.2022-01-07T06-01-38Z -O /usr/local/bin/mc
RUN chmod +x /usr/local/bin/mc

# Install python deps
COPY requirements.txt /
RUN pip3 install -r /requirements.txt

## Because Sherlock is a special cookie, we have to install it manually
#RUN git clone https://github.com/delftdata/sherlock-project.git /sherlock
#RUN pip3 install -e /sherlock
#
# Pre-download sherlock's data
#RUN python -c 'from sherlock.features import preprocessing; preprocessing.prepare_feature_extraction()'
#RUN wget https://drive.google.com/uc?id=1kayd5oNRQm8-NCvA8pIrtezbQ-B1_Vmk -O /sherlock/sherlock/features/glove.6B.50d.txt
#RUN wget https://drive.google.com/uc?id=1vdyGJ4aB71FCaNqJKYX387eVufcH4SAu -O /sherlock/sherlock/features/par_vec_trained_400.pkl.docvecs.vectors_docs.npy
#RUN wget https://drive.google.com/uc?id=1hwE8We05oZLrACRibY8jc81NGughv79q -O /sherlock/sherlock/features/par_vec_trained_400.pkl.trainables.syn1neg.npy
#RUN wget https://drive.google.com/uc?id=1StGoalk5SMbWX8Z-5weSbIAtK771UwoC -O /sherlock/sherlock/features/par_vec_trained_400.pkl.wv.vectors.npy

# Copy run script
COPY start.sh /
RUN chmod a+x start.sh

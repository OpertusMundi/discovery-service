FROM openjdk:11-jre-slim-buster

# Install deps
RUN apt-get clean
RUN apt-get update --fix-missing
RUN apt-get install -y unzip
RUN apt-get install -y wget
RUN apt-get install -y python3
RUN apt-get install -y screen
RUN apt-get install -y python3-pip


# Install python deps
COPY ./requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt

# Install metanome
RUN wget https://github.com/delftdata/Metanome/releases/download/1.0.0-daisy/deployment-1.2-SNAPSHOT-package_with_tomcat.zip
RUN unzip ./deployment-1.2-SNAPSHOT-package_with_tomcat.zip -d metanome
RUN rm ./deployment-1.2-SNAPSHOT-package_with_tomcat.zip

# Add algo
RUN wget https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/projekte/repeatability/DataProfiling/Metanome_Algorithms/BINDER-1.2-SNAPSHOT.jar -P /metanome/backend/WEB-INF/classes/algorithms

# Copy run script
ADD ./start.sh ./start
RUN chmod a+x ./start
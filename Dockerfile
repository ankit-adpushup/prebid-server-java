FROM maven:3.6.3-openjdk-8
# FROM openjdk:14-jdk

# Add localhost trusted certificate
# RUN yum install ca-certificates
# RUN update-ca-trust force-enable
# COPY ./localhost.crt /etc/pki/ca-trust/source/anchors/
# RUN update-ca-trust extract

# RUN mkdir /app
# WORKDIR /app

# # First copy only the pom file. This is the file with less change
# COPY ./pom.xml .

# # Download the package and make it cached in docker image
# RUN mvn -B -f ./pom.xml -s /usr/share/maven/ref/settings-docker.xml dependency:resolve

# COPY ./ /app/
# RUN mkdir /app/config
# RUN chmod -R a+w /app
# RUN mvn install:install-file -Dfile=./jar-files/eaio-uuid-3.4.2-MULE-002-SNAPSHOT.jar -DgroupId=org.mule.com.github.stephenc.eaio-uuid -DartifactId=eaio-uuid -Dversion=3.4.2-MULE-002-SNAPSHOT -Dpackaging=jar
# RUN mvn install:install-file -Dfile=./jar-files/Database-1.0.jar -DgroupId=com.adpushup.e3 -DartifactId=Database -Dversion=1.0 -Dpackaging=jar
# RUN mvn clean package -DskipTests
# CMD java -jar target/prebid-server.jar --spring.config.additional-location=config/prebid-config.yaml

#########################################################
# FROM openjdk:14-jdk

# ARG MAVEN_VERSION=3.6.3
# ARG USER_HOME_DIR="/root"
# ARG SHA=c35a1803a6e70a126e80b2b3ae33eed961f83ed74d18fcd16909b2d44d7dada3203f1ffe726c17ef8dcca2dcaa9fca676987befeadc9b9f759967a8cb77181c0
# ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries

# RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
#   && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
#   && echo "${SHA}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
#   && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
#   && rm -f /tmp/apache-maven.tar.gz \
#   && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn
# ENV MAVEN_HOME /usr/share/maven


# # Add localhost trusted certificate
# RUN yum install ca-certificates
# RUN update-ca-trust force-enable
# # COPY ./localhost.crt /etc/pki/ca-trust/source/anchors/
# RUN update-ca-trust extract

# RUN mkdir /app
# WORKDIR /app
# COPY ./ /app/
# RUN mkdir /app/config
# RUN chmod -R a+w /app
# RUN mvn install:install-file -Dfile=./jar-files/eaio-uuid-3.4.2-MULE-002-SNAPSHOT.jar -DgroupId=org.mule.com.github.stephenc.eaio-uuid -DartifactId=eaio-uuid -Dversion=3.4.2-MULE-002-SNAPSHOT -Dpackaging=jar
# RUN mvn install:install-file -Dfile=./jar-files/Database-1.0.jar -DgroupId=com.adpushup.e3 -DartifactId=Database -Dversion=1.0 -Dpackaging=jar
# RUN mvn clean package -DskipTests
# CMD java -jar target/prebid-server.jar --spring.config.additional-location=config/prebid-config.yaml

####################################################################

FROM openjdk:14-jdk

ARG USER_HOME_DIR="/root"

# Add localhost trusted certificate
RUN yum install ca-certificates
RUN update-ca-trust force-enable
# COPY ./localhost.crt /etc/pki/ca-trust/source/anchors/
RUN update-ca-trust extract

RUN mkdir /app
WORKDIR /app
# COPY ./ /app/
RUN mkdir /app/config
# RUN chmod -R a+w /app

CMD java -jar target/prebid-server.jar --spring.config.additional-location=config/prebid-config.yaml

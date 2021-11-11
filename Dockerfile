FROM openjdk:8u292
WORKDIR /opt
COPY ./target/prebid-server.jar /opt/
COPY ./config/prebid-config.yaml /opt/config/
COPY ./config/logging-properties.xml /opt/config/
#RUN  mvn install:install-file -Dfile='/var/lib/jenkins/workspace/prebid-server-java/jar-files/eaio-uuid-3.4.2-MULE-002-SNAPSHOT.jar' -DgroupId=org.mule.com.github.stephenc.eaio-uuid -DartifactId=eaio-uuid -Dversion=3.4.2-MULE-002-SNAPSHOT -Dpackaging=jar -DgeneratePom=true
#RUN mvn clean package -DskipTests=true -Dcheckstyle.skip
CMD java -jar /opt/prebid-server.jar --spring.config.additional-location=config/prebid-config.yaml --adpushup.customConfig.couchbase-ips=$COUCHBASE_HOST

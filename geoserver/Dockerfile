FROM openjdk:8-jre
MAINTAINER Yong Wook Kim <ywkim@illinois.edu>

ARG GEOSERVER_VERSION=2.14.2

ENV GEOSERVER_PORT=8080 \
    GEOSERVER_HOME=/usr/share/geoserver \
    GEOSERVER_DATA_DIR=/data_dir \
    GEOSERVER_VERSION=${GEOSERVER_VERSION}
VOLUME /data
EXPOSE 8080

# dowload geoserver
RUN wget --quiet http://sourceforge.net/projects/geoserver/files/GeoServer/${GEOSERVER_VERSION}/geoserver-${GEOSERVER_VERSION}-bin.zip \
    && unzip -q geoserver-${GEOSERVER_VERSION}-bin.zip \
    && rm geoserver-${GEOSERVER_VERSION}-bin.zip \
    && mv geoserver-${GEOSERVER_VERSION} /usr/share/geoserver \
    && chown -R root /usr/share/geoserver/ \
    && mv /usr/share/geoserver/data_dir /data_dir

# download and install csw
RUN wget --quiet http://sourceforge.net/projects/geoserver/files/GeoServer/${GEOSERVER_VERSION}/extensions/geoserver-${GEOSERVER_VERSION}-csw-plugin.zip \
    && unzip -q geoserver-${GEOSERVER_VERSION}-csw-plugin.zip \
    && mv *.jar /usr/share/geoserver/webapps/geoserver/WEB-INF/lib/. \
    && rm geoserver-${GEOSERVER_VERSION}-csw-plugin.zip

# download and install jetty utility servlet for CORS
RUN wget --quiet http://central.maven.org/maven2/org/eclipse/jetty/jetty-servlets/9.2.13.v20150730/jetty-servlets-9.2.13.v20150730.jar \
    && mv *.jar /usr/share/geoserver/webapps/geoserver/WEB-INF/lib/. \
    && sed -i "s|<!-- <filter>|<filter>|g" /usr/share/geoserver/webapps/geoserver/WEB-INF/web.xml \
    && sed -i "s|</filter> -->|</filter>|g" /usr/share/geoserver/webapps/geoserver/WEB-INF/web.xml \
    && sed -i "s|<!-- <filter-mapping>|<filter-mapping>|g" /usr/share/geoserver/webapps/geoserver/WEB-INF/web.xml \
    && sed -i "s|</filter-mapping> -->|</filter-mapping>|g" /usr/share/geoserver/webapps/geoserver/WEB-INF/web.xml

CMD /usr/share/geoserver/bin/startup.sh
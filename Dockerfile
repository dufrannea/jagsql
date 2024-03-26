FROM hseeberger/scala-sbt:8u282_1.5.5_3.0.1
COPY build.sh /
CMD ["/build.sh"]
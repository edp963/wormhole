cd rider
mvn clean package -Pdist -DskipTests

zip包在rider-assembly/target下，解压，修改application.conf，进入bin目录下，运行./start.sh即可
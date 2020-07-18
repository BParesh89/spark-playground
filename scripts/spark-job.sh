docker exec master /opt/spark/bin/spark-submit \
--class org.example.broadcastdemo \
--master spark://master:6066 \
--deploy-mode cluster --verbose \
/opt/jars/spark-playground-1.0-SNAPSHOT.jar
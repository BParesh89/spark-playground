docker exec master /opt/spark/bin/spark-submit \
--class org.example.sparkdemo \
--master spark://master:6066 \
--deploy-mode cluster --verbose \
--num-executors 2 \
/opt/jars/spark-playground-1.0-SNAPSHOT.jar
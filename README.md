# msd-metdisease-database-chemont-parents-builder

[![](https://mermaid.ink/img/pako:eNqtk1Fr2zAUhf-KEHtwIK7f81BIrbT1QtvQ9mXEJcjWdSNmSa50XWaa_vfJlsLY1sJaZjAW6N7znasjv9DaCKAL-mh5tyf3rNTEP8tEOZEqQCEdcAep4MircdEpKdLav1UvWwF2RtL09NAY26tsc1WwXV6wA1m921_vQRmNqZAWakw7bkGjmwXsWZL77RuNQTXWZqYyn5ZkyaavRlVSXOeXxXr1LWh3fTX2ZVLXe_kdhk8DPtw24sl5Aj_Q8hrH89oVo4k1DFHy7U3SWKNInCYMUVLmYQ5w6wuXWhxLH0pKDuQiyVvu3HDu-bfw1IPDkyk1X4xDBzdNHhxejNlH9geb_rCR3KGV-nEePrPg4zKZLhebjmEzncJ_gsHTNpAeficWgbhsI-6Y1VtOpji86i1j23sruxa8xoF8TUoabjULuY5Osl92sy_PYJ00Ogvx7kK8J4htSSPubxvvwNb_DOMtgtUc5TNEootIOqcKrOJS-L_5ZTRQUvQyUNKFXwpoeN9iSUv96kv7zl9TWAmJxtJFw1sHc8p7NHeDrukCbQ_HIia5H0PFqtef76x2mQ)](https://mermaid.live/edit#pako:eNqtk1Fr2zAUhf-KEHtwIK7f81BIrbT1QtvQ9mXEJcjWdSNmSa50XWaa_vfJlsLY1sJaZjAW6N7znasjv9DaCKAL-mh5tyf3rNTEP8tEOZEqQCEdcAep4MircdEpKdLav1UvWwF2RtL09NAY26tsc1WwXV6wA1m921_vQRmNqZAWakw7bkGjmwXsWZL77RuNQTXWZqYyn5ZkyaavRlVSXOeXxXr1LWh3fTX2ZVLXe_kdhk8DPtw24sl5Aj_Q8hrH89oVo4k1DFHy7U3SWKNInCYMUVLmYQ5w6wuXWhxLH0pKDuQiyVvu3HDu-bfw1IPDkyk1X4xDBzdNHhxejNlH9geb_rCR3KGV-nEePrPg4zKZLhebjmEzncJ_gsHTNpAeficWgbhsI-6Y1VtOpji86i1j23sruxa8xoF8TUoabjULuY5Osl92sy_PYJ00Ogvx7kK8J4htSSPubxvvwNb_DOMtgtUc5TNEootIOqcKrOJS-L_5ZTRQUvQyUNKFXwpoeN9iSUv96kv7zl9TWAmJxtJFw1sHc8p7NHeDrukCbQ_HIia5H0PFqtef76x2mQ)
https://github.com/eMetaboHUB/Forum-DiseasesChem/blob/master/app/build/classyfire_functions.py

## sansa

https://github.com/SANSA-Stack/SANSA-Stack/tags

## run example

--num-executors 1 

### Local 

```shell
/usr/local/share/spark/bin/spark-submit \
  --conf "spark.eventLog.enabled=true" \
  --conf "spark.eventLog.dir=file:///tmp/spark-events"  \
  --driver-memory 2G \
  --executor-memory 1G  \
  --num-executors 1   \
  --jars ./sansa-ml-spark_2.12-0.8.0-RC3-SNAPSHOT-jar-with-dependencies.jar assembly/msd-metdisease-database-chemont-parents-builder.jar -d ./rdf -r test
```

### MSD 

```shell
spark-submit  \
 --deploy-mode cluster \
 --driver-memory 2G \
 --executor-memory 2G \
 --num-executors 5 \
 --conf spark.yarn.appMasterEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/" \
 --conf spark.executorEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/"  \
 --conf spark.yarn.submit.waitAppCompletion="false" \
 --jars /usr/share/java/sansa-stack-spark_2.12-0.8.4_ExDistAD.jar \
 msd-metdisease-database-chemont-parents-builder.jar
```

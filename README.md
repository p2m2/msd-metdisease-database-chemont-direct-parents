# msd-metdisease-database-chemont-parents-builder

[![](https://mermaid.ink/img/pako:eNqdkU9rwzAMxb9K0CmFmtxzGGxJYGFsK2yXQWA4ltKYxX9w5ENp-93nkG6wwy4RCIT83u8ZdAblkKCEY5B-zN7rzmap7nMzozDEqGeSMwmULPtl8EajUKn7qCeksMuEuLsMLkRTHJ7b-rNq60vW_OtXIxlnWaAOpFh4GcjyvFtjH_IqPb9aXqk3beF6txlZ5YfYL9QsfWzF-tgvlkI54120uJld_7Lbl-qxfWo-_gZoq0b9RadNAUsE7MFQMFJjutB52XTAyUAdlGlEGmScuIPOXpM0-kSlBjW7AOUgp5n2ICO7t5NVUHKI9COqtUwHNzfV9RvLErRH)](https://mermaid.live/edit#pako:eNqdkU9rwzAMxb9K0CmFmtxzGGxJYGFsK2yXQWA4ltKYxX9w5ENp-93nkG6wwy4RCIT83u8ZdAblkKCEY5B-zN7rzmap7nMzozDEqGeSMwmULPtl8EajUKn7qCeksMuEuLsMLkRTHJ7b-rNq60vW_OtXIxlnWaAOpFh4GcjyvFtjH_IqPb9aXqk3beF6txlZ5YfYL9QsfWzF-tgvlkI54120uJld_7Lbl-qxfWo-_gZoq0b9RadNAUsE7MFQMFJjutB52XTAyUAdlGlEGmScuIPOXpM0-kSlBjW7AOUgp5n2ICO7t5NVUHKI9COqtUwHNzfV9RvLErRH)

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
 --executor-memory 8G \
 --num-executors 5 \
 --conf spark.yarn.appMasterEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/" \
 --conf spark.executorEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/"  \
 --conf spark.yarn.submit.waitAppCompletion="false" \
 --jars /usr/share/java/sansa-stack-spark_2.12-0.8.4_ExDistAD.jar \
 msd-metdisease-database-chemont-parents-builder.jar
```

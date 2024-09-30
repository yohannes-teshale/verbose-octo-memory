### HOW TO RUN:

Get input data from kaggle : https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud?resource=download ( not uploaded here because of size restrictions).
rename the data to input.csv and mode it under src/main/resources directory.

Setup Hbase, Local or with HDFS,

Run 
```docker compose up```
to start kafka and zookeeper servers.

Run the kafka producer class, Then The spark streaming project class.
Model training is not nessasary since the saved model is uplaoded already under the model directory.

#before running this application, you need to build mysql's table for this application. 

home_path="/home/cc/"
home_source=${home_path}"storm/riot-bench/modules/tasks/src/main/resources/"

${home_path}storm/bin/storm kill ETL_SQL_Topology_Taxi

cd ~/storm/riot-bench/

~/maven/bin/mvn clean compile package -DskipTests

cd -

#sleep 30


${home_path}storm/bin/storm jar ${home_path}storm/riot-bench/modules/storm/target/iot-bm-storm-0.1-jar-with-dependencies.jar in.dream_lab.bm.stream_iot.storm.topo.apps.ETL_SQL_Topology C ETL_SQL_Topology_Taxi ${home_source}TAXI_sample_data_senml.csv TAXI-10  1.0   ${home_path}storm/riot-bench/output/    ${home_source}tasks_TAXI.properties  test

# storm jar <stormJarPath>   in.dream_lab.bm.stream_iot.storm.topo.micro.MicroTopologyDriver  C  <TopoName>  <inputDataFilePath used by CustomEventGen and spout>   PLUG-<expNum>  <rate as 1x,2x>  <outputLogPath>   <tasks.properties File Path>   <microTaskName>
 
# microTaskName
#BlockWindowAverage"
#DistinctApproxCount"
#Accumlator"
#BloomFilterCheck"
#BloomFilterTrain"
#RangeFilterCheck"
#AzureBlobDownload"
#AzureBlobUpload"
#AzureTable"
#AzureWrite"
#MQTTPublish"
#ZipMultipleBuffer"
#LinearRegressionTrainBatched"
#DecisionTreeTrainBatched"
#PiByViete"
#XMLParse"
#SenMlParse"
#CsvToSenML"
#DecisionTreeClassify"
#DecisionTreeTrain"
#LinearRegressionPredictor"
#LinearRegressionTrain"
#SimpleLinearRegressionPredictor"
#KalmanFilter"
#SecondOrderMoment"
#Interpolation"
#Annotate"
#NoOperation"
#LineChartPlot"
#MultiLineChartPlot"

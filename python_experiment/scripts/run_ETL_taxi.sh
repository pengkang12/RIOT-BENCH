home_path="/home/cc/"
home_source=${home_path}"storm/riot-bench/modules/tasks/src/main/resources/"

${home_path}storm/bin/storm kill ETLTopology-taxi

cd ~/storm/riot-bench/

#~/maven/bin/mvn clean compile package -DskipTests

cd -

sleep 50


${home_path}storm/bin/storm jar ${home_path}storm/riot-bench/modules/storm/target/iot-bm-storm-0.1-jar-with-dependencies.jar in.dream_lab.bm.stream_iot.storm.topo.apps.ETLTopology C ETLTopology-taxi ${home_source}TAXI_sample_data_senml.csv SENML 0.1   ${home_path}storm/riot-bench/output/    ${home_source}tasks_TAXI.properties  test

# storm jar <stormJarPath>   in.dream_lab.bm.stream_iot.storm.topo.micro.MicroTopologyDriver  C  <TopoName>  <inputDataFilePath used by CustomEventGen and spout>   PLUG-<expNum>  <rate as 1x,2x>  <outputLogPath>   <tasks.properties File Path>   <microTaskName>
 

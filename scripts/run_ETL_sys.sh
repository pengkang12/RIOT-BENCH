home_path="/home/cc/"
home_source=${home_path}"storm/riot-bench/modules/tasks/src/main/resources/"

${home_path}storm/bin/storm kill ETLTopologySYS

cd ~/storm/riot-bench/

~/maven/bin/mvn clean compile package -DskipTests

cd -

sleep 60

${home_path}storm/bin/storm jar ${home_path}storm/riot-bench/modules/storm/target/iot-bm-storm-0.1-jar-with-dependencies.jar in.dream_lab.bm.stream_iot.storm.topo.apps.ETLTopology C ETLTopology-sys ${home_source}SYS_sample_data_senml.csv SENML 1.0   ${home_path}storm/riot-bench/output/    ${home_source}tasks.properties  test

#  Command Meaning: topology-fully-qualified-name <local-or-cluster> <Topo-name> <input-dataset-path-name> <Experi-Run-id> <scaling-factor> 
#<output dir name> <tasks properites filename> <tasks name>

# <task name> only uses in micro. 
#    Example command: SampleTopology L NA /var/tmp/bangalore.csv E01-01 0.001

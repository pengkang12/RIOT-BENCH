home_path="/home/cc/"
home_source=${home_path}"storm/riot-bench/modules/tasks/src/main/resources/"

data='SYS'
${home_path}storm/bin/storm kill ETLTopology${data}Simplify

${home_path}storm/bin/storm kill ETLTopology${data}Simplify2

cd ~/storm/riot-bench/

~/maven/bin/mvn clean compile package -DskipTests

cd -
sleep 10

${home_path}storm/bin/storm jar ${home_path}storm/riot-bench/modules/storm/target/iot-bm-storm-0.1-jar-with-dependencies.jar in.dream_lab.bm.stream_iot.storm.topo.apps.ETLTopologySimplify C ETLTopology${data}Simplify ${home_source}${data}_sample_data_senml.csv SENML 0.02   ${home_path}storm/riot-bench/output/    ${home_source}tasks.properties  test

exit

${home_path}storm/bin/storm jar ${home_path}storm/riot-bench/modules/storm/target/iot-bm-storm-0.1-jar-with-dependencies.jar in.dream_lab.bm.stream_iot.storm.topo.apps.ETLTopologySimplify2 C ETLTopology${data}Simplify2 ${home_source}${data}_sample_data_senml.csv SENML 0.02   ${home_path}storm/riot-bench/output/    ${home_source}tasks.properties  test

exit
${home_path}storm/bin/storm jar ${home_path}storm/riot-bench/modules/storm/target/iot-bm-storm-0.1-jar-with-dependencies.jar in.dream_lab.bm.stream_iot.storm.topo.apps.ETLTopologySimplify1 C ETLTopology${data}Simplify1 ${home_source}${data}_sample_data_senml.csv SENML 0.1   ${home_path}storm/riot-bench/output/    ${home_source}tasks.properties  test

exit


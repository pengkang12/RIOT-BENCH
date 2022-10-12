bash /home/cc/storm/riot-bench/scripts/run_ETL_simply.sh
redis-cli flushall

for i in {1..5}
do
sleep 120
python perf.py ETLTopologySYSSimplify &
#
#python3 collect_data.py >> bo.log &
done

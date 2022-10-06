package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.BlockWindowAverage;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BlockWindowAverageBolt extends BaseRichBolt {

    private Properties p;
    private ArrayList<String> useMsgList;
    public BlockWindowAverageBolt(Properties p_){
        p=p_;
    }
    OutputCollector collector; private static Logger l;  public static void initLogger(Logger l_) {     l = l_; }
    Map<String, BlockWindowAverage> blockWindowAverageMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector; initLogger(LoggerFactory.getLogger("APP"));
        blockWindowAverageMap = new HashMap<String, BlockWindowAverage>();
        String useMsgField = p.getProperty("AGGREGATE.BLOCK_AVERAGE.USE_MSG_FIELD");
        String [] msgField = useMsgField.split(",");
        useMsgList = new ArrayList<String>();
        for(String s : msgField )
        {
            useMsgList.add(s);
        }
    }


    @Override
    public void execute(Tuple input) {

        String msgId = input.getStringByField("MSGID");
        String sensorMeta=input.getStringByField("META");
        String sensorID=input.getStringByField("SENSORID");
        String obsType=input.getStringByField("OBSTYPE");
        String obsVal = input.getStringByField("OBSVAL");

        String airquality=obsVal.split(",")[4]; // airquality as last obs. in input


        if(useMsgList.contains(obsType))
        {
            String key = sensorID + obsType;
            BlockWindowAverage blockWindowAverage = blockWindowAverageMap.get(key);
            if(blockWindowAverage == null){
                blockWindowAverage=new BlockWindowAverage();
                blockWindowAverage.setup(l,p);
                blockWindowAverageMap.put(key, blockWindowAverage);
            }

            HashMap<String, String> map = new HashMap<String, String>();

            map.put(AbstractTask.DEFAULT_KEY, airquality);
            blockWindowAverage.doTask(map);

            Float avgres = blockWindowAverage.getLastResult();  //  Avg of last window is used till next comes
            sensorMeta = sensorMeta.concat(",").concat(obsType);
            obsType = "AVG";

            if(avgres!=null ) {
                if(avgres!=Float.MIN_VALUE)
                {

                    collector.emit(new Values(sensorMeta,sensorID,obsType,avgres.toString(),obsVal,msgId,"AVG"));

                }
                else {
                    if (l.isWarnEnabled()) l.warn("Error in BlockWindowAverageBolt");
                    throw new RuntimeException();
                }
            }
        }
    }

    @Override
    public void cleanup()
    {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("META","SENSORID","OBSTYPE","AVGRES","OBSVAL","MSGID","ANALAYTICTYPE"));
    }


}

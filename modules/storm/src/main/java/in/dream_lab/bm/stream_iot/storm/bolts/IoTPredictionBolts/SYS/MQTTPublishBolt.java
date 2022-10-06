package in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.io.MQTTPublishTask;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MQTTPublishBolt extends BaseRichBolt {

    private Properties p;

    public MQTTPublishBolt(Properties p_)
    {
         p=p_;

    }
    OutputCollector collector;
    private static Logger l; 
    public static void initLogger(Logger l_) {     l = l_; }
    MQTTPublishTask mqttPublishTask; 
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        initLogger(LoggerFactory.getLogger("APP"));

        mqttPublishTask= new MQTTPublishTask();

        mqttPublishTask.setup(l,p);
    }

    @Override
    public void execute(Tuple input) 
    {
    	String msgId = (String)input.getValueByField("MSGID");
    	String meta = (String)input.getValueByField("META");
//    	String obsType = (String)input.getValueByField("OBSTYPE");
        String analyticsType = input.getStringByField("ANALAYTICTYPE");
    	String obsVal = input.getStringByField("OBSVAL");

        StringBuffer temp=new StringBuffer();
        String res="";
        if(analyticsType.equals("DTC")){
            res = (String) input.getValueByField("RES");
            temp.append(msgId).append(",").append(meta).append(",").append(analyticsType).append(",obsVal:").append(obsVal).append(",RES:").append(res);
        }
        if(analyticsType.equals("MLR")){
            res =  (input.getValueByField("ERROR")).toString();
            temp.append(msgId).append(",").append(meta).append(",").append(analyticsType).append(",obsVal:").append(obsVal).append(",ERROR:").append(res);
        }


        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, String.valueOf(temp));
    	mqttPublishTask.doTask(map);
    	collector.emit(new Values(msgId, meta, obsVal));
    }

    @Override
    public void cleanup() {
    	mqttPublishTask.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MSGID", "META", "OBSVAL"));
    }

}


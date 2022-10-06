package in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobUploadTask;
import in.dream_lab.bm.stream_iot.tasks.io.SQLBlobUploadTask;

public class SQLBlobUploabBolt extends BaseRichBolt {

    private Properties p;

    public SQLBlobUploabBolt(Properties p_){
        p=p_;

    }

    OutputCollector collector;
    private static Logger l; // TODO: Ensure logger is initialized before use
    public static void initLogger(Logger l_) {
        l = l_;
    }

    SQLBlobUploadTask sqlBlobUploadTask;
    String baseDirname="";
    String fileName="T";
    String datasetName="";

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector=outputCollector;
        initLogger(LoggerFactory.getLogger("APP"));

        sqlBlobUploadTask=new SQLBlobUploadTask();

        sqlBlobUploadTask.setup(l,p);


    }

    @Override
    public void execute(Tuple input) {
        String res = "0";
        String msgId = input.getStringByField("MSGID");

        fileName=input.getStringByField("FILENAME");

        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, fileName);
	//l.info("BLOBUPLOAD map "+map.toString());
        Float blobRes = sqlBlobUploadTask.doTask(map);

        
        if(blobRes!=null ) {
	    if(blobRes!=Float.MIN_VALUE)	
	    {
		//l.info("BLOBUPLOAD map1 "+blobRes.toString());
                collector.emit(new Values(msgId,fileName));
            }else {
                if (l.isWarnEnabled()) l.warn("Error in SqlBlobUploadTaskBolt");
                throw new RuntimeException();
            }
	}
    }

    @Override
    public void cleanup() {
    	sqlBlobUploadTask.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("MSGID","FILENAME"));
    }

}

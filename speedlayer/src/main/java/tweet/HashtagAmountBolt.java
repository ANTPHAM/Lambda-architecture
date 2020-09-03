package tweet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class HashtagAmountBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;

	}

	@Override
	public void execute(Tuple input) {
		try {
			SaveMongoDb(input);
		} catch (ParseException e) {
			e.printStackTrace();
			outputCollector.fail(input);
		}

	}
	
    public void SaveMongoDb(Tuple input) throws ParseException 
    { 
    	String hashTag = input.getStringByField("hashtag");
    	Long time_ms = (Long)input.getLongByField("time_ms");
    	//using the method IncrementHashCount in the class MongoDBClass to put data to MongoDb 
    	MongoDBClass.IncrementHashCount(time_ms,hashTag, 1);
    	
    	
       
    } 

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}

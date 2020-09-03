package speedlayer;

import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.storm.command.list;
import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TweetParsingBolt extends BaseRichBolt {
private OutputCollector outputCollector;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;

	}

	@Override
	public void execute(Tuple input) {
		try {
			process(input); 
		} catch (ParseException e) {
			e.printStackTrace();
			outputCollector.fail(input);
		}

	}
	
	public void process(Tuple input) throws ParseException {
		JSONParser jsonParser = new JSONParser();
		JSONObject data = (JSONObject)jsonParser.parse(input.getStringByField("value"));
		Long id = (Long)data.get("id");
		String created_time = (String)data.get("created_at");
		String time_ms = (String)data.get("timestamp_ms");
		Long time_msLong = Long.parseLong(time_ms);
		String text = (String)data.get("text");
		
		JSONObject h = ((JSONObject)data.get("entities"));
		List<JSONObject> hash = (List<JSONObject>)(h.get("hashtags"));
		List<String> hashtags = new ArrayList<String>();
		for (Object outObj : hash) {
			String hashStr = (String)((JSONObject)outObj).get("text");
			outputCollector.emit(new Values(id, created_time, time_msLong,hashStr,text));
		}
		
		outputCollector.ack(input);
		System.out.printf("====== Tweets: ID: %d  Created at: %s === Hastags: %s\n",id,created_time, hashtags);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "created_time", "time_ms","hashtag","text"));

	}

}

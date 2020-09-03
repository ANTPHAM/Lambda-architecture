package speedlayer;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;


public class App 
{
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
    	TopologyBuilder builder = new TopologyBuilder();
    	// Création d'un objet KafkaSpoutConfigBuilder 
        // On passe au constructeur l'adresse d'un broker Kafka ainsi que 
        // le nom d'un topic KafkaSpoutConfig.Builder
    	KafkaSpoutConfig.Builder<String, String> spoutConfigBuilder = KafkaSpoutConfig.builder("localhost:9093", "tweet_analysis");
    	// On définit ici le groupe Kafka auquel va appartenir le spout
    	spoutConfigBuilder.setGroupId("hashtag_dashboard"); 
    	// Création d'un objet KafkaSpoutConfig
    	KafkaSpoutConfig<String, String> spoutConfig = spoutConfigBuilder.build();
    	// Création d'un objet KafkaSpout
    	builder.setSpout("tweet_consumer", new KafkaSpout<String, String>(spoutConfig));
    	// Création d'un objet KafkaBolt
    	builder.setBolt("tweet-parsing", new TweetParsingBolt())
    	.shuffleGrouping("tweet_consumer");
    	builder.setBolt("mongo-db",  new HashtagAmountBolt())
     	.shuffleGrouping("tweet-parsing");
    	
    	StormTopology topology = builder.createTopology();

    	Config config = new Config();
    	config.setMessageTimeoutSecs(60*30);
    	String topologyName = "tweetanalysis"; 
    	if(args.length > 0 && args[0].equals("remote")) {
    		StormSubmitter.submitTopology(topologyName, config, topology);
    	}
    	else {
    		LocalCluster cluster;
			try {
				cluster = new LocalCluster();
	        	cluster.submitTopology(topologyName, config, topology);
			} catch (Exception e) {
				// generer un catch block
				e.printStackTrace();
			}
    	}
    }
}

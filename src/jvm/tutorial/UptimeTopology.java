package tutorial;

// Standard Storm Imports
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;

import tutorial.spouts.UptimeSpout;
import tutorial.bolts.LoadParserBolt;


public class UptimeTopology {
	
	public static void main(String[] args) throws Exception {
		// Build the topology.
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("uptime", new UptimeSpout(), 1);
		builder.setBolt("parseLoad", new LoadParserBolt(), 1)
				.shuffleGrouping("uptime");
		
		// Set some configuration options to enable debugging.
		Config conf = new Config();
		conf.setDebug(true);
		
		// Run the cluster in local mode.
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		Utils.sleep(60000);
		cluster.killTopology("test");
		cluster.shutdown(); 
	}
}

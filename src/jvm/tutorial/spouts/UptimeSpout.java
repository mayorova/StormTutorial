package tutorial.spouts;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.Utils;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;

public class UptimeSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		try {
			Runtime r = Runtime.getRuntime();
			Process p = r.exec("/usr/bin/uptime");
            InputStream stdout = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(stdout);
            BufferedReader br = new BufferedReader(isr);
            String lastLine = null;
            String tmpLine = null;
            while ( (tmpLine = br.readLine()) != null)
            {
            	lastLine = tmpLine;
            }
			int returnCode = p.waitFor();
			if (returnCode != 0)
			{
				throw new RuntimeException("uptime command returned non-zero value: " + returnCode);
			}				

			
			if (lastLine != null)
			{
				_collector.emit(new Values(lastLine));					
			}
		}
		catch (Exception e)
		{
			throw new RuntimeException("Failed to execute uptime command: " + e.getMessage());
		}
		
		// Do a short sleep, we'll keep getting polled for tuples 
		// even if there isn't anything there.
		Utils.sleep(1000);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}

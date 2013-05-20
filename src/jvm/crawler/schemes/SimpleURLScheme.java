package crawler.schemes;

import java.util.LinkedList;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;

public class SimpleURLScheme implements Scheme {

	@Override
	public List<Object> deserialize(byte[] ser) {
		List<Object> results = new LinkedList<Object>();
		results.add(new String(ser));
		return results;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("url");
	}

}

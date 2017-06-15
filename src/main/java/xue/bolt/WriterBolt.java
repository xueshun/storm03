package xue.bolt;

import java.io.FileWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WriterBolt implements IRichBolt{
	private static final long serialVersionUID = 1L;
	
	private FileWriter writer;
	
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			writer = new FileWriter("F://message.txt");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private boolean flag = false;

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		try {
			if(!flag && word.equals("hadoop")){
				flag =true;
				int a = 1/0;
			}
			writer.write(word);
			writer.write("\n\r");
			writer.flush();
		} catch (Exception e) {
			e.printStackTrace();
			collector.fail(input);
		}
		collector.emit(input,new Values(word));
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}

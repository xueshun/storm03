package xue.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SpliterBolt implements IRichBolt {
	private static final long serialVersionUID = 1L;

	private OutputCollector collector ;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	private boolean flag = false;
	@Override
	public void execute(Tuple input) {
		try {
			String subjects = input.getStringByField("subjects");
			
			String[] words = subjects.split(",");
			for (String word : words) {
				//注意这里循环发送消息，要携带tuple对象，用于处理异常时重发策略
				collector.emit(input,new Values(word));
			}
			collector.ack(input);
		} catch (Exception e) {
			e.printStackTrace();
			collector.fail(input);
		}
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
	@Override
	public void cleanup() {
		
	}


	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}

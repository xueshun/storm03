package xue.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import xue.bolt.SpliterBolt;
import xue.bolt.WriterBolt;
import xue.spout.MessageSpout;

/**
 * Storm系统中有一组叫做“acker” 的特殊任务，他们负责跟踪DAG(有向无环图)中的每个信息
 * 
 * acker任务保存了spout消息ID到一对值的映射，第一个值就是spout的任务id，通过这个id，acker
 * 		就知道消息处理完成时该通知哪个spout任务。
 * 		第二个值是一个64bit的数字我们称作为“ack val”,它是树中所有消息的随机id的异或结果
 * 		ack val表示了整个树的状态，无论这个树有多大，另需要这个固定大小的数字就可以跟踪整个树，当
 * 		消息被创建和被应答的时候都会有相同的消息id发送过来做异或运算
 * 
 * 每当acker发现一个树的ack val值为0的时候，它就知道这棵树已经被完全处理 了。因为消息的随机id是一个64bit
 * 		的值，因此ack val在树处理完之前被置为0的概率非常小。
 * @author Administrator
 *
 */
public class MessageTopology {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MessageSpout());
		builder.setBolt("split-bolt", new SpliterBolt()).shuffleGrouping("spout");
		builder.setBolt("write-bolt", new WriterBolt()).shuffleGrouping("split-bolt");
		
		//本地配置
		Config config = new Config();
        config.setDebug(false);
		
        LocalCluster cluster = new LocalCluster();
        System.out.println(cluster);
        cluster.submitTopology("message", config, builder.createTopology());
        Thread.sleep(10000);
        cluster.killTopology("message");
        cluster.shutdown();
	}
}

//package com.hzw.test
//
//
//import com.hzw.fdc.json.JsonUtil.toJsonNode
//import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.functions.sink.SinkFunction
//import org.apache.flink.streaming.api.scala.DataStream
//import org.apache.flink.test.util.MiniClusterWithClientResource
//import org.codehaus.jackson.JsonNode
//import org.scalatest._
//import org.scalatest.flatspec.AnyFlatSpec
//import org.scalatest.matchers.should
//import org.slf4j.{Logger, LoggerFactory}
//
//import java.util._
//
//
//
///**
// * @author ：gdj
// * @date ：Created in 2021/11/5 14:19
// * @description：${description }
// * @modified By：
// * @version: $version$
// */
//class StreamingJobIntegrationTest extends AnyFlatSpec with should.Matchers with BeforeAndAfter {
//
//  private val logger: Logger = LoggerFactory.getLogger(classOf[StreamingJobIntegrationTest])
//
//  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
//    .setNumberSlotsPerTaskManager(1)
//    .setNumberTaskManagers(1)
//    .build)
//
//  before {
//    flinkCluster.before()
//  }
//
//  after {
//    flinkCluster.after()
//  }
//
//
//  "IncrementFlatMapFunction pipeline" should "incrementValues" in {
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    // configure your test environment
//    env.setParallelism(2)
//
//    // values are collected in a static variable
//    CollectSink.values.clear()
//
//
//    val node = toJsonNode("", logger)
//
//    val value1 :DataStream[JsonNode]= env.fromElements(node, node, node).addSink(new CollectSink())
//
//
//    // create a stream of custom elements and apply transformations
////    env.fromElements(node, node, node)
////      .map(x=>{x})
////      .addSink(new CollectSink())
//
//    // execute
//    env.execute()
//
//    // verify your results
//    CollectSink.values should contain allOf (node, node, node)
//  }
//}
//
//// create a testing sink
//class CollectSink extends SinkFunction[JsonNode] {
//
//  override def invoke(value: JsonNode, context: SinkFunction.Context): Unit = {
//    println(v)
//    CollectSink.values
//
//  }
//}
//
//object CollectSink {
//  // must be static
//  val values: util.List[Long] = Collections.synchronizedList(new util.ArrayList())

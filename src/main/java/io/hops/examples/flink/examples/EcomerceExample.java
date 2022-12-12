package io.hops.examples.flink.examples;

import com.logicalclocks.base.engine.FeatureGroupUtils;
import com.logicalclocks.flink.FeatureStore;
import com.logicalclocks.flink.HopsworksConnection;
import com.logicalclocks.flink.StreamFeatureGroup;
import io.hops.examples.flink.ecomerce.StoreEventDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.time.Duration;
import java.util.Properties;

public class EcomerceExample {
  private FeatureGroupUtils utils = new FeatureGroupUtils();
  
  public void run(String featureGroupName, Integer featureGroupVersion, String sourceTopic) throws Exception {
    
    String windowType = "tumbling";
    Duration maxOutOfOrderness = Duration.ofSeconds(60);
    
    // define flink env
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().enableObjectReuse();
    env.enableCheckpointing(30000);
    
    //get feature store handle
    FeatureStore fs = HopsworksConnection.builder().build().getFeatureStore();
    
    // get or create stream feature group
    StreamFeatureGroup featureGroup = fs.getOrCreateStreamFeatureGroup(featureGroupName, featureGroupVersion);
    
    // get kafka
    Properties kafkaProperties = utils.getKafkaProperties(featureGroup, null);
    
    // define transaction source
    KafkaSource<StoreEvent> transactionSource = KafkaSource.<StoreEvent>builder()
      .setProperties(kafkaProperties)
      .setTopics(sourceTopic)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.of(new StoreEventDeserializer()))
      //.setValueOnlyDeserializer(new TransactionsDeserializer())
      .build();
    
    // define watermark strategy
    WatermarkStrategy<StoreEvent> customWatermark = WatermarkStrategy
      .<StoreEvent>forBoundedOutOfOrderness(maxOutOfOrderness)
      .withTimestampAssigner((event, timestamp) -> event.getDeserializationTimestamp());
  
    
    /*
    DataStream<Map<String, Object>> aggregationStream = (DataStream<Map<String, Object>>) env.fromSource(transactionSource, customWatermark,
        "Transaction Kafka " +
        "Source")
      .rescale()
      .rebalance()
      .keyBy(r -> r.getKey().toString())
      .filter(r -> r.getEventType().toString().equals("ADD_TO_BAG"))
      .keyBy(r -> r.getKey().toString())
      .window(TumblingEventTimeWindows.of(Time.minutes(60)))
      .aggregate(new CountAggregate());
  
    
    // insert stream
    featureGroup.insertStream(aggregationStream);
     */
    
    env.execute("Window aggregation of " + windowType);
  }
  
  public static void main(String[] args) throws Exception {
  
    Options options = new Options();
  
    options.addOption(Option.builder("featureGroupName")
      .argName("featureGroupName")
      .required(true)
      .hasArg()
      .build());
  
    options.addOption(Option.builder("featureGroupVersion")
      .argName("featureGroupVersion")
      .required(true)
      .hasArg()
      .build());
  
    options.addOption(Option.builder("sourceTopic")
      .argName("sourceTopic")
      .required(true)
      .hasArg()
      .build());
  
    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);
  
    String featureGroupName = commandLine.getOptionValue("featureGroupName");
    Integer featureGroupVersion = Integer.parseInt(commandLine.getOptionValue("featureGroupVersion"));
    String sourceTopic = commandLine.getOptionValue("sourceTopic");
  
    EcomerceExample ecomerceExample = new EcomerceExample();
    ecomerceExample.run(featureGroupName, featureGroupVersion, sourceTopic);
  }
  
}

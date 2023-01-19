package io.hops.examples.spark;

import io.hops.util.Hops;

import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.MainClass;

import com.logicalclocks.hsfs.engine.SparkEngine;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.stddev;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.avro.functions.from_avro;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

public class TransactionFraudExample {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionFraudExample.class);
  private static final String TOPIC = "";
  
  public static void main(String[] args) throws Exception {
    
    HopsworksConnection connection = HopsworksConnection.builder().build();
    
    FeatureStore fs = connection.getFeatureStore();
    LOGGER.info("Feature Store " + fs);
  
    // get or create stream feature groups
    StreamFeatureGroup featureGroup10m = fs.getOrCreateStreamFeatureGroup("card_transactions_10m_agg",
      1,
      "description",
      Arrays.asList("cc_num"),
      null,  // arrays.asList("country")
      null,
      true,
      new StatisticsConfig(true, true, true, true),
      "eventTime");
  
    StreamFeatureGroup featureGroup1h = fs.getOrCreateStreamFeatureGroup("card_transactions_1h_agg",
      1,
      "description",
      Arrays.asList("cc_num"),
      null,  // arrays.asList("country")
      null,
      true,
      new StatisticsConfig(true, true, true, true),
      "eventTime");
  
    StreamFeatureGroup featureGroup12h = fs.getOrCreateStreamFeatureGroup("card_transactions_12h_agg",
      1,
      "description",
      Arrays.asList("cc_num"),
      null,  // arrays.asList("country")
      null,
      true,
      new StatisticsConfig(true, true, true, true),
      "eventTime");
  
  
    Dataset<Row> dfFromKafka = SparkEngine.getInstance().getSparkSession().readStream().format("kafka").
      option("kafka.bootstrap.servers", Hops.getBrokerEndpoints()).
      option("subscribe", TOPIC).
      option("startingOffsets", "earliest").
      option("kafka.security.protocol", "SSL").
      option("kafka.ssl.truststore.location", Hops.getTrustStore()).
      option("kafka.ssl.truststore.password", Hops.getKeystorePwd()).
      option("kafka.ssl.keystore.location", Hops.getKeyStore()).
      option("kafka.ssl.keystore.password", Hops.getKeystorePwd()).
      option("kafka.ssl.key.password", Hops.getKeystorePwd()).
      option("kafka.ssl.endpoint.identification.algorithm", "").
      load();

    //Dataset<Row> dfDeser = dfFromKafka.select(from_avro(col("value"), Hops.getSchema(TOPIC)).as("logs"));
    Dataset<Row> dfDeser = dfFromKafka.selectExpr("CAST(value AS STRING)")
      .select(from_avro(col("value"), Hops.getSchema(TOPIC)).alias("value"))
      .select("value.tid", "value.datetime", "value.cc_num", "value.amount")
      .selectExpr("CAST(tid as string)", "CAST(datetime as string)", "CAST(cc_num as long)", "CAST(amount as double)");
    
    Dataset<Row> windowed10mSignalDF = dfDeser
      .selectExpr("CAST(tid as string)", "CAST(datetime as timestamp)", "CAST(cc_num as long)",
        "CAST(amount as double)")
      .withWatermark("datetime", "60 minutes")
      .groupBy(window(col("datetime"), "10 minutes"), col("cc_num"))
      .agg(avg("amount").alias("avg_amt_per_10m"), stddev("amount").alias("stdev_amt_per_10m"),
        count("cc_num").alias("num_trans_per_10m"))
      .select("cc_num", "num_trans_per_10m", "avg_amt_per_10m", "stdev_amt_per_10m");
  
    featureGroup10m.insertStream(windowed10mSignalDF);
    
    Dataset<Row> windowed1hSignalDF = dfDeser
      .selectExpr("CAST(tid as string)", "CAST(datetime as timestamp)", "CAST(cc_num as long)",
        "CAST(amount as double)")
      .withWatermark("datetime", "60 minutes")
      .groupBy(window(col("datetime"), "60 minutes"), col("cc_num"))
      .agg(avg("amount").alias("avg_amt_per_1h"), stddev("amount").alias("stdev_amt_per_1h"),
        count("cc_num").alias("num_trans_per_1h"))
      .select("cc_num", "num_trans_per_1h", "avg_amt_per_1h", "stdev_amt_per_1h");
  
    featureGroup1h.insertStream(windowed1hSignalDF);
  
    Dataset<Row> windowed12hSignalDF = dfDeser
      .selectExpr("CAST(tid as string)", "CAST(datetime as timestamp)", "CAST(cc_num as long)",
        "CAST(amount as double)")
      .withWatermark("datetime", "60 minutes")
      .groupBy(window(col("datetime"), "12 hours"), col("cc_num"))
      .agg(avg("amount").alias("avg_amt_per_12h"), stddev("amount").alias("stdev_amt_per_12h"),
        count("cc_num").alias("num_trans_per_1h"))
      .select("cc_num", "num_trans_per_1h", "avg_amt_per_1h", "stdev_amt_per_1h");
  
    featureGroup12h.insertStream(windowed12hSignalDF);
  }
}

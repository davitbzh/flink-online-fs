package io.hops.examples.spark;

import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.engine.SparkEngine;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

public class EmptyFeatureGroups {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmptyFeatureGroups.class);
  
  public static void main(String[] args) throws IOException, FeatureStoreException, ParseException {
    HopsworksConnection connection = HopsworksConnection.builder().build();
  
    FeatureStore fs = connection.getFeatureStore();
    LOGGER.info("Feature Store " + fs);
  
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkEngine.getInstance().getSparkSession().sparkContext());
  
    List<StructField> schema_10m = Arrays.asList(
      DataTypes.createStructField("cc_num", DataTypes.LongType, true),
      DataTypes.createStructField("num_trans_per_10m", DataTypes.LongType, true),
      DataTypes.createStructField("avg_amt_per_10m", DataTypes.DoubleType, true),
      DataTypes.createStructField("stdev_amt_per_10m", DataTypes.DoubleType, true)
    );
    StructType Fg10mSchema = DataTypes.createStructType(schema_10m);
  
    List<StructField> schema_1h = Arrays.asList(
      DataTypes.createStructField("cc_num", DataTypes.LongType, true),
      DataTypes.createStructField("num_trans_per_1h", DataTypes.LongType, true),
      DataTypes.createStructField("avg_amt_per_1h", DataTypes.DoubleType, true),
      DataTypes.createStructField("stdev_amt_per_1h", DataTypes.DoubleType, true)
    );
    StructType Fg1hSchema = DataTypes.createStructType(schema_1h);
  
    List<StructField> schema_12h = Arrays.asList(
      DataTypes.createStructField("cc_num", DataTypes.LongType, true),
      DataTypes.createStructField("num_trans_per_12h", DataTypes.LongType, true),
      DataTypes.createStructField("avg_amt_per_12h", DataTypes.DoubleType, true),
      DataTypes.createStructField("stdev_amt_per_12h", DataTypes.DoubleType, true)
    );
    StructType Fg12hSchema = DataTypes.createStructType(schema_12h);
    
    Dataset<Row> fg10mDf =
      SparkEngine.getInstance().getSparkSession().createDataFrame(jsc.emptyRDD(), Fg10mSchema);
    Dataset<Row> fg1hDf =
      SparkEngine.getInstance().getSparkSession().createDataFrame(jsc.emptyRDD(), Fg1hSchema);
    Dataset<Row> fg12hDf =
      SparkEngine.getInstance().getSparkSession().createDataFrame(jsc.emptyRDD(), Fg12hSchema);
    
    // get or create stream feature groups
    FeatureGroup
      featureGroup10m = fs.getOrCreateFeatureGroup("card_transactions_10m_agg",
      3, Arrays.asList("cc_num"), true, null);
    
    FeatureGroup featureGroup1h = fs.getOrCreateFeatureGroup("card_transactions_1h_agg",
      3, Arrays.asList("cc_num"), true, null);
    
    FeatureGroup featureGroup12h = fs.getOrCreateFeatureGroup("card_transactions_12h_agg",
      3, Arrays.asList("cc_num"), true, null);
    
    featureGroup10m.insert(fg10mDf);
    featureGroup1h.insert(fg1hDf);
    featureGroup12h.insert(fg12hDf);
  }
}

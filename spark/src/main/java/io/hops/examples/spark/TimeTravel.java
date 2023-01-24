package io.hops.examples.spark;

import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.hsfs.FeatureGroup;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureView;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.StatisticsConfig;
import com.logicalclocks.hsfs.TimeTravelFormat;
import com.logicalclocks.hsfs.constructor.Query;
import com.logicalclocks.hsfs.engine.SparkEngine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TimeTravel {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeTravel.class);
  
  public static void main(String[] args)
    throws IOException, FeatureStoreException, ParseException, SQLException, ClassNotFoundException {
  
    HopsworksConnection connection = HopsworksConnection.builder().build();
  
    FeatureStore fs = connection.getFeatureStore();
    LOGGER.info("Feature Store " + fs);
    
    List<StructField> marketingFields = Arrays.asList(
      DataTypes.createStructField("customer_id", DataTypes.IntegerType, true),
      DataTypes.createStructField("ts", DataTypes.LongType, true),
      DataTypes.createStructField("complaints_7d", DataTypes.IntegerType, true),
      DataTypes.createStructField("outbound_7d", DataTypes.IntegerType, true),
      DataTypes.createStructField("coupon_14d", DataTypes.IntegerType, true)
    );
    StructType marketingSchema = DataTypes.createStructType(marketingFields);
  
    List<StructField> contractFields = Arrays.asList(
      DataTypes.createStructField("contract_id", DataTypes.IntegerType, true),
      DataTypes.createStructField("ts", DataTypes.LongType, true),
      DataTypes.createStructField("contract_type", DataTypes.StringType, true)
    );
    StructType contractSchema = DataTypes.createStructType(contractFields);
  
    List<StructField> churnFields = Arrays.asList(
      DataTypes.createStructField("customer_id", DataTypes.IntegerType, true),
      DataTypes.createStructField("contract_id", DataTypes.IntegerType, true),
      DataTypes.createStructField("ts", DataTypes.LongType, true),
      DataTypes.createStructField("contract_churn", DataTypes.IntegerType, true)
    );
    StructType churnSchema = DataTypes.createStructType(churnFields);
    
    
    List<Row> marketingRows = Arrays.asList(
      RowFactory.create(1, 10010L, 0, 0, 1),
      RowFactory.create(1, 10174L, 3, 0, 4),
      RowFactory.create(1, 10257L, 7, 0, 3),
      RowFactory.create(1, 10352L, 3, 0, 5),
      RowFactory.create(1, 10753L, 0, 0, 0),
      RowFactory.create(1, 10826L, 0, 0, 1),
  
      RowFactory.create(2, 10017L, 0, 1, 1),
      RowFactory.create(2, 10021L, 0, 1, 1),
      RowFactory.create(2, 10034L, 0, 1, 2),
  
      RowFactory.create(3, 10035L, 1, 3, 0),
      RowFactory.create(3, 10275L, 1, 2, 0),
  
      RowFactory.create(5, 10546L, 0, 1, 0),
      RowFactory.create(5, 10598L, 2, 2, 1),
      RowFactory.create(5, 13567L, 0, 1, 0),
      RowFactory.create(5, 16245L, 0, 1, 0),
  
      RowFactory.create(6, 10213L, 0, 0, 1),
      RowFactory.create(6, 10234L, 0, 5, 0),
      RowFactory.create(6, 10436L, 0, 0, 1),
  
      RowFactory.create(7, 10056L, 0, 0, 0),
      RowFactory.create(7, 10056L, 0, 1, 0),
      RowFactory.create(7, 10056L, 0, 2, 1),
      RowFactory.create(7, 10056L, 0, 3, 0),
  
      RowFactory.create(8, 10012L, 0, 0, 1),
      RowFactory.create(8, 10023L, 10, 0, 1),
      RowFactory.create(8, 10033L, 0, 0, 1)
    );
    
    List<Row> newContractRows = Arrays.asList(
      RowFactory.create(1, 100, 10010L, 0),
      RowFactory.create(2, 101, 10017L, 0),
      RowFactory.create(3, 102, 10035L, 0),
      RowFactory.create(4, 103, 10023L, 0),
      RowFactory.create(5, 104, 10546L, 0),
      RowFactory.create(6, 105, 10213L, 0),
      RowFactory.create(7, 106, 10056L, 0),
      RowFactory.create(8, 107, 10012L, 0)
    );
  
    List<Row> contractRows = Arrays.asList(
      RowFactory.create(100, 10010L, "Long-term"),
      RowFactory.create(101, 10017L, "Short-term"),
      RowFactory.create(102, 10035L, "Trial"),
      RowFactory.create(103, 10023L, "Short-term"),
      RowFactory.create(104, 10546L, "Long-term"),
      RowFactory.create(105, 10213L, "Trial"),
      RowFactory.create(106, 10056L, "Long-term"),
      RowFactory.create(107, 10012L, "Short-term")
    );
  
    List<Row> churnedContracts = Arrays.asList(
      RowFactory.create(1, 100, 10356L, 1),
      RowFactory.create(5, 104, 10692L, 1),
      RowFactory.create(6, 105, 10375L, 1),
      RowFactory.create(8, 107, 10023L, 1)
    );
 
    // create dataframes
    Dataset<Row> marketingDF =
      SparkEngine.getInstance().getSparkSession().createDataFrame(marketingRows, marketingSchema);
  
    Dataset<Row> contractsDf =
      SparkEngine.getInstance().getSparkSession().createDataFrame(contractRows, contractSchema);
  
    Dataset<Row> newContractsDf =
      SparkEngine.getInstance().getSparkSession().createDataFrame(newContractRows, churnSchema);
    
    Dataset<Row> churnedContractsDf =
      SparkEngine.getInstance().getSparkSession().createDataFrame(churnedContracts, churnSchema);
  
    // create feature groups
    FeatureGroup marketingFg =
      fs.getOrCreateFeatureGroup("marketing", 1,
        "Features about inbound/outbound communication with customers", Arrays.asList("customer_id"),
        null, null, true, TimeTravelFormat.HUDI, null, "ts");
    marketingFg.insert(marketingDF);
  
    FeatureGroup contractsFg = fs.getOrCreateFeatureGroup("contracts", 1, "Contract information features",
      Arrays.asList("contract_id"), null, null, true, TimeTravelFormat.HUDI,
      null, "ts");
    contractsFg.insert(contractsDf);
  
    FeatureGroup churnFg = fs.getOrCreateFeatureGroup("churn", 1,
      "Contract information features", Arrays.asList("customer_id"), null, null,
      true, TimeTravelFormat.HUDI, null, "ts");
    churnFg.insert(newContractsDf);
    churnFg.insert(churnedContractsDf);
    
    // query 
    Query query = churnFg.selectAll().join(
        contractsFg.select(Arrays.asList("contract_type")), Arrays.asList("contract_id"))
      .join(marketingFg.select(
          Arrays.asList("complaints_7d", "outbound_7d", "coupon_14d")),
        Arrays.asList("customer_id"));
  
    // create feature view
    FeatureView fv = fs.getOrCreateFeatureView("marketingFV", query, 1);
  
    // sering vector
    fv.initServing();
  
    Map<String, Object> pkMap = new HashMap<String, Object>() {
      {
        put("customer_id", 1);
        put("contract_id" , 100);
      }
    };
  
    fv.getFeatureVector(pkMap);
  }
}
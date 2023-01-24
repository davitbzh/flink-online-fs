package io.hops.examples.spark;

import com.logicalclocks.hsfs.DataFormat;
import com.logicalclocks.hsfs.FeatureStore;
import com.logicalclocks.hsfs.FeatureView;
import com.logicalclocks.hsfs.HopsworksConnection;
import com.logicalclocks.hsfs.StreamFeatureGroup;
import com.logicalclocks.hsfs.constructor.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class FeatureViewTrainingData {
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureViewTrainingData.class);
  
  public static void main(String[] args) throws Exception {
  
    HopsworksConnection connection = HopsworksConnection.builder().build();
  
    FeatureStore fs = connection.getFeatureStore();
    LOGGER.info("Feature Store " + fs);
  
    // get or create stream feature groups
    StreamFeatureGroup featureGroup10m = fs.getOrCreateStreamFeatureGroup("card_transactions_10m_agg", 1);
    StreamFeatureGroup featureGroup1h = fs.getOrCreateStreamFeatureGroup("card_transactions_1h_agg", 1);
    StreamFeatureGroup featureGroup12h = fs.getOrCreateStreamFeatureGroup("card_transactions_12h_agg", 1);
    
    // Select features for training data.
    Query query =
      featureGroup10m.select(Arrays.asList("stdev_amt_per_10m", "avg_amt_per_10m", "num_trans_per_10m"))
        .join(featureGroup1h.select(
          Arrays.asList("stdev_amt_per_1h", "avg_amt_per_1h", "num_trans_per_1h")))
        .join(featureGroup12h.select(
          Arrays.asList("stdev_amt_per_12h", "avg_amt_per_12h", "num_trans_per_12h")));
    
    // create feature view
    FeatureView fv = fs.getOrCreateFeatureView("transaction", query, 1);
    
    // create training dataset
    fv.createTrainTestSplit((float) 0.2, null, null, null, null, "description",
      DataFormat.CSV);
    
    // sering vector
    fv.initServing();
  
    List<String> cardNumbers =
      Arrays.asList("4867010117638802", "4564139086560436", "4638396144844325", "4460285888258185", "4032763187099525");
    
    for (int i= 0; i< cardNumbers.size(); i++){
      int finalI = i;
      fv.getFeatureVector(new HashMap<String, Object>() {{put("cc_num", cardNumbers.get(finalI));}});
    }
  }
}

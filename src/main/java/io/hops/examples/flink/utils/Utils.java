package io.hops.examples.flink.utils;

import com.logicalclocks.base.FeatureStoreException;
import com.logicalclocks.flink.HopsworksConnection;
import com.logicalclocks.base.metadata.HopsworksClient;
import com.logicalclocks.base.metadata.HopsworksHttpClient;

import java.io.IOException;
import java.util.Properties;

public class Utils {
  
  public Properties getKafkaProperties(String topic) throws FeatureStoreException, IOException {
    HopsworksConnection connection = HopsworksConnection.builder().build();
    HopsworksHttpClient client = HopsworksClient.getInstance().getHopsworksHttpClient();
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "broker.kafka.service.consul:9091");
    properties.put("security.protocol", "SSL");
    properties.put("ssl.truststore.location", client.getTrustStorePath());
    properties.put("ssl.truststore.password", client.getCertKey());
    properties.put("ssl.keystore.location", client.getKeyStorePath());
    properties.put("ssl.keystore.password", client.getCertKey());
    properties.put("ssl.key.password", client.getCertKey());
    properties.put("ssl.endpoint.identification.algorithm", "");
    properties.put("topic", topic);
    return properties;
  }
  
  public Properties getKafkaProperties() throws FeatureStoreException, IOException {
    HopsworksConnection connection = HopsworksConnection.builder().build();
    HopsworksHttpClient client = HopsworksClient.getInstance().getHopsworksHttpClient();
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "broker.kafka.service.consul:9091");
    properties.put("security.protocol", "SSL");
    properties.put("ssl.truststore.location", client.getTrustStorePath());
    properties.put("ssl.truststore.password", client.getCertKey());
    properties.put("ssl.keystore.location", client.getKeyStorePath());
    properties.put("ssl.keystore.password", client.getCertKey());
    properties.put("ssl.key.password", client.getCertKey());
    properties.put("ssl.endpoint.identification.algorithm", "");
    return properties;
  }
  
}

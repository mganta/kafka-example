package kafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;


public class CustomPartitioner  implements Partitioner {

    public void configure(Map<String, ?> configs) {}

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
       
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        
        //optional check
        if(numPartitions < 3)
              return 0;
        
        if (keyBytes == null) {
                return 0;
        } else {
            return getPartitionIdForKey(keyBytes);
        }
    }

    public static int getPartitionIdForKey(byte[] keyBytes) {
        String keyString = new String(keyBytes);
        switch (keyString) {
        case "partition1":
               return 1;
        case "partition2":
               return 2;
        default:
               return 0;
        }
    }
    
    public void close() {}
}


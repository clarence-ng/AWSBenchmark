package com.awsbenchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Metrics {

    private static final String format = "%-15s\t%7.2f\t%4d\t%4d\t%4d\t%4d\t%4d\t%4d";
    public static final Log logger = LogFactory.getLog(Metrics.class);

    public final Queue<Long> latencies = new ConcurrentLinkedQueue<Long>();
    public volatile long throttled;
    public volatile long serviceError;
    public volatile long clientError;
    public volatile long error;
    public volatile long ok;
    public volatile double iopsConsumed;
    
    
    public void apply(Metrics other) {
        this.throttled += other.throttled;
        this.serviceError += other.serviceError;
        this.clientError += other.clientError;
        this.error += other.error;
        this.ok += other.ok;
        this.iopsConsumed += other.iopsConsumed;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Metrics [throttled=").append(throttled)
                .append(", serviceError=").append(serviceError)
                .append(", clientError=").append(clientError)
                .append(", error=").append(error).append(", ok=").append(ok)
                .append(", iopsConsumed=").append(iopsConsumed)
                .append("]");
        return builder.toString();
    }
    
    public static final void computeTp(String name, Queue<Long> latencies) {
        ArrayList<Long> sorted = new ArrayList<Long>(latencies);
        Collections.sort(sorted);

        int indexOfTp50 = (int) (sorted.size() * 0.50D);
        int indexOfTp90 = (int) (sorted.size() * 0.90D);
        int indexOfTp95 = (int) (sorted.size() * 0.95D);
        int indexOfTp99 = (int) (sorted.size() * 0.99D);
        int indexOfTp999 = (int) (sorted.size() * 0.999D);
        
//        logger.info(indexOfTp90 + " " + indexOfTp95 + " " + indexOfTp99 + " " + indexOfTp999 + " " + (sorted.size() -1));
        long tp50 = sorted.get(indexOfTp50);
        long tp90 = sorted.get(indexOfTp90);
        long tp95 = sorted.get(indexOfTp95);
        long tp99 = sorted.get(indexOfTp99);
        long tp999 = sorted.get(indexOfTp999);
        long tp100 = sorted.get(sorted.size() -1);
        
        long totalLatency = 0;
        for (long l : sorted) {
            totalLatency += l;
        }
        double tpAvg = (double) totalLatency / sorted.size();
        logger.info(String.format(format, name, tpAvg, tp50, tp90, tp95, tp99, tp999, tp100));
    }
    
    
}

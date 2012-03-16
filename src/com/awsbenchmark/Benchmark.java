package com.awsbenchmark;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.Request;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.handlers.RequestHandler;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.CreateTableRequest;
import com.amazonaws.services.dynamodb.model.DescribeTableRequest;
import com.amazonaws.services.dynamodb.model.GetItemRequest;
import com.amazonaws.services.dynamodb.model.GetItemResult;
import com.amazonaws.services.dynamodb.model.Key;
import com.amazonaws.services.dynamodb.model.KeySchema;
import com.amazonaws.services.dynamodb.model.KeySchemaElement;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodb.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodb.model.PutItemRequest;
import com.amazonaws.services.dynamodb.model.PutItemResult;
import com.amazonaws.services.dynamodb.model.TableDescription;
import com.amazonaws.services.dynamodb.model.TableStatus;
import com.amazonaws.util.TimingInfo;

/**
 * Example command
 * 
 * java -jar -Dcom.amazonaws.sdk.enableRuntimeProfiling=1 -Dnetworkaddress.cache.ttl=-1 Benchmark.jar  put 1000 10000 0 10000000 1 2 5 72.21.202.144 HTTP
 * 
 */
public class Benchmark {

    private static final Logger logger = Logger.getLogger(Benchmark.class);

    private final String clientMetrics = "Client";
    private final String signingMetrics = "Signing";
    private final String unmarhsallingMetrics = "Unmarshalling";
    private final String marshallingMetrics = "Marshalling";
    private final String httpclientMetrics = "httpclient";
    private final String processResMetrics = "processResponse";
    private final String beforeSendMetrics = "beforeSend";

    volatile Metrics metrics = new Metrics();
    volatile Queue<Long> signing = new ConcurrentLinkedQueue<Long>();
    volatile Queue<Long> unmarshalling = new ConcurrentLinkedQueue<Long>();
    volatile Queue<Long> marshalling = new ConcurrentLinkedQueue<Long>();
    volatile Queue<Long> httpclient = new ConcurrentLinkedQueue<Long>();
    volatile Queue<Long> beforeSend = new ConcurrentLinkedQueue<Long>();
    volatile Metrics total = new Metrics();
    
    private final ThreadLocal<Long> requestStarts = new ThreadLocal<Long>();

    private final Random randomizer = new Random();

    private final Queue<PutItemRequest> puts = new ConcurrentLinkedQueue<PutItemRequest>();
    private final Queue<GetItemRequest> gets = new ConcurrentLinkedQueue<GetItemRequest>();

    private volatile AmazonDynamoDBClient client;
    private volatile String endpoint;
    
    public static void main(String... args) throws Exception {
        java.security.Security.setProperty("networkaddress.cache.ttl" , "-1");
        java.security.Security.setProperty("networkaddress.cache.negative.ttl" , "0");
        
        PropertyConfigurator.configure("log4j.properties");
        new Benchmark().run(args);
    }

    public void run(String... args) throws Exception {
        final String operation = args[0];
        final int itemBytes = Integer.parseInt(args[1]);
        final long iops = Long.parseLong(args[2]);
        final String tableName = "Benchmarking" + iops;
        final long itemStart = Long.parseLong(args[3]);
        final long itemCount = Long.parseLong(args[4]);
        
        final int threads = Integer.parseInt(args[5]);
        final int maxConns = Integer.parseInt(args[6]);
        
        final int metricsPeriod = Integer.parseInt(args[7]);
        endpoint = args[8];
        final String protocol = args[9];

        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(threads + 2);

        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setMaxConnections(maxConns);
        configuration.setMaxErrorRetry(0);
        configuration.setProtocol(Protocol.valueOf(protocol));

        AWSCredentials credentials = new PropertiesCredentials(new File("AwsCredentials.properties"));
        client = new AmazonDynamoDBClient(credentials, configuration);
        client.setEndpoint(endpoint);
        client.addRequestHandler(new RequestHandler() {
            @Override
            public void beforeRequest(Request<?> request) {
                // TODO Auto-generated method stub

            }

            @Override
            public void afterResponse(Request<?> request, Object response,
                    TimingInfo timingInfo) {
                Number prep = timingInfo.getCounter("beforeSend");
                Number sign = timingInfo.getCounter("signing");
                Number http = timingInfo.getCounter("httpclient");
                Number unmarshall = null;
                TimingInfo responseTime = timingInfo.getSubMeasurement("response-processing");
                if (responseTime != null) {
                    unmarshall = timingInfo.getSubMeasurement("response-processing").getEndTime() - timingInfo.getSubMeasurement("response-processing").getStartTime();
                }

                if (prep != null) {
                    signing.add(sign.longValue());
                }
                if (http != null) {
                    httpclient.add(http.longValue());
                }
                if (unmarshall != null) {
                    unmarshalling.add(unmarshall.longValue());
                }
                if (prep != null) {
                    beforeSend.add(prep.longValue());
                }
                
                if (requestStarts.get() != null) {
                    marshalling.add(timingInfo.getStartTime() - requestStarts.get()); 
                }
            }

            @Override
            public void afterError(Request<?> request, Exception e) {
                // TODO Auto-generated method stub

            }

        });

        final Semaphore permits = new Semaphore(5 * 1000);
        executor.execute(new Runnable(){
            @Override
            public void run() {
                try {

                    long i = 0;
                    while (true) {

                        try {
                            permits.acquire();
                        } catch(InterruptedException e) {
                            logger.info("object create interrupted", e);
                            return;
                        }

                        if (operation.equalsIgnoreCase("get")) {
                            gets.add(get(tableName, client, i + itemStart));
                        } else { 
                            puts.add(put(tableName, client, i + itemStart, itemBytes));
                        }

                        i = (i + 1) % itemCount;

                    }

                }
                catch (Exception e1) {
                    logger.error(e1);
                }
            }
        });

        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch doneLatch = new CountDownLatch(threads);
        final long itemPerThread = (long) ((double) itemCount) / threads;

        for (long i = 0; i < threads; i++) {

            final long start = i * itemPerThread;

            executor.execute(new Runnable(){

                @Override
                public void run() {

                    try {
                        latch.await();

                        while (true) {
                            for (long i = start; i < start + itemPerThread ; i++) {
                                long start = System.currentTimeMillis();
                                requestStarts.set(start);
                                boolean succeed = true;
                                try {
                                    if (operation.equalsIgnoreCase("get")) {
                                        GetItemResult r = client.getItem(gets.poll());
                                        metrics.iopsConsumed += r.getConsumedCapacityUnits();
                                    } else { 
                                        PutItemResult r = client.putItem(puts.poll());
                                        metrics.iopsConsumed += r.getConsumedCapacityUnits();
                                    }
                                    long end = System.currentTimeMillis();
                                    metrics.latencies.add(end - start);
                                } catch(ProvisionedThroughputExceededException e) {
                                    metrics.throttled ++;
                                    succeed = false;
                                } catch(AmazonServiceException e) {
                                    metrics.serviceError ++;
                                    succeed = false;
                                    logger.error(e);
                                } catch(AmazonClientException e) {
                                    metrics.clientError ++;
                                    succeed = false;
                                    logger.error(e);
                                } catch(Exception e) {
                                    metrics.error ++;
                                    succeed = false;
                                    logger.error(e);
                                } finally {
                                    permits.release();
                                    if (succeed) {
                                        metrics.ok ++;
                                    }
                                }
                            }
                        }

                    } catch (Exception e1) {
                        logger.error(e1);
                    } finally {
                        logger.info("done submitted requests");
                        doneLatch.countDown();
                    }
                }

            });

        }

        createTable("Benchmarking" + iops, client);
        logger.info("verified table exists");
        latch.countDown();

        Future<?> f = executor.scheduleWithFixedDelay(new Runnable(){

            @Override
            public void run() {
                try {
                    Metrics oldClient = metrics;
                    Queue<Long> oldbeforeSend = beforeSend;
                    Queue<Long> oldsigning = signing;
                    Queue<Long> oldhttpclient = httpclient;
                    Queue<Long> oldunmarshalling = unmarshalling;
                    Queue<Long> oldmarshalling = marshalling;
                    Queue<Long> oldprocessRes = client.client.processRes;
                    
                    metrics = new Metrics();
                    marshalling = new ConcurrentLinkedQueue<Long>();
                    beforeSend = new ConcurrentLinkedQueue<Long>();
                    signing = new ConcurrentLinkedQueue<Long>();
                    httpclient = new ConcurrentLinkedQueue<Long>();
                    unmarshalling = new ConcurrentLinkedQueue<Long>();
                    client.client.processRes = new ConcurrentLinkedQueue<Long>();
                    
                    Metrics.computeTp(beforeSendMetrics, oldbeforeSend);
                    Metrics.computeTp(marshallingMetrics, oldmarshalling);
                    Metrics.computeTp(signingMetrics, oldsigning);
                    Metrics.computeTp(httpclientMetrics, oldhttpclient);
                    Metrics.computeTp(unmarhsallingMetrics, oldunmarshalling);
                    Metrics.computeTp(processResMetrics, oldprocessRes);
                    Metrics.computeTp(clientMetrics, oldClient.latencies);

                    logger.info(oldClient);
                    total.apply(oldClient);
                    logger.info(total);
                } catch (Exception e) {
                    logger.error(e);
                }
            }

        }, 5, metricsPeriod, TimeUnit.SECONDS);
        
        doneLatch.await();
        executor.shutdown();
        executor.shutdownNow();
        f.get();
    }

    private PutItemRequest put(String tableName, AmazonDynamoDBClient client, long seed, int byteCount) {
        Map<String, AttributeValue> item = newItem(seed, byteCount);
        return new PutItemRequest(tableName, item);
    }

    private GetItemRequest get(String tableName, AmazonDynamoDBClient client, long seed) {
        return new GetItemRequest()
        .withTableName(tableName)
        .withKey(new Key().withHashKeyElement(new AttributeValue(String.valueOf(seed))));
    }

    private Map<String, AttributeValue> newItem(long seed, int byteCount) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("Key", new AttributeValue(String.valueOf(seed)));
        item.put("Value", new AttributeValue(newString(byteCount)));
        return item;
    }

    private String newString(int byteCount) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < byteCount; i++) {
            int nextSeed = randomizer.nextInt();
            //Generates random character between A and Y. Because random is generating sigedn ints the integer generated below is really ranging from 65(A) to 89(Y)
            builder.append((char) ((nextSeed % 13) + 77)); 
        }
        return builder.toString();
    }

    public static void createTable(String tableName, AmazonDynamoDBClient client) {
        DescribeTableRequest request = new DescribeTableRequest()
        .withTableName(tableName);
        
        boolean needCreate = true;
        try{
            TableDescription tableDescription = client.describeTable(
                    request).getTable();
            if (TableStatus.ACTIVE.equals(TableStatus.valueOf(tableDescription.getTableStatus()))) {
                return;
            }
            logger.info("checking table status: " + tableDescription);
            needCreate = false;
        } catch( AmazonClientException e) {
            //continue
        }
        
        if (needCreate) {
            // Create a table with a primary key named 'name', which holds a string
            CreateTableRequest createTableRequest = new CreateTableRequest()
            .withTableName(tableName)
            .withKeySchema(new KeySchema(new KeySchemaElement().withAttributeName("Key").withAttributeType("S")))
            .withProvisionedThroughput(new ProvisionedThroughput()
            .withReadCapacityUnits(10000L)
            .withWriteCapacityUnits(10000L));
            
            TableDescription createdTableDescription = client.createTable(
                    createTableRequest).getTableDescription();
            
            logger.info("Created Table: " + createdTableDescription);
        }

        // Wait for it to become active

        waitForTableToBecomeAvailable(tableName, client);
    }

    private static void waitForTableToBecomeAvailable(String tableName, AmazonDynamoDBClient client) {
        System.out.println("Waiting for " + tableName + " to become ACTIVE...");

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(1000 * 20);
            } catch (Exception e) {
                logger.error("", e);
            }
            try {
                DescribeTableRequest request = new DescribeTableRequest()
                .withTableName(tableName);
                TableDescription tableDescription = client.describeTable(
                        request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                System.out.println("  - current state: " + tableStatus);
                logger.info("  - current state: " + tableStatus);
                if (tableStatus.equals(TableStatus.ACTIVE.toString()))
                    return;
            } catch (AmazonServiceException ase) {
                if (ase.getErrorCode().equalsIgnoreCase(
                "ResourceNotFoundException") == false)
                    throw ase;
            }
        }

        throw new RuntimeException("Table " + tableName + " never went active");
    }
}

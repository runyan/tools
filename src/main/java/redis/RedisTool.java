package redis;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A tool to transfer data from a source Redis to a target Redis
 *
 * @author yanrun
 **/
@Slf4j
@SuppressWarnings({"unused"})
public class RedisTool implements AutoCloseable {

    /**
     * default timeout
     */
    private static final int DEFAULT_TIMEOUT = 3600 * 1000;
    /**
     * default port
     */
    private static final int DEFAULT_PORT = 6379;
    /**
     * default Redis database
     */
    private static final int DEFAULT_DATABASE = 0;

    /**
     * used to check the result of {@link redis.clients.jedis.Jedis#ttl}
     * this value represents a key which never expired
     */
    private static final int KEY_NEVER_EXPIRED = -1;
    /**
     * used to check the result of {@link redis.clients.jedis.Jedis#ttl}
     * this value represents a key does not exists
     */
    private static final int KEY_NOT_EXISTS = -2;

    /**
     * used by {@link redis.RedisTool#handleData} to determine whether to use multi-thread mode or not
     */
    private static final int MAX_NUM_PER_THREAD = 10000;

    /**
     * max number of threads
     */
    private static final int MAX_THREAD_NUM = 500;

    /**
     * number of threads used by multi-thread mode
     */
    private int threadNum;
    /**
     * used by multi-thread mode, number of items handled by thread
     */
    private int itemPerThread;

    /**
     * used by multi-thread mode, accumulated number of data transferred
     */
    private AtomicInteger totalNum = new AtomicInteger(0);

    /**
     * source Redis connection pool
     */
    private final JedisPool srcJedisPool;
    /**
     * target Redis connection pool
     */
    private final JedisPool targetJedisPool;

    /**
     * source Redis
     */
    private final Jedis srcJedis;
    /**
     * target Redis
     */
    private final Jedis targetJedis;

    private final CopyOnWriteArrayList<String> failedKeyList = new CopyOnWriteArrayList<>();

    private static final int MAX_RETRY_TIMES = 3;

    public RedisTool(RedisConfig srcRedisConfig, RedisConfig targetRedisConfig) {
        if(Objects.isNull(srcRedisConfig)) {
            throw new NullPointerException("Source Redis config cannot be null");
        }
        if(Objects.isNull(targetRedisConfig)) {
            throw new NullPointerException("Target Redis config cannot be null");
        }
        srcJedisPool = initJedisPool(srcRedisConfig);
        targetJedisPool = initJedisPool(targetRedisConfig);
        srcJedis = srcJedisPool.getResource();
        targetJedis = targetJedisPool.getResource();
    }

    /**
     * Init Redis poll config
     *
     * @param redisConfig config to be used
     * @return Redis poll after configuration
     */
    private JedisPool initJedisPool(RedisConfig redisConfig) {
        GenericObjectPoolConfig redisPoolConfig = new GenericObjectPoolConfig();
        int maxIdle = redisConfig.getMaxIdle();
        if(maxIdle > 0) {
            redisPoolConfig.setMaxIdle(maxIdle);
        }
        int minIdle = redisConfig.getMinIdle();
        if(minIdle > 0) {
            redisPoolConfig.setMinIdle(minIdle);
        }
        int maxTotal = redisConfig.getMaxTotal();
        if(maxTotal > 0) {
            redisPoolConfig.setMaxTotal(maxTotal);
        }
        String host = redisConfig.getHost();
        host = Optional.ofNullable(host).orElseThrow(() -> new NullPointerException("host cannot be null"));
        int port = redisConfig.getPort();
        if(port <= 0) {
            port = DEFAULT_PORT;
        }
        String password = redisConfig.getPassword();
        password = Optional.ofNullable(password).orElse("");
        int database = redisConfig.getDatabase();
        if(database < 0) {
            database = DEFAULT_DATABASE;
        }
        int timeout = redisConfig.getTimeout();
        if(timeout < 0) {
            timeout = DEFAULT_TIMEOUT;
        }
        return new JedisPool(redisPoolConfig, host, port, timeout, password, database);
    }

    /**
     * Return the total amount of data transferred
     *
     * @return total amount of data transferred
     */
    public int getTotalNumOfDataTransferred() {
        return totalNum.intValue();
    }

    /**
     * Close the used Redis pools
     */
    @Override
    public void close() {
        closeJedisConnections();
        closeJedisPools();
    }

    /**
     * Close Jedis pools
     */
    private void closeJedisPools() {
        closeJedisPool(srcJedisPool);
        closeJedisPool(targetJedisPool);
    }

    /**
     * Close a Jedis pool
     *
     * @param jedisPool Jedis pool to be closed
     */
    private void closeJedisPool(JedisPool jedisPool) {
        if(Objects.nonNull(jedisPool) && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }

    /**
     * Close jedis connections
     */
    private void closeJedisConnections() {
        closeJedis(srcJedis);
        closeJedis(targetJedis);
    }

    /**
     * Close and release jedis connection
     *
     * @param jedis jedis to be closed
     */
    private void closeJedis(Jedis jedis) {
        if(Objects.nonNull(jedis) && jedis.isConnected()) {
            jedis.disconnect();
            jedis.close();
        }
    }

    /**
     * Transfer data of a known key from source to target
     *
     * @param key Redis key
     * @param shouldDeleteOnFind  if true and the key already exists in target Redis,
     *                            data in target Redis will be deleted
     */
    public void transferKnownKeyData(String key, boolean shouldDeleteOnFind) {
        long ttl = srcJedis.ttl(key);
        if(ttl == KEY_NOT_EXISTS) {
            throw new IllegalArgumentException(key + " does not exists");
        }
        String type = srcJedis.type(key);
        if(targetJedis.exists(key) && shouldDeleteOnFind) {
            targetJedis.del(key);
        }
        doAddData(key, type, srcJedis, targetJedis);
        if(ttl != KEY_NEVER_EXPIRED) {
            targetJedis.expireAt(key, calculateUnixTime(ttl));
        }
    }

    /**
     * Transfer data of a known key pattern from source Redis to target Redis
     *
     * @param keyPattern Redis key pattern
     */
    public void transferKnownKeyPatternData(String keyPattern) {
        List<String> keys = scanKeys(keyPattern, srcJedis);
        handleData(keys);
    }

    /**
     * Add newly added data to target Redis
     *
     * @param keyPattern Redis key pattern
     */
    public void syncAdditionalData(String keyPattern) {
        List<String> srcKeys = scanKeys(keyPattern, srcJedis);
        int srcKeySize = srcKeys.size();
        if(srcKeySize == 0) {
            log.warn("cannot find key pattern {} in source Redis", keyPattern);
            return;
        }
        log.info("source db key size: {}", srcKeySize);
        List<String> targetKeys = scanKeys(keyPattern, targetJedis);
        log.info("target db key size: {}", targetKeys.size());
        srcKeys.removeAll(targetKeys);
        log.info("num of data need to be transferred: {}", srcKeys.size());
        handleData(srcKeys);
    }

    /**
     * Check existence of a Redis key and determine whether should continue parse this key
     *
     * @param key Redis key
     * @param ttl time to live
     * @return should continue parse this key
     */
    private boolean handleTimeToLive(String key, long ttl) {
        boolean shouldContinue = true;
        if(ttl == KEY_NOT_EXISTS) {
            log.info("key: {} expired", key);
            shouldContinue = false;
        }
        return shouldContinue;
    }

    /**
     * Check Redis keys and choose which mode to use(single-thread or multi-thread)
     *
     * @param keyList Redis key list
     */
    private void handleData(List<String> keyList) {
        if(Objects.isNull(keyList) || keyList.isEmpty()) {
            log.warn("empty keys");
            return;
        }
        int totalItems = keyList.size();
        log.info("num of keys: {}", totalItems);
        if(totalItems > MAX_NUM_PER_THREAD) {
            useMultiThread(keyList, totalItems);

        } else {
            useSingleThread(keyList);
        }
        if(!failedKeyList.isEmpty()) {
            retry();
        }
    }

    private void retry() {
        List<String> failedKeys = new ArrayList<>(failedKeyList);
        doRetry(failedKeys);
    }

    private void doRetry(List<String> failedKeys) {
        if(Objects.isNull(failedKeys) || failedKeys.isEmpty()) {
            log.warn("empty keys, no need to retry");
            return ;
        }
        int keyNum = failedKeys.size();
        boolean retrySuccess = keyNum == 1 ? singleThreadRetry(failedKeys.get(0)) : multiThreadRetry(failedKeys);
        String msg = retrySuccess ? "retry success" : "retry failed after " + MAX_RETRY_TIMES + " times";
        log.info(msg);
    }

    private boolean singleThreadRetry(String key) {
        int retries = 0;
        while (retries < MAX_RETRY_TIMES) {
            log.info("retry {} times", retries);
            try {
                addDataToRedis(key, srcJedis, targetJedis, Thread.currentThread().getName());
                break;
            } catch (Exception e) {
                log.error("error while retrying: exception = {}", e);
            }
            retries++;
        }
        return retries < MAX_RETRY_TIMES;
    }

    private boolean multiThreadRetry(List<String> keys) {
        int failedKeyNum = keys.size();
        int retries = 0;
        int targetPageNum = 5;
        int itemForOneThread = failedKeyNum / targetPageNum + 4;
        int endIndex;
        List<String> keysPerThread;
        ExecutorService executorService = Executors.newFixedThreadPool(targetPageNum);
        Future<List<String>> retryFailed;
        List<String> retryFailedKeys;
        List<String> needToRetryKeys = new ArrayList<>();
        while (retries < MAX_RETRY_TIMES) {
            log.info("retry {} times", retries);
            for(int i = 0; i < targetPageNum; i++) {
                endIndex = (i + 1) * itemForOneThread;
                endIndex = endIndex > failedKeyNum ? failedKeyNum : endIndex;
                keysPerThread = keys.subList(i * itemForOneThread, endIndex);
                retryFailed = executorService.submit(new RetryTask(keysPerThread));
                try {
                    retryFailedKeys = retryFailed.get();
                    if(!retryFailedKeys.isEmpty()) {
                        needToRetryKeys.addAll(retryFailedKeys);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if(needToRetryKeys.isEmpty()) {
                break;
            }
            itemForOneThread = needToRetryKeys.size() / targetPageNum + 4;
            retries++;
        }
        executorService.shutdown();
        return retries < MAX_RETRY_TIMES;
    }

    /**
     * Use single thread to transfer data
     *
     * @param keyList Redis key list
     */
    private void useSingleThread(List<String> keyList) {
        if(Objects.isNull(keyList) || keyList.isEmpty()) {
            log.warn("empty keys");
            return;
        }
        log.info("using single thread transfer");
        int num = 0;
        String threadName = Thread.currentThread().getName();
        for(String key : keyList) {
            addDataToRedis(key, srcJedis, targetJedis, threadName);
        }
        log.info("Num of data added: {}", num);
    }

    /**
     * Use multi-thread to transfer data
     *
     * @param keyList Redis key list
     */
    private void useMultiThread(List<String> keyList, int totalItems) {
        int pages = totalItems % MAX_NUM_PER_THREAD;
        int tempPageNum = totalItems / MAX_NUM_PER_THREAD;
        threadNum = pages == 0 ? tempPageNum : tempPageNum + 1;
        List<String> subList;
        int endIndex;
        if(threadNum > MAX_NUM_PER_THREAD) {
            handleGiganticSize(totalItems);
        } else {
            itemPerThread = MAX_NUM_PER_THREAD;
        }
        log.info("using multi-thread transfer, thread num: {}", threadNum);
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for(int i = 0; i < threadNum; i++) {
            endIndex = (i + 1) * itemPerThread;
            endIndex = endIndex > totalItems ? totalItems : endIndex;
            subList = keyList.subList(i * itemPerThread, endIndex);
            executorService.submit(new SubTask(subList, countDownLatch));
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("exception while countDownlatch awaits: {}", e);
            Thread.currentThread().interrupt();
        } finally {
            executorService.shutdown();
        }
    }

    /**
     * recalculate items handled per thread, to make sure thead number will not exceeds {@link redis.RedisTool#MAX_THREAD_NUM}
     *
     * @param totalItems total items
     */
    private void handleGiganticSize(int totalItems) {
        threadNum = MAX_THREAD_NUM;
        itemPerThread = totalItems / threadNum;
    }

    /**
     * Transfer data from source Redis to target Redis
     *
     * @param key Redis key
     * @param type type of the Redis data
     * @param srcJedis source Redis
     * @param targetJedis target Redis
     */
    private void doAddData(String key, String type, Jedis srcJedis, Jedis targetJedis) {
        if(Objects.isNull(key) || key.isEmpty()) {
            throw new NullPointerException("empty key");
        }
        if(Objects.isNull(type) || type.isEmpty()) {
            throw new NullPointerException("empty type");
        }
        if(Objects.isNull(srcJedis)) {
            throw new NullPointerException("source Jedis is null");
        }
        if(Objects.isNull(targetJedis)) {
            throw new NullPointerException("target Jedis is null");
        }
        switch (type) {
            case "string":
                targetJedis.set(key, srcJedis.get(key));
                break;
            case "hash":
                targetJedis.hset(key, srcJedis.hgetAll(key));
                break;
            case "list":
                long length = srcJedis.llen(key);
                List<String> list = srcJedis.lrange(key, 0, length);
                String[] listArr = collectionToArray(list);
                targetJedis.rpush(key, listArr);
                break;
            case "set":
                Set<String> set = srcJedis.smembers(key);
                String[] setArr = collectionToArray(set);
                targetJedis.sadd(key, setArr);
                break;
            default:
                throw new IllegalStateException("unsupported type: " + type);
        }
    }

    /**
     * Convert type from java.util.Collection to String array
     *
     * @param collection collection to be parsed
     * @return String array transformed from collection
     */
    private String[] collectionToArray(Collection<String> collection) {
        if(Objects.isNull(collection) || collection.isEmpty()) {
            throw new NullPointerException("collection is null");
        }
        String[] arr = new String[collection.size()];
        return collection.toArray(arr);
    }

    /**
     * Get keys of a certain pattern from source Redis
     *
     * @param keyPattern key pattern
     * @param jedis source Redis
     * @return keys
     */
    private List<String> scanKeys(String keyPattern, Jedis jedis) {
        if(Objects.isNull(keyPattern) || keyPattern.isEmpty()) {
            throw new NullPointerException("empty keyPattern");
        }
        if(Objects.isNull(jedis)) {
            throw new NullPointerException("cannot scan keys in a null jedis");
        }
        log.info("begin to scan keys for pattern: {}", keyPattern);
        List<String> keyList = new ArrayList<>(MAX_NUM_PER_THREAD);
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams scanParams = new ScanParams();
        scanParams.count(100000);
        scanParams.match(keyPattern);
        List<String> keys;
        ScanResult<String> scanResult;
        do {
            scanResult = jedis.scan(cursor, scanParams);
            keys = scanResult.getResult();
            if(Objects.nonNull(keys) && !keys.isEmpty()) {
                keyList.addAll(keys);
            }
            cursor = scanResult.getCursor();
        } while (!"0".equalsIgnoreCase(cursor));
        keyList = new ArrayList<>(new HashSet<>(keyList));
        log.info("finish scanning, num of keys: {}", keyList.size());
        return keyList;
    }

    /**
     * Get the Unix timestamp based on current time and time to live of a Redis data,
     * i.e. to determine when the data should expired
     *
     * @param ttl time to live
     * @return Unix timestamp when the data should expired
     */
    private long calculateUnixTime(long ttl) {
        long currentTime = System.currentTimeMillis() / 1000;
        return currentTime + ttl;
    }

    private void addDataToRedis(String key, Jedis sourceJedis, Jedis targetJedis, String threadName) {
        if(!targetJedis.exists(key)) {
            String type = sourceJedis.type(key);
            long ttl = sourceJedis.ttl(key.trim());
            if(!handleTimeToLive(key, ttl)) {
                return;
            }
            doAddData(key, type, sourceJedis, targetJedis);
            log.info("{} added key: {}", threadName, key);
            if(ttl > 0) {
                targetJedis.expireAt(key, calculateUnixTime(ttl));
            }
        } else {
            log.info("{} already exists", key);
        }
    }
    /**
     * Used by {@link redis.RedisTool#handleData} in multi-thread mode, transfer partial data to target Redis
     */
    private final class SubTask implements Runnable {

        private List<String> keyList;
        private CountDownLatch countDownLatch;
        private Jedis sourceJedis;
        private Jedis targetJedis;

        SubTask(List<String> keyList, CountDownLatch countDownLatch) {
            this.keyList = keyList;
            this.countDownLatch = countDownLatch;
            sourceJedis = srcJedisPool.getResource();
            targetJedis = targetJedisPool.getResource();
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            int numOfAdded = 0;
            try {
                sourceJedis.connect();
                targetJedis.connect();
                Iterator<String> keyItor = keyList.iterator();
                String key;
                while (keyItor.hasNext()) {
                    key = keyItor.next();
                    addDataToRedis(key, sourceJedis, targetJedis, threadName);
                    totalNum.getAndIncrement();
                    keyItor.remove();
                }
            } catch (Exception e) {
                log.error("exception while adding data, thread: {}, exception: {}", threadName, e);
                failedKeyList.addAll(keyList);
            } finally {
                countDownLatch.countDown();
                log.info("Thread: {}, number of data added: {}", threadName, numOfAdded);
                log.info("{} countDownlatch: {}", threadName, countDownLatch.getCount());
                closeJedisConnections();
            }
        }

        private void closeJedisConnections() {
            closeJedis(targetJedis);
            closeJedis(sourceJedis);
        }
    }

    private class RetryTask implements Callable<List<String>> {

        private List<String> retryFailedKeyList;
        private List<String> failedKeyList;
        private Jedis srcJedis;
        private Jedis targetJedis;

        RetryTask(List<String> failedKeyList) {
            this.failedKeyList = failedKeyList;
            this.retryFailedKeyList = new ArrayList<>();
            srcJedis = srcJedisPool.getResource();
            targetJedis = targetJedisPool.getResource();
            srcJedis.connect();
            targetJedis.connect();
        }

        @Override
        public List<String> call()  {
            String threadName = Thread.currentThread().getName();
            try {
                for(String key : failedKeyList) {
                    try {
                        addDataToRedis(key, srcJedis, targetJedis, threadName);
                        totalNum.getAndIncrement();
                    } catch (Exception e) {
                        retryFailedKeyList.add(key);
                        log.error("error while retrying, thread name: {}, key: {}", threadName, key);
                    }
                }
            } finally {
                targetJedis.disconnect();
                targetJedis.close();
                srcJedis.disconnect();
                srcJedis.close();
            }
            return retryFailedKeyList;
        }
    }

}

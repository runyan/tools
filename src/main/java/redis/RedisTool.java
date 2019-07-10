package redis;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;
import java.util.concurrent.*;

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
     * source Redis connection pool
     */
    private final JedisPool srcJedisPool;
    /**
     * target Redis connection pool
     */
    private final JedisPool destJedisPool;

    /**
     * source Redis
     */
    private final Jedis srcJedis;
    /**
     * target Redis
     */
    private final Jedis destJedis;

    public RedisTool(RedisConfig srcRedisConfig, RedisConfig destRedisConfig) {
        if(Objects.isNull(srcRedisConfig)) {
            throw new NullPointerException("Source Redis config cannot be null");
        }
        if(Objects.isNull(destRedisConfig)) {
            throw new NullPointerException("Destination Redis config cannot be null");
        }
        srcJedisPool = initJedisPool(srcRedisConfig);
        destJedisPool = initJedisPool(destRedisConfig);
        srcJedis = srcJedisPool.getResource();
        destJedis = destJedisPool.getResource();
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
     * Close the used Redis pools
     */
    @Override
    public void close() {
        closeJedises();
        closeJedisPools();
    }

    /**
     * Close Jedis pools
     */
    private void closeJedisPools() {
        closeJedisPool(srcJedisPool);
        closeJedisPool(destJedisPool);
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
    private void closeJedises() {
        closeJedis(srcJedis);
        closeJedis(destJedis);
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
     * Transfer data of a known key from source to destination
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
        if(destJedis.exists(key) && shouldDeleteOnFind) {
            destJedis.del(key);
        }
        addData(key, type, srcJedis, destJedis);
        if(ttl != KEY_NEVER_EXPIRED) {
            destJedis.expireAt(key, calculateUnixTime(ttl));
        }
    }

    /**
     * Transfer data of a known key pattern from source to destination
     *
     * @param keyPattern Redis key pattern
     */
    public void transferKnownKeyPatternData(String keyPattern) {
        List<String> keys = scanKeys(keyPattern, srcJedis);
        handleData(keys);
    }

    /**
     * Add newly added data to destination
     *
     * @param keyPattern Redis key pattern
     */
    public void syncAdditionalData(String keyPattern) {
        List<String> secKeys = scanKeys(keyPattern, srcJedis);
        log.info("source db key size: {}", secKeys.size());
        List<String> destKeys = scanKeys(keyPattern, destJedis);
        log.info("destination db key size: {}", destKeys.size());
        secKeys.removeAll(destKeys);
        log.info("num of data need to be transferred: {}", secKeys.size());
        handleData(secKeys);
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
            log.info("{} expired", key);
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
            return ;
        }
        useSingleThread(keyList);
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
        long ttl;
        boolean shouldContinue;
        String type;
        int num = 0;
        for(String key : keyList) {
            if(!destJedis.exists(key)) {
                type = srcJedis.type(key);
                type = type.toLowerCase();
                ttl = srcJedis.ttl(key);
                shouldContinue = handleTimeToLive(key, ttl);
                if(!shouldContinue) {
                    continue;
                }
                addData(key, type, srcJedis, destJedis);
                if(ttl > 0) {
                    destJedis.expireAt(key, calculateUnixTime(ttl));
                }
                num++;
            } else {
                log.info("{} already exists", key);
            }
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
        int pageNum = pages == 0 ? tempPageNum : tempPageNum + 1;
        List<String> subList;
        int endIndex;
        log.info("using multi-thread transfer, thread num: {}", pageNum);
        ExecutorService executorService = Executors.newFixedThreadPool(pageNum);
        CountDownLatch countDownLatch = new CountDownLatch(pageNum);
        for(int i = 0; i < pageNum; i++) {
            endIndex = (i + 1) * MAX_NUM_PER_THREAD;
            endIndex = endIndex > totalItems ? totalItems : endIndex;
            subList = keyList.subList(i * MAX_NUM_PER_THREAD, endIndex);
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
     * Transfer data from source Redis to target Redis
     *
     * @param key Redis key
     * @param type type of the Redis data
     * @param srcJedis source Redis
     * @param destJedis target Redis
     */
    private void addData(String key, String type, Jedis srcJedis, Jedis destJedis) {
        if(Objects.isNull(key) || key.isEmpty()) {
            throw new NullPointerException("empty key");
        }
        if(Objects.isNull(type) || type.isEmpty()) {
            throw new NullPointerException("empty type");
        }
        if(Objects.isNull(srcJedis)) {
            throw new NullPointerException("null srcJedis");
        }
        if(Objects.isNull(destJedis)) {
            throw new NullPointerException("null destJedis");
        }
        switch (type) {
            case "string":
                destJedis.set(key, srcJedis.get(key));
                break;
            case "hash":
                destJedis.hset(key, srcJedis.hgetAll(key));
                break;
            case "list":
                long length = srcJedis.llen(key);
                List<String> list = srcJedis.lrange(key, 0, length);
                String[] listArr = collectionToArray(list);
                destJedis.rpush(key, listArr);
                break;
            case "set":
                Set<String> set = srcJedis.smembers(key);
                String[] setArr = collectionToArray(set);
                destJedis.sadd(key, setArr);
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
            throw new NullPointerException("null collection");
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
            throw new NullPointerException("null jedis");
        }
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
            targetJedis = destJedisPool.getResource();
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            int numOfAdded = 0;
            try {
                sourceJedis.connect();
                targetJedis.connect();
                String type;
                long ttl;
                for(String key : keyList) {
                    if(!targetJedis.exists(key)) {
                        type = sourceJedis.type(key);
                        ttl = sourceJedis.ttl(key.trim());
                        if(!handleTimeToLive(key, ttl)) {
                            continue;
                        }
                        addData(key, type, sourceJedis, targetJedis);
                        log.info("{} added key: {}", threadName, key);
                        if(ttl > 0) {
                            targetJedis.expireAt(key, calculateUnixTime(ttl));
                        }
                        numOfAdded++;
                    } else {
                        log.info("{} already exists", key);
                    }
                }
            } catch (Exception e) {
                log.error("exception while adding data, thread: {}, exception: {}", threadName, e);
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

}

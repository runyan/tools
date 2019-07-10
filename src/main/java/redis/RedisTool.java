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
 * @author yanrun
 **/
@Slf4j
@SuppressWarnings({"unused"})
public class RedisTool implements AutoCloseable {

    private static final int DEFAULT_TIMEOUT = 3600 * 1000;
    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_DATABASE = 0;

    private static final int KEY_NEVER_EXPIRED = -1;
    private static final int KEY_NOT_EXISTS = -2;

    private static final int MAX_NUM_PER_THREAD = 10000;

    private final JedisPool srcJedisPool;
    private final JedisPool destJedisPool;

    public RedisTool(RedisConfig srcRedisConfig, RedisConfig destRedisConfig) {
        if(Objects.isNull(srcRedisConfig)) {
            throw new NullPointerException("Source Redis config cannot be null");
        }
        if(Objects.isNull(destRedisConfig)) {
            throw new NullPointerException("Destination Redis config cannot be null");
        }
        srcJedisPool = initJedisPool(srcRedisConfig);
        destJedisPool = initJedisPool(destRedisConfig);
    }

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

    @Override
    public void close() {
        if(Objects.nonNull(srcJedisPool) && !srcJedisPool.isClosed()) {
            srcJedisPool.close();
        }
        if(Objects.nonNull(destJedisPool) && !destJedisPool.isClosed()) {
            destJedisPool.close();
        }
    }

    private void closeJedis(Jedis jedis) {
        if(Objects.nonNull(jedis) && jedis.isConnected()) {
            jedis.disconnect();
            jedis.close();
        }
    }

    public void transferKnownKeyData(String key) {
        Jedis srcJedis = srcJedisPool.getResource();
        Jedis destJedis = destJedisPool.getResource();
        long ttl = srcJedis.ttl(key);
        if(ttl == KEY_NOT_EXISTS) {
            throw new IllegalArgumentException(key + " does not exists");
        }
        String type = srcJedis.type(key);
        addData(key, type, srcJedis, destJedis);
        if(ttl != KEY_NEVER_EXPIRED) {
            destJedis.expireAt(key, calculateUnixTime(ttl));
        }
        closeJedis(destJedis);
        closeJedis(srcJedis);
    }

    public void transferKnownKeyPatternData(String keyPattern) {
        Jedis srcJedis = srcJedisPool.getResource();
        List<String> keys = scanKeys(keyPattern, srcJedis);
        handleData(keys);
        closeJedis(srcJedis);
    }

    public void syncAdditionalData(String keyPattern) {
        Jedis srcJedis = srcJedisPool.getResource();
        Jedis destJedis = destJedisPool.getResource();
        List<String> secKeys = scanKeys(keyPattern, srcJedis);
        log.info("source db key size: {}", secKeys.size());
        List<String> destKeys = scanKeys(keyPattern, destJedis);
        log.info("destination db key size: {}", destKeys.size());
        secKeys.removeAll(destKeys);
        log.info("num of data need to be transferred: {}", secKeys.size());
        handleData(secKeys);
        closeJedis(destJedis);
        closeJedis(srcJedis);
    }

    private boolean handleTimeToLive(String key, long ttl) {
        boolean shouldContinue = true;
        if(ttl == KEY_NOT_EXISTS) {
            log.info("{} expired", key);
            shouldContinue = false;
        }
        return shouldContinue;
    }

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

    private void useSingleThread(List<String> keyList) {
        if(Objects.isNull(keyList) || keyList.isEmpty()) {
            log.warn("empty keys");
            return;
        }
        log.info("using single thread transfer");
        Jedis srcJedis = srcJedisPool.getResource();
        Jedis destJedis = destJedisPool.getResource();
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
        closeJedis(destJedis);
        closeJedis(srcJedis);
    }

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

    private String[] collectionToArray(Collection<String> collection) {
        if(Objects.isNull(collection) || collection.isEmpty()) {
            throw new NullPointerException("null collection");
        }
        String[] arr = new String[collection.size()];
        return collection.toArray(arr);
    }

    private List<String> scanKeys(String key, Jedis jedis) {
        if(Objects.isNull(key) || key.isEmpty()) {
            throw new NullPointerException("empty key");
        }
        if(Objects.isNull(jedis)) {
            throw new NullPointerException("null jedis");
        }
        List<String> keyList = new ArrayList<>(MAX_NUM_PER_THREAD);
        String cursor = ScanParams.SCAN_POINTER_START;
        ScanParams scanParams = new ScanParams();
        scanParams.count(100000);
        scanParams.match(key);
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

    private long calculateUnixTime(long ttl) {
        long currentTime = System.currentTimeMillis() / 1000;
        return currentTime + ttl;
    }

    private final class SubTask implements Runnable {

        private List<String> keyList;
        private CountDownLatch countDownLatch;

        SubTask(List<String> keyList, CountDownLatch countDownLatch) {
            this.keyList = keyList;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            Jedis srcJedis = srcJedisPool.getResource();
            Jedis destJedis = destJedisPool.getResource();
            int numOfAdded = 0;
            try {
                srcJedis.connect();
                destJedis.connect();
                String type;
                long ttl;
                for(String key : keyList) {
                    if(!destJedis.exists(key)) {
                        type = srcJedis.type(key);
                        ttl = srcJedis.ttl(key.trim());
                        if(!handleTimeToLive(key, ttl)) {
                            continue;
                        }
                        addData(key, type, srcJedis, destJedis);
                        log.info("{} added key: {}", threadName, key);
                        if(ttl > 0) {
                            destJedis.expireAt(key, calculateUnixTime(ttl));
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
                closeJedis(destJedis);
                closeJedis(srcJedis);
            }
        }
    }

}

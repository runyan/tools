package test;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import redis.RedisConfig;
import redis.RedisTool;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * @author yanrun
 **/
@Slf4j
public class TestRedisTool {

    private RedisConfig srcConfig;
    private RedisConfig destConfig;

    @Before
    public void init() {
        srcConfig = RedisConfig.builder()
                .host("localhost")
                .port(6379)
                .timeout(3600 * 1000)
                .password("password")
                .database(8)
                .build();
        destConfig = RedisConfig.builder()
                .host("localhost")
                .port(6379)
                .timeout(3600 * 1000)
                .password("password")
                .database(0)
                .build();
    }

    @Test
    public void test() {
        try (RedisTool tool = new RedisTool(srcConfig, destConfig)) {
            long startTime = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(8));
            String key = "DISTRIBUTION:CACHE:*";
            tool.transferKnownKeyPatternData(key);
            long endTime = LocalDateTime.now().toEpochSecond(ZoneOffset.ofHours(8));
            log.info("total time: {} s", (endTime - startTime));
        }
    }
}

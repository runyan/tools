package redis;

import lombok.Builder;
import lombok.Data;

/**
 * Redis config class
 * properties to set: maxIdle, minIdle, maxTotal, host, port, timeout, password, database
 *
 * @author yanrun
 **/
@Builder
@Data
public class RedisConfig {

    private int maxIdle;
    private int minIdle;
    private int maxTotal;

    private String host;
    private int port;
    private int timeout;
    private String password;
    private int database;
}

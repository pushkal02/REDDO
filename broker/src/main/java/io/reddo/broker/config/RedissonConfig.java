package io.reddo.broker.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    /**
     * Explicitly configures and registers the RedissonClient bean.
     * This client is used as our Distributed Lock Manager (DLM).
     */
    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        Config config = new Config();
        String redisAddress = String.format("redis://%s:%d", redisHost, redisPort);
        
        config.useSingleServer()
              .setAddress(redisAddress)
              .setConnectionMinimumIdleSize(5)
              .setConnectionPoolSize(10)
              .setConnectTimeout(10000);
        
        return Redisson.create(config);
    }
}

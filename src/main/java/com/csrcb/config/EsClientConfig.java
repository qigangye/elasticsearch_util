package com.csrcb.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @Classname EsClientConfig
 * @Description 获取es的相关配置属性，以及属性值
 * @Date 2021/6/2 17:30
 * @Created by gangye
 */
@Configuration
@Data
public class EsClientConfig {
    @Value("${elasticsearch.config.host}")
    private String hosts;

    @Value("${elasticsearch.config.connectTimeout}")
    private int connectTimeout;

    @Value("${elasticsearch.config.socketTimeout}")
    private int socketTimeout;

    @Value("${elasticsearch.config.maxRetryTimeout}")
    private int maxRetryTimeout;

    @Value("${elasticsearch.config.connRequestTimeOut}")
    private int connRequestTimeOut;

}

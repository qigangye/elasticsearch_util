package com.csrcb.config;

import com.csrcb.common.DefineConstant;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Classname EsClient
 * @Description es的客户端连接
 * @Date 2021/6/3 9:24
 * @Created by gangye
 */
public class EsClient {
    public static String hosts;

    public static int connectTimeout;

    public static int socketTimeout;

    public static int maxRetryTimeout;

    public static int connRequestTimeOut;

    private static RestHighLevelClient restHighLevelClient = null;

    public EsClient(){
        EsClientConfig esClientConfig = new EsClientConfig();
        hosts = esClientConfig.getHosts();
        connectTimeout = esClientConfig.getConnectTimeout();
        socketTimeout = esClientConfig.getSocketTimeout();
        maxRetryTimeout = esClientConfig.getMaxRetryTimeout();
        connRequestTimeOut = esClientConfig.getConnRequestTimeOut();
    }

    public static RestHighLevelClient getInstance(){
        if (restHighLevelClient == null){
            synchronized (RestHighLevelClient.class){
                restHighLevelClient = createClient();
            }
        }
        return restHighLevelClient;
    }

    private static RestHighLevelClient createClient(){
        List<String> sockets = Arrays.asList(hosts.split(DefineConstant.COMMAN_SIGN));
        List<HttpHost> httpHosts = new ArrayList<>();
        for (String socket : sockets){
            httpHosts.add(new HttpHost(socket.split(DefineConstant.COLON_SIGN)[0], Integer.valueOf(socket.split(DefineConstant.COLON_SIGN)[1]),"http"));
        }
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[0])).setMaxRetryTimeoutMillis(maxRetryTimeout);
        builder.setRequestConfigCallback((RequestConfig.Builder requestConfigBuilder) -> {
            requestConfigBuilder.setConnectTimeout(connectTimeout);
            requestConfigBuilder.setConnectionRequestTimeout(connRequestTimeOut);
            requestConfigBuilder.setSocketTimeout(socketTimeout);
            return requestConfigBuilder;
        });

        restHighLevelClient = new RestHighLevelClient(builder);
        return restHighLevelClient;
    }
}

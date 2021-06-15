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
    public static String HOSTS;

    public static int CONNECTTIMEOUT;

    public static int SOCKETTIMEOUT;

    public static int MAXRETRYTIMEOUT;

    public static int CONNREQUESTTIMEOUT;

    private static RestHighLevelClient restHighLevelClient = null;

    public static void setConfigInfo(EsClientConfig esClientConfig) {
        EsClient.HOSTS = esClientConfig.getHosts();
        EsClient.CONNECTTIMEOUT = esClientConfig.getConnectTimeout();
        EsClient.SOCKETTIMEOUT = esClientConfig.getSocketTimeout();
        EsClient.MAXRETRYTIMEOUT = esClientConfig.getMaxRetryTimeout();
        EsClient.CONNREQUESTTIMEOUT = esClientConfig.getConnRequestTimeOut();
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
        List<String> sockets = Arrays.asList(HOSTS.split(DefineConstant.COMMAN_SIGN));
        List<HttpHost> httpHosts = new ArrayList<>();
        for (String socket : sockets){
            httpHosts.add(new HttpHost(socket.split(DefineConstant.COLON_SIGN)[0], Integer.valueOf(socket.split(DefineConstant.COLON_SIGN)[1]),"http"));
        }
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[0])).setMaxRetryTimeoutMillis(MAXRETRYTIMEOUT);
        builder.setRequestConfigCallback((RequestConfig.Builder requestConfigBuilder) -> {
            requestConfigBuilder.setConnectTimeout(CONNECTTIMEOUT);
            requestConfigBuilder.setConnectionRequestTimeout(CONNREQUESTTIMEOUT);
            requestConfigBuilder.setSocketTimeout(SOCKETTIMEOUT);
            return requestConfigBuilder;
        });

        restHighLevelClient = new RestHighLevelClient(builder);
        return restHighLevelClient;
    }
}

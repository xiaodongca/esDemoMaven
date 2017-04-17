package com.cxd.util;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;

/**
 * Created by cai x d
 * on2017/4/14 0014.
 */
public class Connect {

    private static Client client;

    public Connect(){

    }
    /**
     * 连接ElasticSearch数据库
     */
    public  synchronized  static  Client getConnect(){
        try {
            client  = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
            System.out.println("连接成功！！！");
        }catch (Exception e){
            e.printStackTrace();
        }
        return client;
    }
}

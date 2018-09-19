package com.sixth.gp1707.day13;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 20:09 2018/8/8
 * @ 自己操作ES api
 */
public class ElasticSearchByMyself {
    // 这个是用来访问ES的客户端
    private Client client;

    /*
     * 获取Transport Client
     * */
    @Before
    public void getClient() throws UnknownHostException {
        client = TransportClient
                .builder()
                .build()
                .addTransportAddress(
                        new InetSocketTransportAddress(
                                InetAddress.getByName("localhost"), 9300)
                );
    }

    /*
     * 自动创建索引，创建文档，创建映射有三种方式：
     * 1、使用json
     * 2、使用map
     * 3、使用es的帮助类XContentFactory
     * */
    @Test
    public void createDocument_Json() {
        String source = "{" +
                "\"id\":\"1\"," +
                "\"title\":\"testjson\"," +
                "\"content\":\"test by myself\"" +
                "}";
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "5")
                .setSource(source)
                .get();
        IndexResponseInfo(indexResponse);
    }

    @Test
    public void createDocument_Map() {
        Map<String, Object> source = new HashMap<>();
        source.put("id", "6");
        source.put("title", "testmap");
        source.put("content", "test map by myself");

        IndexResponse indexResponse = client.prepareIndex("blog", "article", "6")
                .setSource(source)
                .get();
        IndexResponseInfo(indexResponse);
    }

    @Test
    public void createDocument_XC() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", "7")
                .field("title", "test XC")
                .field("content", "test XC by myself")
                .endObject();
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "8")
                .setSource(source)
                .get();
        IndexResponseInfo(indexResponse);
    }

    /*
     * 搜索文档数据有两种：单个索引和多个索引
     * */
    @Test
    public void getData_Single() {
        GetResponse getResponse = client.prepareGet("blog", "article", "8")
                .get();
        System.out.println(getResponse.getSourceAsString());
    }

    @Test
    public void getData_Multi() {
        MultiGetResponse multiGetResponse = client.prepareMultiGet()
                .add("blog", "article", "1", "2", "3", "4", "6")
                .get();

        for (MultiGetItemResponse response : multiGetResponse) {
            GetResponse getResponse = response.getResponse();
            if (getResponse.isExists()) {
                System.out.println(getResponse.getSourceAsString());
            }
        }
    }

    /*
     * 更新文档数据有三种方法：
     * 1、使用updateRequest对象，在client的update方法之外new UpdateRequest对象
     * 2、使用updateResponse对象，在client的update方法之内new UpdateRequest对象
     * 3、使用upsert方法
     * */
    @Test
    public void update_Request() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest("blog", "article", "8");
        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject()
                .field("id", "8")
                .endObject());

        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println("index: " + updateResponse.getIndex());
        System.out.println("type: " + updateResponse.getType());
        System.out.println("id: " + updateResponse.getId());
        System.out.println("version: " + updateResponse.getVersion());
        System.out.println("isCreated: " + updateResponse.isCreated());
    }

    @Test
    public void update_Response() throws IOException, ExecutionException, InterruptedException {
        UpdateResponse updateResponse = client.update(new UpdateRequest("blog", "article", "8")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "7")
                        .endObject()))
                .get();
        System.out.println("index: " + updateResponse.getIndex());
        System.out.println("type: " + updateResponse.getType());
        System.out.println("id: " + updateResponse.getId());
        System.out.println("version: " + updateResponse.getVersion());
        System.out.println("isCreated: " + updateResponse.isCreated());
    }

    @Test
    public void update_upsert() throws IOException, ExecutionException, InterruptedException {
        IndexRequest source = new IndexRequest("blog", "article", "7")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "7")
                        .field("title", "updated")
                        .field("content","updated upsert")
                        .endObject());

        UpdateRequest upsert = new UpdateRequest("blog", "article", "7")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "7")
                        .field("title", "update")
                        .field("content", "update use upsert")
                        .endObject())
                .upsert(source);

        UpdateResponse updateResponse = client.update(upsert).get();
        System.out.println("index: " + updateResponse.getIndex());
        System.out.println("type: " + updateResponse.getType());
        System.out.println("id: " + updateResponse.getId());
        System.out.println("version: " + updateResponse.getVersion());
        System.out.println("isCreated: " + updateResponse.isCreated());
    }

    @After
    public void close() {
        client.close();
    }

    private void IndexResponseInfo(IndexResponse indexResponse) {
        System.out.println("index: " + indexResponse.getIndex());
        System.out.println("type: " + indexResponse.getType());
        System.out.println("id: " + indexResponse.getId());
        System.out.println("version: " + indexResponse.getVersion());
        System.out.println("isCreated: " + indexResponse.isCreated());
        System.out.println("finish...");
    }
}

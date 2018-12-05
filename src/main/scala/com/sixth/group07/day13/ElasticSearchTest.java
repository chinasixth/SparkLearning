package com.sixth.group07.day13;

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 15:20 2018/8/8
 * @ ES api 练习
 */
public class ElasticSearchTest {
    private Client client;

    // 获取Transport client
    @Before
    public void getClient() throws UnknownHostException {
        // 请求编码qpi的连接的默认端口是9300
        // 如果访问的是ES集群，可以多添加几个节点，这样在一个节点出现网络问题时，自动切换到另一个节点
        // 也就是多几个addTransportAddress
        client = TransportClient
                .builder()
                .build()
                .addTransportAddress(
                        new InetSocketTransportAddress(
                                InetAddress.getByName("localhost"), 9300));

    }

    /*
     * 使用json，来自动创建索引，创建文档，自动创建映射
     * */
    @Test
    public void createDocument_1() {
        // json格式的数据
        String source = "{" +
                "\"id\":\"1\"," +
                "\"title\":\"memeda\"," +
                "\"content\":\"tuoer\"" +
                "}";
        // 指定给哪个index输入数据，最后一个是指定id
        // execute().actionGet() == get()
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "1")
                .setSource(source)
                .get();
        // 获取响应的信息
        this.responseInfo(indexResponse);
    }

    /*
     * 使用map插入一条数据
     * */
    @Test
    public void createDecument_2() {
        Map<String, Object> source = new HashMap<>();
        source.put("id", "2");
        source.put("title", "nihao");
        source.put("content", "langer");

        // 如果不指定id，会自动生成一个id
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "2")
                .setSource(source)
                .get();
        this.responseInfo(indexResponse);
    }

    /*
     * 使用es帮助类插入数据，这是最常用的一种方式
     * */
    @Test
    public void createDocument_3() throws IOException {
        // jsonBuilder就是将接下来的数据转换成字符串
        XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", "3")
                .field("title", "biubiubiu~")
                .field("content", "xuaner")
                .endObject();
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "3")
                .setSource(source)
                .get();
        this.responseInfo(indexResponse);
    }

    /*
     * 搜索文档数据-单个索引
     * */
    @Test
    public void testGetData_1() {
        GetResponse getResponse = client.prepareGet("blog", "article", "1")
                .get();
        System.out.println(getResponse.getSourceAsString());
    }

    /*
     * 搜索文档数据-多个索引
     * */
    @Test
    public void testGetData_2() {
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("blog", "article", "1")
                // 指定多个id
                .add("blog", "article", "2", "3")
                .get();

        // 遍历获取的值
        for (MultiGetItemResponse itemRespons : multiGetItemResponses) {
            GetResponse response = itemRespons.getResponse();
            // 可能查询的条件没有数据
            if (response.isExists()) {
                String sourceAsString = response.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }
    }

    /*
     * 更新文档数据
     * */
    @Test
    public void testUpdate() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest request = new UpdateRequest();
        request.index("blog");
        request.type("article");
        request.id("2");
        request.doc(XContentFactory.jsonBuilder()
                .startObject()
                .field("title", "update")
                .field("content", "update")
                .endObject());
        UpdateResponse updateResponse = client.update(request).get();
        System.out.println("index: " + updateResponse.getIndex());
        System.out.println("type: " + updateResponse.getType());
        System.out.println("id: " + updateResponse.getId());
        System.out.println("version: " + updateResponse.getVersion());
        System.out.println("isCreated: " + updateResponse.isCreated());

    }

    /*
     * 更新文档数据
     * */
    @Test
    public void testUpdate_2() throws IOException, ExecutionException, InterruptedException {
        UpdateResponse updateResponse = client.update(new UpdateRequest("blog", "article", "3")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "3")
                        .field("title", "update")
                        .field("content", "update")
                        .endObject()))
                .get();
        System.out.println("index: " + updateResponse.getIndex());
        System.out.println("type: " + updateResponse.getType());
        System.out.println("id: " + updateResponse.getId());
        System.out.println("version: " + updateResponse.getVersion());
        System.out.println("isCreated: " + updateResponse.isCreated());

    }

    /*
     * 更新文档数据
     * */
    @Test
    public void testUpdate_3() throws IOException, ExecutionException, InterruptedException {
        // 设置一个查询条件，使用id查询，如果查找不到，则添加文档数据，使用较多
        IndexRequest indexRequest = new IndexRequest("blog", "article", "4")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "4")
                        .field("title", "代表索引分片")
                        .field("content", "es可以分片")
                        .endObject());
        // 设置更新的数据，使用id查询，如果找到数据，则更新文档数据
        UpdateRequest updateRequest = new UpdateRequest("blog", "article", "4")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "更新以后的title")
                        .endObject())
                .upsert(indexRequest);

        client.update(updateRequest).get();
    }

    /*
     * 删除文档数据
     * */
    @Test
    public void testDelete() {
        client.prepareDelete("blog", "article", "3").get();
    }

    /*
     * day15
     * 查询
     * */
    @Test
    public void testSearch() {
        // queryStringQuery("检索的词")查询，是针对多字段的查询，也就是从所有的字段中去查询这个词
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                // 如果是指定多个汉字，只能对第一个汉字进行检索，默认的分词器只能分一个汉字
                // 在存储数据的时候就需要指定分词器，要不然即使后来指定分词器，也没有什么用
                .setQuery(
                        QueryBuilders.queryStringQuery("tuoer"))
                .get();
//        // 获取数据的结果集对象，然后获取数据命中次数，也就是找到几条
//        SearchHits hits = searchResponse.getHits();
//        System.out.println("查询的结果数量有" + hits.getTotalHits() + "条");
//        // 遍历数据，将所有的结果打印出来
//        Iterator<SearchHit> it = hits.iterator();
//        while (it.hasNext()) {
//            SearchHit searchHit = it.next();
//            // 获取整条数据
//            System.out.println("所有的数据的json数据格式为： " + searchHit.getSourceAsString());
//            // 获取每个字段的值
//            System.out.println("指定id字段：" + searchHit.getSource().get("id"));
//            System.out.println("指定title字段：" + searchHit.getSource().get("title"));
//            System.out.println("指定content字段：" + searchHit.getSource().get("content"));
//        }
        this.getData(searchResponse);
    }

    // ##############ik分词器之后的操作#######################

    /*
     * 创建索引
     * */
    @Test
    public void testCreateIndex() {
        client.admin()
                .indices()
                .prepareCreate("blog")
                .get();
    }

    /*
     * 删除索引
     * */
    @Test
    public void testDeleteIndex() {
        client.admin()
                .indices()
                .prepareDelete("blog")
                .get();
    }

    /*
     * 创建映射
     * */
    @Test
    public void testCreateIndexMapping() throws IOException, ExecutionException, InterruptedException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("article")
                        .startObject("properties")
                            .startObject("id")
                                .field("type", "integer")
                                // 如果是no，数据将存储到磁盘的一个地方，设置为yes，可以分散存储
                                .field("store", "yes")
                            .endObject()
                            .startObject("title")
                                .field("type", "string")
                                .field("store", "yes")
                                .field("analyzer", "ik")
                            .endObject()
                            .startObject("content")
                                .field("type", "string")
                                .field("store", "yes")
                                .field("analyzer", "ik")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        PutMappingRequest request = Requests
                .putMappingRequest("blog")
                .type("article")
                .source(mappingBuilder);
        client.admin().indices().putMapping(request).get();
    }

    /*
    * ik分词器后的各种查询
    * */
    @Test
    public void testSearchIK(){
        // queryStringQuery方法查询
        // 会将给定的值，按照分词器中有的进行分词，然后匹配分词结果中的值
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.queryStringQuery("tuoer"))
//                .get();

        // 词条查询，给定什么就是什么，不会对给定的词条进行拆分
        // 它仅匹配在给定字段中含有该词的文档，而且是确切的
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.termsQuery("content","tuoer"))
//                .get();
//        getData(searchResponse);

        // 通配符查询 *(任意匹配，即使组成的不是一个词) ?(是一个占位符，代表只匹配一个)
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.wildcardQuery("content","*er"))
//                .get();
//        getData(searchResponse);

        // 模糊查询，如果给定的值和想要的值的偏差不太大，是可以查询到正确的结果的
        // 是基于编辑距离算法来匹配文档
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.fuzzyQuery("content","touer"))
//                .get();
//        getData(searchResponse);

        // 字段匹配查询
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
////                .setQuery(QueryBuilders.matchQuery("content","tu")
////                        .analyzer("ik")
////                        .fuzziness(0.1))
//                .setQuery(QueryBuilders.matchPhrasePrefixQuery("content","tu"))
//                .get();
//        getData(searchResponse);

        // id查询
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                // idsQuery是指定type
//                .setQuery(QueryBuilders.idsQuery().ids("1","2","3"))
//                .get();
//        getData(searchResponse);

        // 范围查询
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
////                .setQuery(QueryBuilders.rangeQuery("id").gte(1).lte(3))
////                .setQuery(QueryBuilders.rangeQuery("id").from(1).to(3))
//                .setQuery(QueryBuilders.rangeQuery("content").from("").to("tuoer"))
//                .get();
//        getData(searchResponse);

        // bool查询
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.boolQuery()
                .must(QueryBuilders.termsQuery("content","tuoer"))
                .should(QueryBuilders.rangeQuery("id").from(1).to(3)))
                .get();
        getData(searchResponse);
    }




    /*
    * 模拟多条数据，用于分页操作
    * */
    @Test
    public void createDocuments() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        for (int i = 1; i <= 100; i++) {
            Article article = new Article();
            article.setId(i);
            article.setTitle("title:" + i);
            article.setContent("content:" + i);

            String source = objectMapper.writeValueAsString(article);

            client.prepareIndex("blog", "article", String.valueOf(article.getId()))
                    .setSource(source)
                    .get();
        }
    }

    /*
    * 分页查询
    * */
    @Test
    public void testPage(){
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("blog")
                .setTypes("article")
                // 查询所有
                .setQuery(QueryBuilders.matchAllQuery());
        // 从第一条开始检索，第一页显示20条数据
        searchRequestBuilder.setFrom(0).setSize(20);
        searchRequestBuilder.addSort("id", SortOrder.DESC);

        SearchResponse searchResponse = searchRequestBuilder.get();
        this.getData(searchResponse);
    }

    /*
    * 加权
    * */
    @Test
    public void testBoolQuery(){
//        SearchResponse searchResponse = client.prepareSearch("blog")
//                .setTypes("article")
//                .setQuery(QueryBuilders.boolQuery()
//                        .should(QueryBuilders.termsQuery("content", "tuoer").boost(7f))
//                        .should(QueryBuilders.termsQuery("content", "xuaner").boost(6f)))
//                .get();
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("tuoer")
                        // 在指定字段的时候指定权重值
                        .field("content^10")
                        .field("title^5"))
                .get();

        getData(searchResponse);
    }











    /*
    * 获取响应的数据
    * */
    private void getData(SearchResponse searchResponse){
        // 获取数据的结果集对象，然后获取数据命中次数，也就是找到几条
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询的结果数量有" + hits.getTotalHits() + "条");
        // 遍历数据，将所有的结果打印出来
        Iterator<SearchHit> it = hits.iterator();
        while (it.hasNext()) {
            SearchHit searchHit = it.next();
            // 获取整条数据
            System.out.println("所有的数据的json数据格式为： " + searchHit.getSourceAsString());
            // 获取每个字段的值
            System.out.println("指定id字段：" + searchHit.getSource().get("id"));
            System.out.println("指定title字段：" + searchHit.getSource().get("title"));
            System.out.println("指定content字段：" + searchHit.getSource().get("content"));
        }
    }


    @After
    public void close() {
        client.close();
        System.out.println("finish...");
    }

    private void responseInfo(IndexResponse indexResponse) {
        // 获取响应的信息
        System.out.println("index: " + indexResponse.getIndex());
        System.out.println("type: " + indexResponse.getType());
        System.out.println("id: " + indexResponse.getId());
        System.out.println("version: " + indexResponse.getVersion());
        System.out.println("isCreated: " + indexResponse.isCreated());
    }
}







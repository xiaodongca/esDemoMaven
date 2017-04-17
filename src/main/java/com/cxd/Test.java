package com.cxd;


import com.cxd.util.Connect;
import com.cxd.util.DataFactory;
import com.cxd.util.JsonUtil;
import net.sf.json.JSONObject;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
/**
 * Created by cai x d
 * on2017/4/14 0014.
 */
public class Test {

    private static Client client = Connect.getConnect();

    public static void main(String[] args) {
        Test.createMap();

    }

    /**
     * Bolg创建索引和数据(List方式)
     */
    public static void createData() {
        try {
            List<String> jsonData = DataFactory.getInitJsonData();
            for (int i = 0; i < jsonData.size(); i++) {
                IndexResponse response = client.prepareIndex("blog", "article1").setSource(jsonData.get(i)).get();
                if (response.isCreated()) {
                    System.out.println("创建成功!");
                }
            }
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建索引插入数据
     */
    public static void createIndex(){
        try{
            IndexResponse response = client.prepareIndex("prelesson","teachPlan","2").setSource(
                    jsonBuilder().startObject()
                            .field("planName","高一教学计划")
                            .field("schoolId","1")
                            .field("sujectName","数学").endObject()
            ).get();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     *  Map转成json插入
     */
    public static void createMap(){
        Map<Object,String> map = new HashMap<Object,String>();
        map.put("user","cxd");
        map.put("age","18");
        map.put("password","123456");
        map.put("id","1");
        JSONObject jsonObject = JsonUtil.mapJson(map);
        IndexResponse response = client.prepareIndex("user","map").setSource(jsonObject).execute().actionGet();
    }

    /**
     * Bolg插入数据
     */
    public static void createJson(String index, String type) {

        try {
            String json = "{" +
                    "\"user\":\"kimchy\"," +
                    "\"postDate\":\"2013-01-30\"," +
                    "\"message\":\"trying out Elasticsearch\"" +
                    "}";
            IndexResponse response = client.prepareIndex(index.toLowerCase(), type).setSource(json).get();
            if (response.isCreated()) {
                System.out.println("插入成功！！！");
            } else {
                System.out.println("插入失败！！！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个索引
     *
     * @param indexName 索引名
     */
    public static void createIndex(String indexName) {
        try {
            //判断这个索引是否存在
            if (isIndexExists("indexName")) {
                System.out.println("Index  " + indexName + " already exits!");
            } else {
                CreateIndexRequest cIndexRequest = new CreateIndexRequest(indexName.toLowerCase());
                CreateIndexResponse cIndexResponse = client.admin().indices().create(cIndexRequest)
                        .actionGet();
                if (cIndexResponse.isAcknowledged()) {
                    System.out.println("create index successfully！");
                } else {
                    System.out.println("Fail to create index!");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断索引是否存在
     */
    public static boolean isIndexExists(String indexName) {
        boolean flag = false;
        try {
            IndicesExistsRequest indicesExistsRequest = new IndicesExistsRequest(indexName);
            IndicesExistsResponse indicesExistsResponse = client.admin().indices().exists(indicesExistsRequest).actionGet();
            if (indicesExistsResponse.isExists()) {
                flag = true;
            } else {
                flag = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return flag;
    }

    /**
     * 删除索引库
     */
    public static void deleteIndex(String indexName) {
        if (isIndexExists(indexName)) {
            DeleteIndexResponse response = client.admin().indices().prepareDelete(indexName.toLowerCase()).execute().actionGet();
            if (response.isAcknowledged()) {
                System.out.println("删除成功");
            } else {
                System.out.println("出了故障");
            }
        } else {
            System.out.println("删除的索引不存在");
        }
    }

    /**
     * 删除一条数据
     */
    public static void deleteDoc(){
        DeleteResponse response = client.prepareDelete("bolg","article","1").execute().actionGet();
    }

    /**
     * 查询
     */
    public static void query() {

        try {
            //查询title字段或content字段中包含Git关键字的文档:
            QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery("git", "title", "content");
            SearchResponse response = client.prepareSearch("blog").setTypes("article").setQuery(queryBuilder).execute().actionGet();
            SearchHits hits = response.getHits();
            if (hits.totalHits() > 0) {
                for (SearchHit hit : hits) {
                    System.out.println("score:" + hit.getScore() + ":\t" + hit.getSource());
                }
            } else {
                System.out.println("搜到0条结果");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询整张表
     */
    public static  void queryType(){
        try{
            SearchResponse response = client.prepareSearch("bolg").setTypes("article").execute().actionGet();
            SearchHits hits = response.getHits();
            if (hits.totalHits() > 0) {
                for (SearchHit hit : hits) {
                    System.out.println("score:" + hit.getScore() + ":\t" + hit.getSource());
                }
            } else {
                System.out.println("搜到0条结果");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 更新一：创建一个updateRequest,然后将其发送给client
     * 已经有的情况下可以更新
     */
    public static void upMethod1() {
        try {
            UpdateRequest request = new UpdateRequest();
            request.index("bolg");
            request.type("article");
            request.id("2");
            request.doc(jsonBuilder().startObject().field("content", "学习目标 掌握java泛型的产生意义ssss").endObject());
            client.update(request).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * prepareUpdate() 使用脚本更新索引
     */
    public static void upMethod2() {
        client.prepareUpdate("bolg", "article", "1").
                setScript(new Script("ctx._source.title = \\\"git入门\\\"\"",
                        ScriptService.ScriptType.INLINE, null, null)).get();


    }

    /**
     * 增加新的字段
     */
    public  static void upMethod4(){
        try{
            UpdateRequest request = new UpdateRequest("bolg","article","1")
                    .doc(jsonBuilder().startObject().field("comment","0").endObject());
            client.update(request).get();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 如果文档不存在就创建新的索引
     */
    public static void upMethod5() {
        // 方法五：upsert 如果文档不存在则创建新的索引
        try {
            IndexRequest indexRequest = new IndexRequest("blog", "article", "10").source(jsonBuilder().startObject()
                    .field("title", "Git安装10").field("content", "学习目标 git。。。10").endObject());

            UpdateRequest uRequest2 = new UpdateRequest("blog", "article", "10").doc(
                    jsonBuilder().startObject().field("title", "Git安装").field("content", "学习目标 git。。。").endObject())
                    .upsert(indexRequest);
            client.update(uRequest2).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

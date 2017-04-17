package com.cxd.util;

import com.cxd.entity.Blog;
import net.sf.json.JSONObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.Map;

/**
 * Created by cai x d
 * on2017/4/14 0014.
 */
public class JsonUtil {
    //java实体对象转json对象
    public  static String model2Json(Blog blog){
        String jsonData = null;
        try {
            XContentBuilder jsonBuild = XContentFactory.jsonBuilder();
            jsonBuild.startObject().field("id", blog.getId()).field("title", blog.getTitle())
                    .field("posttime", blog.getPosttime()).field("content",blog.getContent()).endObject();
            jsonData = jsonBuild.string();
            System.out.println(jsonData);

        }catch (Exception e){
            e.printStackTrace();
        }
        return jsonData;
    }
    //将Map转化成json对象
    public static JSONObject mapJson(Map map){
        JSONObject jsonObject = JSONObject.fromObject(map);
        return  jsonObject;
    }
}

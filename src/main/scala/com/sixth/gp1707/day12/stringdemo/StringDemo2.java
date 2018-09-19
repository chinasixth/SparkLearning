package com.sixth.gp1707.day12.stringdemo;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;

import java.io.*;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 20:00 2018/8/7
 * @
 */
public class StringDemo2 {
    // 如果是使用的默认端口可以不用指定
    static Jedis jedis = new Jedis("hadoop05");

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        jedis.del("user");
//        StringTest();
        objectTest();
//        objectToJsonTest();
        jedis.close();
    }

    /*
     * 将字符串缓存到String数据结构中
     * */
    public static void StringTest() {
        jedis.set("user:001:name", "tuoer");
        jedis.mset("user:002:name", "langer", "user:003:name", "xuaner");

        String username001 = jedis.get("user:001:name");
        String username002 = jedis.get("user:002:name");
        String username003 = jedis.get("user:003:name");

        System.out.println(username001);
        System.out.println(username002);
        System.out.println(username003);
    }

    /*
     * 将对象缓存到String数据结构中
     * */
    public static void objectTest() throws IOException, ClassNotFoundException {
        ProduceInfo p = new ProduceInfo();
        p.setName("IphoneX");
        p.setPrice(8999.0);
        p.setProductDesc("死贵死贵的");

        // 将对象序列化
        ByteArrayOutputStream ba = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(ba);
        // 用对象序列化的方式将ProduceInfo序列化写入流中
        // 这里出现了一个错误是NotSerialize的错误，原因是 。。。
        oos.writeObject(p);
        // 将ba流转换为字节数组
        byte[] pBytes = ba.toByteArray();

        // 将序列化好的数据缓存到redis中
        jedis.set("produce:001".getBytes(), pBytes);

        // 读取刚刚缓存的数据
        byte[] pBytesRes = jedis.get("produce:001".getBytes());
        // 反序列化
        ByteArrayInputStream bi = new ByteArrayInputStream(pBytesRes);
        ObjectInputStream ois = new ObjectInputStream(bi);
        ProduceInfo pRes = (ProduceInfo) ois.readObject();

        System.out.println(pRes);
    }

    /*
     * 将对象转换为json字符串缓存到redis
     * */
    public static void objectToJsonTest() {
        ProduceInfo p = new ProduceInfo();
        p.setName("Iphone4");
        p.setPrice(480.0);
        p.setProductDesc("砸核桃");

        // 将对象转换为json格式
        Gson gson = new Gson();
        String jsonProduceInfo = gson.toJson(p);

        // 缓存到redis
        jedis.set("product:002", jsonProduceInfo);

        // 获取数据
        String jsonRes = jedis.get("product:002");

        // 将json字符串转换为对象
        ProduceInfo produceInfo = gson.fromJson(jsonRes, ProduceInfo.class);

        System.out.println(jsonRes);
        System.out.println(produceInfo);
    }
}

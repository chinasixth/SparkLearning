package com.sixth.gp1707.day12.hashdemo;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 21:12 2018/8/7
 * @
 */
public class BuyCart {
    private static Jedis jedis = new Jedis("hadoop05");

    // 添加商品
    public static void main(String[] args) {
        jedis.hset("cart:user001", "T恤", "2");
        jedis.hset("cart:user002", "手机", "5");
        jedis.hset("cart:user003", "电脑", "1");

        getProduceInfo();

        jedis.close();
    }

    // 查询购物车信息
    public static void getProduceInfo(){
        String pForUser001 = jedis.hget("cart:user001", "T恤");
        System.out.println(pForUser001);

        List<String> pForUser002 = jedis.hmget("cart:user002", "T恤", "手机", "电脑");
        System.out.println(pForUser002);
    }

}

package com.sixth.gp1707.day12.stringdemo;

import redis.clients.jedis.Jedis;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 19:22 2018/8/7
 * @
 */
public class StringDemo1 {
    public static void main(String[] args) {
        // 创建一个Jdeis连接
        Jedis jedis = new Jedis("hadoop05", 6379);

        String pingAck = jedis.ping();
        System.out.println(pingAck);

        String setAck = jedis.set("s1", "111");
        System.out.println(setAck);

        String getAck = jedis.get("s1");
        System.out.println(getAck);

        Long append = jedis.append("s1", "222");
        System.out.println(append);

        String getAck2 = jedis.get("s1");
        System.out.println(getAck2);

        // 这相当于重新设置，原来的数据会清空
        String setAck1 = jedis.set("s1", "333");
        System.out.println(setAck1);

        String getAck1 = jedis.get("s1");
        System.out.println(getAck1);

        // 关闭
        jedis.close();
    }
}

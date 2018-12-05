package com.sixth.group07.day12.stringdemo;

import java.io.Serializable;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 21:05 2018/8/7
 * @
 */

public class ProduceInfo implements Serializable {
    private String name;
    private Double price;
    private String ProductDesc;

    public ProduceInfo() {
    }

    public ProduceInfo(String name, Double price, String productDesc) {
        this.name = name;
        this.price = price;
        ProductDesc = productDesc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getProductDesc() {
        return ProductDesc;
    }

    public void setProductDesc(String productDesc) {
        ProductDesc = productDesc;
    }

    @Override
    public String toString() {
        return "ProduceInfo{" +
                "name='" + name + '\'' +
                ", price=" + price +
                ", ProductDesc='" + ProductDesc + '\'' +
                '}';
    }

}
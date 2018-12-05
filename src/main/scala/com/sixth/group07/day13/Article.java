package com.sixth.group07.day13;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 14:44 2018/8/9
 * @
 */
public class Article {
    private int id;
    private String title;
    private String content;

    public Article() {
    }

    public Article(int id, String title, String content) {
        this.id = id;
        this.title = title;
        this.content = content;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}

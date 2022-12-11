package com.learning.flink.usecase;

public class User {

    private long timestamp;
    private String name;
    private String action;

    public User(String s) {
        String[] userArr = s.replace("\"", "").split(",");
        this.timestamp = Long.parseLong(userArr[0]);
        this.name = userArr[1];
        this.action = userArr[2];
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}

package com.movierecommender.model;

import java.io.Serializable;

public class User implements Serializable {

    private static final long serialVersionUID = 1L;

    private Integer userId;

    private String gender;

    private Integer age;

    private Integer occupation;

    private String zip;

    public User(Integer userId, String gender, Integer age, Integer occupation, String zip) {
        super();
        this.userId = userId;
        this.gender = gender;
        this.age = age;
        this.occupation = occupation;
        this.zip = zip;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getOccupation() {
        return occupation;
    }

    public void setOccupation(Integer occupation) {
        this.occupation = occupation;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    @Override
    public String toString() {
        return "User [userId=" + userId + ", gender=" + gender + ", age=" + age + ", occupation=" + occupation
                + ", zip=" + zip + "]";
    }
}



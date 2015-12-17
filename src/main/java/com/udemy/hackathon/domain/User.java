package com.udemy.hackathon.domain;

public class User {

    private int userId;

    private String gender;

    private int age;

    private int occupation;

    private int zipCode;

    public User(int userId, String gender, int age, int occupation, int zipCode) {
        this.userId = userId;
        this.gender = gender;
        this.age = age;
        this.occupation = occupation;
        this.zipCode = zipCode;
    }

    public int getUserId() {
        return userId;
    }

    public String getGender() {
        return gender;
    }

    public int getAge() {
        return age;
    }

    public int getOccupation() {
        return occupation;
    }

    public int getZipCode() {
        return zipCode;
    }
}

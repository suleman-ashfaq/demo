package com.example.demo;

import java.util.ArrayList;
import java.util.List;

public final class VolatileStorage {

    private VolatileStorage() {};
    private static List<Student> studentList = new ArrayList<>();

    public static List<Student> getStudentList() {
        return studentList;
    }

    public static void addStudent(Student student) {
        studentList.add(student);
    }

}

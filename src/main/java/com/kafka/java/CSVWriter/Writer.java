package com.kafka.java.CSVWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;


public class Writer {

    public boolean writeCSV(String message){
        try (PrintWriter writer = new PrintWriter(new File("test.csv"))) {

            StringBuilder sb = new StringBuilder();
            String[] list = message.split(",");

            for (String a : list){
                sb.append(a+",");
            }

            //System.out.println("done!");
            return true;
        } catch (FileNotFoundException e) {
            System.out.println(e.getMessage());
        }
        return false;
    }

}

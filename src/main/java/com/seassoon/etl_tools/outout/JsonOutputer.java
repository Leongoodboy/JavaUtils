package com.seassoon.etl_tools.outout;

import java.io.*;
import java.util.Queue;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONType;

public class JsonOutputer extends FileOutPuter{


    //protected FileWriter fileWriter;
    protected PrintWriter printWriter;

    public JsonOutputer(String path, String[] header) throws IOException {
        super(path, header);
    }

    public JsonOutputer(OutputStream outputStream, String[] header) {
        super(outputStream, header);
    }

    @Override
    public void initialize() throws Exception {
        printWriter = new PrintWriter(new FileWriter(path));
        //printWriter.write("{\"RECORDS\":[");
    }

    @Override
    public void process(String[] data) throws IOException, Exception {
        if (printWriter == null) {
            this.initialize();
        }
        if(data==null){
            return;
        }

        printWriter.write(convertDataToJson(data,header)+"\n");
        printWriter.flush();
    }

    @Override
    public void close() {
        try {
            if(printWriter != null){
                //printWriter.write("]}");
                this.printWriter.close();
            }
            super.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String convertDataToJson(String[] data, String[] header){
        JSONObject jsonObject = new JSONObject(true);
        for(int i=0;i<header.length;i++) {
            jsonObject.put(header[i],data[i]);
        }
        return jsonObject.toString();
    }

    public void processAll(Queue<String[]> dataQueue){
        if(dataQueue==null) return;

        JSONArray jsonArray = new JSONArray();
        while(dataQueue.size()>0) {
            String[] data = dataQueue.poll();
            JSONObject jsonObject = new JSONObject();
            for (int i = 0; i < header.length; i++) {
                jsonObject.put(header[i], data[i]);
            }
            jsonArray.add(jsonObject);
        }
        printWriter.write(jsonArray.toJSONString());


//        //printWriter.write("[");
//        printWriter.write("{\"RECORDS\":[");
//        while(dataQueue!=null){
//            String[] data = dataQueue.poll();
//            String dataJsonText = convertDataToJson(data,header);
//            printWriter.write(",");
//            printWriter.write(dataJsonText);
//            //printWriter.flush();
//        }
//        printWriter.write("]}");
        printWriter.flush();
        System.out.println("输出Json文件完成，路径："+path);
    }


    //测试
//    public static void main(String[] args){
//        String[] data = {"22","332","11","333","221"};
//        String[] header = {"11","222","11","122","232"};
//        System.out.println(convertDataToJson(data,header));
//    }
}

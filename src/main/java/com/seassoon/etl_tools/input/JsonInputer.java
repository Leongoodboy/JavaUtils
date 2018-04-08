package com.seassoon.etl_tools.input;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.Feature;
import com.seassoon.etl_tools.outout.JsonOutputer;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class JsonInputer extends AbstractInputer {

    private static Logger logger = Logger.getLogger(JsonInputer.class); //日志

    //private String[] header; //表头
    private String type; //json类型，开头为中括号还是大括号
    private String nodeName = "records"; //数据节点名称，默认为records
    //private String[] data; //每一个数据
    protected JSONArray dataJsonArray; //数据在json中的列表
    protected ConcurrentLinkedQueue<String[]> dataQueue = new ConcurrentLinkedQueue<>(); //json中的数据存放入队列

    protected InputStream inputStream; //输入流
    protected String path; //文件路径


//    public JsonInputer() {
//        super();
//    }

    public ConcurrentLinkedQueue<String[]> getDataQueue() {
        return dataQueue;
    }

    /**
     * 参数为输入流的构造方法
     *
     * @param inputStream
     */
    public JsonInputer(InputStream inputStream) {
        super();
        this.inputStream = inputStream;
    }

    /**
     * 参数为文件路径的构造方法
     *
     * @param path
     * @throws FileNotFoundException
     */
    public JsonInputer(String path) throws FileNotFoundException {
        super();
        this.inputStream = new FileInputStream(new File(path));
    }

    public JsonInputer(String path, String nodeName) {
        this.nodeName = nodeName;
        this.path = path;
    }

    public JsonInputer(InputStream inputStream, String nodeName) {
        this.nodeName = nodeName;
        this.inputStream = inputStream;
    }

    @Override
    public boolean read() throws Exception {
        if (header != null && dataJsonArray != null) {
            addAllResults(dataJsonArray, header);
        }

        if (dataQueue.isEmpty()) return false;
        else return true;
    }

    @Override
    public String[] getHeader() throws Exception {
        // 获取表头
        if (header == null) {
            if (dataJsonArray == null) {
                resolveJson(readJsonFileToText(this.inputStream));
            }

            //List<String> tempHeader = new ArrayList<>();
            for (int i = 0; i < dataJsonArray.size(); i++) {
                JSONObject jsonData = dataJsonArray.getJSONObject(i);
                Iterator it = jsonData.keySet().iterator();
                //检查该key在header是否已经存在，如果不存在则加入其中
                while (it.hasNext()) {
                    String column = (String) it.next();
                    if (!columns.contains(column)) {
                        columns.add(column);
                    }
                }
            }
            header = columns.toArray(new String[columns.size()]);

        }


        return header;
    }

    @Override
    public String[] next() throws Exception {
        /////////////

        if (dataQueue != null) {
            if (dataQueue.size() > 0) {
                rowNumber++;
                return dataQueue.poll();
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }


    @Override
    public void initialize() throws Exception {
        this.getHeader();
        read();

    }

    public String readJsonFileToText(String filepath) {
        String text = "";
        try {
            File file = new File(filepath);
            InputStream fis = new FileInputStream(file);
            InputStreamReader isr = new InputStreamReader(fis);
            BufferedReader br = new BufferedReader(isr);
            String temp = br.readLine();
            while (temp != null) {
                text += temp;
                temp = br.readLine();
            }
            br.close();
            isr.close();
            fis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return text;
    }

    public String readJsonFileToText(InputStream inputStream) {
        String text = "";
        try {
            InputStreamReader isr = new InputStreamReader(inputStream);
            BufferedReader br = new BufferedReader(isr);
            String temp = br.readLine();
            while (temp != null) {
                text += temp;
                temp = br.readLine();
            }
            br.close();
            isr.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return text;
    }

    /**
     * 根据json文件中的文本内容，找到对应数据所在的jsonArray并存入对象中
     *
     * @param jsonText
     */
    public void resolveJson(String jsonText) {
        type = jsonText.substring(0, 1);
        //如果json最外层为数组
        if (type.equals("[")) {
            setDataJsonArray(JSONArray.parseArray(jsonText));
        }
        //如果json最外层为对象
        else if (type.equals("{")) {
            //通过feature增加ordered配置
            JSONObject jsonObject = JSONObject.parseObject(jsonText, Feature.OrderedField);

            for (String i : jsonObject.keySet()) {
                if (i.toLowerCase().equals(nodeName)) {
                    JSONArray dataJsonArray = jsonObject.getJSONArray(i);
                    setDataJsonArray(dataJsonArray);
                    break;
                }//如果节点名称为results，解析其对应的数据列表部分
            }
        }
    }

    public void setDataJsonArray(JSONArray dataJsonArray) {
        this.dataJsonArray = dataJsonArray;
    }

    /**
     * 根据表头和放有数据的JsonArray把数据添加到队列中
     *
     * @param dataJsonArray
     * @param header
     */
    protected void addAllResults(JSONArray dataJsonArray, String[] header) {
        for (int i = 0; i < dataJsonArray.size(); i++) {
            JSONObject dataJson = dataJsonArray.getJSONObject(i);
            //按表头的顺序取出数据
            String[] tempData = new String[header.length];
            for (int j = 0; j < header.length; j++) {
                if (dataJson.containsKey(header[j])) {
                    tempData[j] = dataJson.getString(header[j]);
                } else {
                    tempData[j] = null;
                }
            }
            dataQueue.add(tempData);
            recordNumer++;
        }
    }
}

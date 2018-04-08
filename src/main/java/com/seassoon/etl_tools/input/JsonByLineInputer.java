package com.seassoon.etl_tools.input;

import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

public class JsonByLineInputer extends AbstractInputer {


    protected ConcurrentLinkedQueue<String[]> dataQueue = new ConcurrentLinkedQueue<>(); //json中的数据存放入队列
    private String[] data; //每一个数据

    protected String path; //文件路径
    protected InputStream inputStream; //输入流

    public JsonByLineInputer(String path) throws FileNotFoundException {
        super();
        this.path = path;
        this.inputStream = new FileInputStream(new File(path));
    }

    public JsonByLineInputer(InputStream inputStream) {
        super();
        this.inputStream = inputStream;
    }

    @Override
    public boolean read() throws Exception {

        try {
            inputStream = new FileInputStream(new File(path));
            InputStreamReader isr = new InputStreamReader(inputStream);
            BufferedReader br = new BufferedReader(isr);
            //逐行读取数据
            String line;
            while ((line = br.readLine()) != null) {


                //解析该行数据，解析结果放入当前数据，并且加入队列中
                JSONObject dataJson = JSONObject.parseObject(line);

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
            br.close();
            isr.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public String[] getHeader() throws Exception {

        return this.header;
    }

    /**
     * 对外接口，提供获取下一行数据的方法（从队列中提取）
     *
     * @return
     * @throws Exception
     */
    @Override
    public String[] next() throws Exception {

        if (dataQueue != null) {
            if (dataQueue.size() > 0) {
                rowNumber++;
                return dataQueue.poll();
            }
        }
        return null;

    }


    /**
     * 完成所有数据的逐行读入。数据存入队列中
     *
     * @throws Exception
     */
    @Override
    public void initialize() throws Exception {


        /* 遍历所有数据获取header */
        try {
            InputStreamReader isr = new InputStreamReader(inputStream);
            BufferedReader br = new BufferedReader(isr);
            //逐行读取数据
            String line;
            while ((line = br.readLine()) != null) {


                JSONObject data = JSONObject.parseObject(line);

                //获取这条数据的字段到colomns当中
                Iterator it = data.keySet().iterator();
                while (it.hasNext()) {
                    String column = (String) it.next();
                    //columns中是否存在
                    if (!columns.contains(column)) {
                        columns.add(column);
                    }
                }

            }
            header = columns.toArray(new String[columns.size()]);

//            br.close();
//            isr.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        read();//获得header后读取对应数据


    }

    /**
     * 关闭
     *
     * @return
     */
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
}

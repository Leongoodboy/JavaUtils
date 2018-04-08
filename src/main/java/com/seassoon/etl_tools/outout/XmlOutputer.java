package com.seassoon.etl_tools.outout;

import org.dom4j.*;
import org.dom4j.io.XMLWriter;
import org.dom4j.io.OutputFormat;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class XmlOutputer extends FileOutPuter {


    String path; //文件输出路径
    String encoding = "UTF-8"; //文件编码格式

    XMLWriter writer;
    //FileWriter fileWriter;

    public XmlOutputer(String path, String[] header) throws IOException {
        super(path, header);
    }

    public XmlOutputer(String path, String[] header, String encoding) throws IOException {
        super(path, header);
        this.encoding = encoding;
    }

    public XmlOutputer(OutputStream outputStream, String[] header, String encoding) {
        super(outputStream, header);
        this.encoding = encoding;
    }

    @Override
    public void process(String[] data) throws IOException, Exception {
        Queue<String[]> dataQueue = new ConcurrentLinkedQueue<>();
        processAll(dataQueue);
    }

    @Override
    public void initialize() throws Exception {

    }

    //可以在close的时候加上根节点


    /**
     * 输出全部数据到XML文件
     *
     * @param dataQueue
     */
    public void processAll(Queue<String[]> dataQueue) {
        if (dataQueue == null) return;

        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding(encoding);// 设置XML文件的编码格式

        Document document = DocumentHelper.createDocument();
        Element root = document.addElement("RECORDS");//增加根节点RECORDS

        while (!dataQueue.isEmpty()) {
            String[] data = dataQueue.poll();
            Element record = root.addElement("RECORD");
            int size = header.length;
            for (int i = 0; i < size; i++) {
                Element temp = record.addElement(header[i]);
                temp.setText(data[i]);
            }
        }

        try {
            writer = new XMLWriter(outputStream, format);
            writer.write(document);
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block

        }
    }

}

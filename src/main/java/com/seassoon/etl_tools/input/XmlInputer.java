package com.seassoon.etl_tools.input;

import com.sun.xml.internal.bind.v2.runtime.output.NamespaceContextImpl;
import org.dom4j.*;
import org.dom4j.io.SAXReader;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class XmlInputer extends AbstractInputer {

    Document document;
    String rowNodeName = "RECORD";//表示一行的标签名，字段为该标签下的属性，默认值为RECORD
    String xmlFilepath;//XML文件路径

    private ConcurrentLinkedQueue<String[]> headerQueue = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<Map<String, String>> dataMapQueue = new ConcurrentLinkedQueue<>();

    protected ConcurrentLinkedQueue<String[]> dataQueue = new ConcurrentLinkedQueue<>();


    /**
     * 单参数构造方法，指定xml文件路径
     *
     * @param xmlFilepath
     */
    public XmlInputer(String xmlFilepath) {
        this.xmlFilepath = xmlFilepath;
        this.document = readFileToDocument(xmlFilepath);
        //this.xmlText=readFileToText(xmlFilepath);
    }

    /**
     * 两参数构造方法，指定xml文件路径和节点名称
     *
     * @param xmlFilepath
     * @param rowNodeName
     */
    public XmlInputer(String xmlFilepath, String rowNodeName) {
        this.xmlFilepath = xmlFilepath;
        this.rowNodeName = rowNodeName;
        this.document = readFileToDocument(xmlFilepath);
    }

    public void setRowNodeName(String rowNodeName) {
        this.rowNodeName = rowNodeName.toUpperCase();
    }

    @Override
    public boolean read() throws Exception {

        getHeader();//放置好header

        //由dataMapQueue出发放置好dataQueue
        while (!dataMapQueue.isEmpty()) {
            Map<String, String> temp = dataMapQueue.poll();
            int size = header.length;
            String[] data = new String[size];
            for (int i = 0; i < size; i++) {
                if (temp.containsKey(header[i])) {
                    data[i] = temp.get(header[i]);
                } else {
                    data[i] = null;
                }
            }
            dataQueue.add(data);
        }

        if (header == null || dataQueue.isEmpty()) return false;
        return true;
    }

    /**
     * 获取头
     *
     * @return
     * @throws Exception
     */
    @Override
    public String[] getHeader() throws Exception {
        if (header == null) {

            //List<String> names = new ArrayList<>();
            while (!headerQueue.isEmpty()) {
                String[] temp = headerQueue.poll();
                for (int i = 0; i < temp.length; i++) {
                    String name = temp[i];
                    if (!columns.contains(name)) {
                        columns.add(name);
                    }
                }
            }
            header = columns.toArray(new String[columns.size()]);
        }

        return header;
    }


    public int getRowNumber() throws Exception {
        return this.rowNumber;
    }

    /**
     * 下一行
     *
     * @return
     */
    @Override
    public String[] next() throws Exception {
//        if (dataMapQueue.isEmpty())
//            return null;
//
//        if (header == null) return null;
//        Map<String, String> nextDataMap = dataMapQueue.poll();
//        int size = header.length;
//        String[] data = new String[size];
//        for (int i = 0; i < size; i++) {
//            if (nextDataMap.containsKey(header[i])) {
//                data[i] = nextDataMap.get(header[i]);
//            } else {
//                data[i] = null;
//            }
//        }
//        return data;


        if (!dataQueue.isEmpty()) {
            rowNumber++;
            return dataQueue.poll();
        }

        return null;

    }

    /**
     * 关闭
     *
     * @return
     */
    @Override
    public void close() throws Exception {


    }

    @Override
    public void initialize() throws Exception {

        Element root = document.getRootElement();

        //在根节点的子节点中找行标签
        Iterator<Element> it = root.elementIterator(rowNodeName);

        //倘若未找到，则在后续子节点中找行标签
        if(!it.hasNext()){
            List<Element> nodeList = new ArrayList<>();
            addCertainNode(nodeList,root,rowNodeName);
            it = nodeList.iterator();
        }

        while(it.hasNext()) {
            Element rowElement = it.next();
            recordNumer++;

            String rowName = rowElement.getName();

            int count = 0;

            //int size = rowElement.nodeCount();//第一级子节点个数

            List<String> currentHeader = new ArrayList<>();
            Map<String, String> currentData = new LinkedHashMap<>();

            //在子节点中寻找数据
            for (Iterator<Element> sub_it = rowElement.elementIterator(); sub_it.hasNext(); count++) {
                Element element = sub_it.next();
                String name = element.getName();
                String text = element.getText();
                currentHeader.add(name);
                currentData.put(name, text);
            }

            //如果不存在则自己作为数据
            if(currentHeader.size()==0){
                String text = rowElement.getTextTrim();
                currentHeader.add(rowName);
                currentData.put(rowName,text);
            }

            headerQueue.add(currentHeader.toArray(new String[currentHeader.size()]));
            dataMapQueue.add(currentData);
        }



        //read();
    }


    public Document readFileToDocument(String filepath) {

        try {
            SAXReader saxReader = new SAXReader();
            this.document = saxReader.read(xmlFilepath);

        } catch (DocumentException de) {
            de.printStackTrace();
        }
        return document;
    }


//    public Element findFirstNode (Element node, String rowNodeName){
//        //当前节点的名称、文本内容和属性
//        if(node.getName().equals(rowNodeName)){
//            return node;
//        }
//        //node.getTextTrim()
//
//        List<Attribute> listAttr=node.attributes();//当前节点的所有属性的list
//
//
//        //递归遍历当前节点所有的子节点
//        List<Element> elementList =node.elements();//所有一级子节点的list
//        for(Element e:elementList){//遍历所有一级子节点
//            return this.findFirstNode(e,rowNodeName);//递归
//        }
//
//        return null;
//    }

    public List<Element> addCertainNode (List<Element> nodeList, Element node, String rowNodeName){
        //////////
        //List<Element> nodeList = new ArrayList<>();

        if(node.getName().equals(rowNodeName)){
            nodeList.add(node);
        }

        List<Element> elementList = node.elements();
        for(Element e:elementList){
            addCertainNode(nodeList,e,rowNodeName);
        }

        return nodeList;
    }

}

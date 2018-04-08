package etl_tools;

import com.seassoon.etl_tools.input.CsvDistributedInputer;
import com.seassoon.etl_tools.input.JsonInputer;
import com.seassoon.etl_tools.input.XmlInputer;
import com.seassoon.etl_tools.outout.JsonOutputer;
import com.seassoon.etl_tools.outout.XmlOutputer;
import org.dom4j.*;
import org.dom4j.io.SAXReader;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


public class XMLTest {


    static String path = "/Users/chimney/Coding Practice/zc_au_0811.xml";
    static String path2 = "/Users/chimney/Coding Practice/zc_au_0811_copy.xml";
    static String rowNodeName = "RECORD";
    static Integer rowNum = 0;

    static String inputPath = "/Users/chimney/Coding Practice/zc_au_0811.json";
    static String outputPath = "/Users/chimney/Coding Practice/test.xml";
    static String outputPath2 = "/Users/chimney/Coding Practice/test2.xml";
    static String jsonoutput = "/Users/chimney/Coding Practice/test.json";

    static String path3 = "/Users/chimney/Coding Practice/websites.xml";

    public static void main(String[] args) throws Exception {
//        inputertest();
        jsontoxmltest();
//        csvtoxmltest();
//        inputertest2();
//        findnodetest();
//        xmltojson();
    }


    public static void xmltojson() throws Exception{
        //读取文件，行标签作为参数传入
        XmlInputer xmlInputer = new XmlInputer(path3,"RECORD");
        xmlInputer.initialize();
        xmlInputer.read();
        String[] header = xmlInputer.getHeader();

        //写出文件
        JsonOutputer jsonOutputer = new JsonOutputer(jsonoutput,header);
        String[] data;
        while((data=xmlInputer.next())!=null){
            jsonOutputer.process(data);
        }
        jsonOutputer.close();
        xmlInputer.close();
    }

    public static void csvtoxmltest() throws Exception{
        CsvDistributedInputer csvInput= new CsvDistributedInputer("/Users/chimney/Coding Practice/test.csv","utf-8");
        String[] header = csvInput.getHeader();
        Queue<String[]> dataQueue = new ConcurrentLinkedQueue<>();
        String[] data;
        while((data=csvInput.next())!=null){
            dataQueue.add(data);
        }
        XmlOutputer xmlOutputer = new XmlOutputer(outputPath2,header);
        xmlOutputer.processAll(dataQueue);
        xmlOutputer.close();
    }


    public static void jsontoxmltest() throws Exception{
        JsonInputer jsonInputer = new JsonInputer(inputPath);
        jsonInputer.initialize();
        String[] header = jsonInputer.getHeader();
        System.out.println("表头："+Arrays.toString(header));
        Queue<String[]> dataQueue = jsonInputer.getDataQueue();

        XmlOutputer xmlOutputer = new XmlOutputer(outputPath,header);
        xmlOutputer.processAll(dataQueue);
    }


    public static void inputertest() throws Exception {
        XmlInputer xmlInputer = new XmlInputer(path);
        xmlInputer.initialize();
        xmlInputer.read();
        System.out.println("记录数：" + xmlInputer.getRecordNumer());
        String[] temp;
        while ((temp = xmlInputer.next()) != null) {
            System.out.println(xmlInputer.getRowNumber()+" -- "+Arrays.toString(temp));
        }
    }

    public static void inputertest2() throws Exception{
        XmlInputer xmlInputer = new XmlInputer(path,"au_name");
        xmlInputer.initialize();
        xmlInputer.read();
        System.out.println("记录数：" + xmlInputer.getRecordNumer());
        System.out.println("表头："+Arrays.toString(xmlInputer.getHeader()));
        String[] temp;
        while ((temp = xmlInputer.next()) != null) {
            System.out.println(xmlInputer.getRowNumber()+" -- "+Arrays.toString(temp));
        }
        xmlInputer.close();
    }


    public static void directtest() {

        try {
            SAXReader saxReader = new SAXReader();
            Document document = saxReader.read(path);

            Element root = document.getRootElement();

            for (Iterator<Element> it = root.elementIterator(rowNodeName); it.hasNext(); ) {
                Element rowElement = it.next();
                rowNum++;
                System.out.println("====第" + rowNum + "行====");
                String rowName = rowElement.getName();

                for (Iterator<Element> sub_it = rowElement.elementIterator(); sub_it.hasNext(); ) {
                    Element element = sub_it.next();
                    String name = element.getName();
                    String text = element.getText();
                    System.out.println(rowName + "-" + name + ": " + text);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void findnodetest(){

        try {
            SAXReader saxReader = new SAXReader();
            Document document = saxReader.read(path);
            Element root = document.getRootElement();

            XmlInputer xmlInputer = new XmlInputer(path);

            List<Element> nodeList = new ArrayList<>();
            rowNodeName = "au_name";
            xmlInputer.addCertainNode(nodeList,root,rowNodeName);

            int i=1;
            for(Element e: nodeList) {
                System.out.println(i+" -- "+e.getName()+": "+e.getText());
                i++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

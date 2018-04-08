package etl_tools;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.seassoon.etl_tools.input.CsvDistributedInputer;
import com.seassoon.etl_tools.input.JsonByLineInputer;
import com.seassoon.etl_tools.input.JsonInputer;
import com.seassoon.etl_tools.outout.JsonOutputer;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class JsonTest {

    public static void main(String[] args) throws Exception{

        String peopleJsonPath = "/Users/chimney/Coding Practice/people.json";

        String inputPath = "/Users/chimney/Coding Practice/zc_au_0811.json";

        String outputPath = "/Users/chimney/Coding Practice/test.json";

        String outputPath2 = "/Users/chimney/Coding Practice/test2.json";

        /*用zc_au_0811测试JsonInput类的功能*/
        JsonInputer jsonInputer = new JsonInputer(inputPath);
        jsonInputer.initialize();
        System.out.println("记录数："+jsonInputer.getRecordNumer());
        System.out.println(Arrays.toString(jsonInputer.getHeader()));
        String[] data;
        int limit =0;
        while ((data=jsonInputer.next())!=null){
            System.out.println(Arrays.toString(data));
            limit++;
            if(limit>10) break;
        }
        jsonInputer.close();

//        /*测试people.JSON这类的格式解析出来的结果*/
////        JsonInputer jsonInputer = new JsonInputer(peopleJsonPath);
////        String peopleJsonText = jsonInputer.readJsonFileToText(peopleJsonPath);
////        Object o = JSON.parse(peopleJsonText);
//
//

        System.out.println("========================");
        /*测试people.JSON的处理*/
        JsonByLineInputer jsonByLineInputer = new JsonByLineInputer(peopleJsonPath);
        jsonByLineInputer.initialize();
        System.out.println(Arrays.toString(jsonByLineInputer.getHeader()));
        String[] data3;
        while ((data3=jsonByLineInputer.next())!=null){
            System.out.println(Arrays.toString(data3));
        }

        System.out.println("========================");

        /*测试csv转json*/
        CsvDistributedInputer csvInput= new CsvDistributedInputer("/Users/chimney/Coding Practice/test.csv","utf-8");
        String[] header = csvInput.getHeader();
        System.out.println("header:");
        System.out.println(Arrays.toString(header));



        JsonOutputer jsonOutputer = new JsonOutputer(outputPath,header);
        jsonOutputer.initialize();
        String[] data2;

        Queue<String[]> dataQueue = new ConcurrentLinkedQueue<>();//用于测试PROCESSALL

        while ((data2 = csvInput.next()) != null) {
            dataQueue.add(data2);

            System.out.println("data:");
            System.out.println(Arrays.toString(data2));

            jsonOutputer.process(data2);
        }
        jsonOutputer.close();

        /*测试printWriter*/
//        PrintWriter printWriter = new PrintWriter(new FileWriter(outputPath));
//        printWriter.write("{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}");
//        printWriter.close();


        /*测试输出正确的Json格式文件*/
//        JsonInputer jsonInputer2 = new JsonInputer(inputPath);
//        jsonInputer2.initialize();
//        String header2[] = jsonInputer2.getHeader();
//        System.out.println(Arrays.toString(header2));
        JsonOutputer jsonOutputer2 = new JsonOutputer(outputPath2,header);
        jsonOutputer2.initialize();
        jsonOutputer2.processAll(dataQueue);
//        jsonInputer2.close();
        jsonOutputer2.close();

    }

}

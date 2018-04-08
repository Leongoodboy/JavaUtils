package etl_tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.SocketTimeoutException;
import java.util.Map;

public class HttpUtils { 
    
    /** 
     * 发送GET请求 
     *  
     * @param url 
     *            目的地址 
     * @param parameters 
     *            请求参数，Map类型。 
     * @return 远程响应结果 
     * @throws SocketTimeoutException 
     */  
    public static String sendGet(String url, Map<String, String> parameters,int timeout) throws SocketTimeoutException { 
        System.out.println("使用1.1 里面的get 请求");
    	String result="";
        BufferedReader in = null;// 读取响应输入流  
        StringBuffer sb = new StringBuffer();// 存储参数  
        String params = "";// 编码之后的参数
        try {
            // 编码请求参数 
          
            if(parameters.size()==1){
                for(String name:parameters.keySet()){
                    sb.append(name).append("=").append(
                            java.net.URLEncoder.encode(parameters.get(name),  
                            "UTF-8"));
                }
                params=sb.toString();
            }else if(parameters.size()>1){
                for (String name : parameters.keySet()) {  
                    sb.append(name).append("=").append(  
                            java.net.URLEncoder.encode(parameters.get(name),  
                                    "UTF-8")).append("&");  
                }  
                String temp_params = sb.toString();  
                params = temp_params.substring(0, temp_params.length() - 1);  
            }
            String full_url = url + "?" + params; 
            // 创建URL对象  
            java.net.URL connURL = new java.net.URL(full_url);  
            // 打开URL连接  
            java.net.HttpURLConnection httpConn = (java.net.HttpURLConnection) connURL  
                    .openConnection(); 
            if(timeout>0){
                httpConn.setConnectTimeout(timeout);
            }
            // 设置通用属性  
            httpConn.setRequestProperty("Accept", "*/*");  
            httpConn.setRequestProperty("Connection", "Keep-Alive");  
            httpConn.setRequestProperty("User-Agent",  
                    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1)"); 
            httpConn.setRequestProperty("Cookie",  
                    "rememberMe=v3Ad2lpwE3HU5RmO51guWSn56F9LaJsWRRE47y3j3zlomt0ze0xmYlKA1kCSZBbXvCIVmVnziRbtkqNd+E5rFVQWCY2xoMSXMJcgDJF/s8qKzYN2jBd8rt6Om1Ze4mC+OU7A9UmL/ObdluS8B2qO5aMD5IkglGyZ7rwr04NnLmIqxYKQqKoctA3NtBP5PKnADyzsgKCHhlSAq6m7aMcbUCdpqSMokwWthoo2jHiKlpqjc09estzw30TaeRjZ0mvq6t2Re8fRCcd0KHFLvB9bUytRSn5Sd66kWa5N7Wz7thUB4I1dvm/pnuJ2DyqXKyDJcgQlvjLkNE58IXwawAx9hlGIaZkAwuKU3J0gV2LGUD9RFRtZJw1OwVENw/q5iIZiO4GWGm4R3niIoos1mUJq1YLak53c4k71YkVCTcAmWdlZ41yyYhN7qzL8QIUhfIF8HusMP/N34HX1IKqVw14HRU7QQBMV78HGfhKzBL/Q+A6jp7grnzUFmU3OAOpnTjH5; JSESSIONID=35b5d773-f19e-47ab-a73d-4676ee0e8b8c"); 
            // 建立实际的连接  
            httpConn.connect();  
            // 定义BufferedReader输入流来读取URL的响应,并设置编码方式  
            in = new BufferedReader(new InputStreamReader(httpConn  
                    .getInputStream(), "UTF-8"));  
            String line;  
            // 读取返回的内容  
            while ((line = in.readLine()) != null) {  
                result += line;  
            }  
        }catch(SocketTimeoutException timeoutException){
            throw timeoutException;
        }
        catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {  
                if (in != null) {  
                    in.close();  
                }  
            } catch (IOException ex) {  
                ex.printStackTrace();  
            }  
        }
        return result ;
    }  
  
    /** 
     * 发送POST请求 
     *  
     * @param url 
     *            目的地址 
     * @param parameters 
     *            请求参数，Map类型。 
     * @return 远程响应结果 
     * @throws SocketTimeoutException 
     */  
    public static String sendPost(String url, Map<String, String> parameters,int timeout) throws SocketTimeoutException {  
    	System.out.println("使用1.1 里面的post 请求");
    	String result = "";// 返回的结果  
        BufferedReader in = null;// 读取响应输入流  
        PrintWriter out = null;  
        StringBuffer sb = new StringBuffer();// 处理请求参数  
        String params = "";// 编码之后的参数  
        try {  
            // 编码请求参数  
            if (parameters.size() == 1) {  
                for (String name : parameters.keySet()) {  
                    sb.append(name).append("=").append(  
                            java.net.URLEncoder.encode(parameters.get(name),  
                                    "UTF-8"));  
                }  
                params = sb.toString();  
            } else if(parameters.size()>1){  
                for (String name : parameters.keySet()) {  
                    sb.append(name).append("=").append(  
                            java.net.URLEncoder.encode(parameters.get(name),  
                                    "UTF-8")).append("&");  
                }  
                String temp_params = sb.toString();  
                params = temp_params.substring(0, temp_params.length() - 1);  
            }  
            // 创建URL对象  
            java.net.URL connURL = new java.net.URL(url);  
            // 打开URL连接  
            java.net.HttpURLConnection httpConn = (java.net.HttpURLConnection) connURL  
                    .openConnection();  
            // 设置通用属性  
            httpConn.setRequestProperty("Accept", "*/*");  
            httpConn.setRequestProperty("Connection", "Keep-Alive");  
            httpConn.setRequestProperty("User-Agent",  
                    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1)");
            if(timeout>0){
                httpConn.setConnectTimeout(timeout);
            }
            System.out.println(params);
            // 设置POST方式  
            httpConn.setDoInput(true);  
            httpConn.setDoOutput(true);  
            // 获取HttpURLConnection对象对应的输出流  
            out = new PrintWriter(httpConn.getOutputStream());  
            // 发送请求参数  
            out.write(params);  
            // flush输出流的缓冲  
            out.flush();  
            // 定义BufferedReader输入流来读取URL的响应，设置编码方式  
            in = new BufferedReader(new InputStreamReader(httpConn  
                    .getInputStream(), "UTF-8"));  
            String line;  
            // 读取返回的内容  
            while ((line = in.readLine()) != null) {  
                result += line;  
            }  
        }catch(SocketTimeoutException socketTimeoutException){
            throw socketTimeoutException;
        } catch (Exception e) {  
            e.printStackTrace();  
        } finally {  
            try {  
                if (out != null) {  
                    out.close();  
                }  
                if (in != null) {  
                    in.close();  
                }  
            } catch (IOException ex) {  
                ex.printStackTrace();  
            }  
        }  
        return result;  
    }  
}
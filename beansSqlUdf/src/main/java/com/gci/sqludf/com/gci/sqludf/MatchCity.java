package com.gci.sqludf.com.gci.sqludf;

import java.security.MessageDigest;
import org.apache.hadoop.hive.ql.exec.UDF;

public class MatchCity extends UDF{
    public static void main(String[] args) {
        String address = "新疆维吾尔自治区曹尼玛市天河区棠下街道中山大道西大舜商务中心(科新路)";
        System.out.println("city：" + evaluate(address));
    }

    public static String evaluate(String address) {
        try{
            int length = address.length();
            int fromIdx = -1; //截取市名称开始的那一位
            int toIdx = -1; // 截取市结束的那一位
            for (int i = 0; i < length; i++) {
                String addressi = address.substring(i,i+1);
                if (addressi.equals("省")|(addressi.equals("区"))){
                    fromIdx = i+1;
                }
                if (addressi.equals("市")){
                    toIdx = i+1;
                    break;
                }

            }

            String city = address.substring(fromIdx, toIdx);
            return city;
        }catch (Exception e){
            return "广州市";  //如果报错，就默认广州市
        }



    }
}

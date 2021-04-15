package com.gci.forecast;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jpmml.evaluator.*;

import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.InputField;

// 预测入口类
public class CdPredict {
    public static void main(String[] args) {
        CdPredict cp = new CdPredict();
//        Evaluator evaluator = cp.loadPmml2();
        String modelAddr = "D:/project_code/src_bigdata_machine_learning/src/cd_bus_at_predict/code/new/V1.0/train/tmp_model/tmp_model83.pmml";
        Evaluator evaluator = cp.loadPmml(modelAddr);
        System.out.println("evaluator="+evaluator);
        System.out.println("done!");
        Double r = cp.predict(evaluator);
    }

    // 主函数，预测
    private double predict(Evaluator evaluator){
        // 获取输入数据
        Map<String, Object> inputData = this.getInputData();  // this相当于python的self  本类

        List<InputField> inputFields = evaluator.getInputFields();  // 获取pmml模型的特征
        System.out.println("inputFields="+inputFields);
        Map<FieldName, FieldValue> arguments = new LinkedHashMap<FieldName, FieldValue>();
        for (InputField inputField : inputFields) {
            FieldName inputFieldName = inputField.getName();
            Object rawValue = inputData.get(inputFieldName.getValue());
            FieldValue inputFieldValue = inputField.prepare(rawValue);
            arguments.put(inputFieldName, inputFieldValue);
        }
        System.out.println("arguments="+arguments);

        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        System.out.println("result="+results);
        return 0;
    }

    //获取到站预测pmml模型
    private Evaluator loadPmml(String modelAddr) {
        Evaluator evaluator = null;
        try {
            evaluator = new LoadingModelEvaluatorBuilder().load(new File(modelAddr)).build();  //获取可用来预测的模型
        } catch (IOException e) {
            // TODO Auto-generated catch block
//			System.out.println(1);
//			e.printStackTrace();
        } catch (SAXException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (JAXBException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return evaluator;
    }

    // 获取实时输入数据流
    private Map<String, Object> getInputData() {
        Map<String, Object> inputData = new HashMap<String, Object>();
//        inputData.put("waiting_time_trend_index", 1);
//        inputData.put("5min_parti", 172);
//        inputData.put("route_id", 4473);
//        inputData.put("10min_parti", 86);
//
//        inputData.put("last1_waitingtime", 28);
//        inputData.put("conbine_id", 10004462289681.0);
//        inputData.put("60min_parti", 14);
//        inputData.put("mid_num", 12);
//
//        inputData.put("holiday", 0);
//        inputData.put("15min_parti", 57);
//        inputData.put("direction", 0);
//        inputData.put("week_parti", 4);
//        inputData.put("30min_parti", 45);

        inputData.put("conbine_id", 3029730282.0);
        inputData.put("mid_num", 14);
        inputData.put("direction", 1);
        inputData.put("minute5_parti", 172);

        inputData.put("minute15_parti", 57);
        inputData.put("hour_parti", 14);
        inputData.put("week_parti", 1);
        inputData.put("last1_waitingtime", 1080);
        return inputData;
    }

}




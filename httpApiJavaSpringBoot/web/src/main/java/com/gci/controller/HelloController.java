package com.gci.controller;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;



/**
 * 业务逻辑实现的主函数 sayHelloGet/sayHelloPost
 */

@RestController
@RequestMapping("/")  //
public class HelloController {
    // get请求业务逻辑处理
    @GetMapping("helloSpringGet")
    public Map<String,Double> myAddDoGet(@RequestParam Map<String,Object> reqMap){   //@RequestParam Map<String,Object> reqMap
        System.out.println("hhhhh");
        return this.myAddDoPost(reqMap);
    }

    // post请求业务逻辑处理
    @PostMapping("helloSpringPost")
    public Map<String,Double> myAddDoPost(@RequestParam Map<String,Object> reqMap) {  //通过注解获取请求参数  @RequestParam Map<String,Object> reqMap
        Map<String,Double> result = new HashMap<String,Double>();
        Map<String,Double> dataMap = new HashMap<String,Double>();
        String dataString = reqMap.get("data").toString();
        Gson gson = new Gson();
        dataMap = gson.fromJson(dataString,new TypeToken<Map<String, Object>>() {}.getType());
        Double a = dataMap.get("a");
        Double b = dataMap.get("b");
        Double sum_ = a + b;

        result.put("sum", sum_);
        return result;
    }
}

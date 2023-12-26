package com.bigdata.insightanalytics.controller;

import com.bigdata.insightanalytics.bean.NameValue;
import com.bigdata.insightanalytics.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 控制层
 */
@RestController
public class PublisherController {
    @Autowired
    PublisherService publisherService;

    @GetMapping("detailByItem")
    public Map<String ,Object> detailByItem(
            @RequestParam("date") String date,
            @RequestParam("itemName") String itemName,
            @RequestParam(value = "pageNo",required = false,defaultValue = "1") Integer pageNo,
            @RequestParam(value = "pageSize",required = false,defaultValue = "20") Integer pageSize
    ){
        Map<String , Object> results = publisherService.doDetailByItem(date,itemName,pageNo,pageSize);
        return results;
    }
    //交易分析
    @GetMapping("statsByItem")
    public List<NameValue> stateByItem(
            @RequestParam("itemName") String itemName,
            @RequestParam("date") String date,
            @RequestParam("t") String t){
        List<NameValue> results = publisherService.doStateByItem(itemName, date, t);

        return results;
    }





    //日活统计
    @GetMapping("dauRealtime")
    public Map<String,Object> duaRealtime(@RequestParam("td")String td){
        Map<String, Object> results = publisherService.doDauRealtime(td);
        return results;
    }
}

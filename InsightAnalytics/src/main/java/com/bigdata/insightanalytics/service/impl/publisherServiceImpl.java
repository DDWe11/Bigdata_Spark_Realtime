package com.bigdata.insightanalytics.service.impl;

import com.bigdata.insightanalytics.bean.NameValue;
import com.bigdata.insightanalytics.mapper.PublisherMapper;
import com.bigdata.insightanalytics.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 业务层
 */
@Service
public class publisherServiceImpl implements PublisherService {
    @Autowired
    PublisherMapper publisherMapper;
    //活跃分析
    @Override
    public Map<String, Object> doDauRealtime(String td) {
        //业务处理
        Map<String, Object> dauResults = publisherMapper.searchDau(td);
        return dauResults;
    }

    //交易分析
    @Override
    public List<NameValue> doStateByItem(String itemName, String date, String t) {
        List<NameValue> searchResults = publisherMapper.searchStateByItem(itemName,date,typeToField(t));
        return transformResults(searchResults,t);
    }

    //明细查询
    @Override
    public Map<String, Object> doDetailByItem(String date, String itemName, Integer pageNo, Integer pageSize) {
        //计算分页开始的位置
        int from = (pageNo-1)*pageSize;
        Map<String, Object> searchResults = publisherMapper.searchDetailByItem(date,itemName,from,pageSize);

        return searchResults;
    }

    public List<NameValue> transformResults(List<NameValue> searchResults,String t){
        if("gender".equals(t)){
            if(searchResults.size()>0){
                for(NameValue nameValue : searchResults){
                    String name = nameValue.getName();
                    if(name.equals("F")){
                        nameValue.setName("女");
                    }else if (name.equals("M")){
                        nameValue.setName("男");
                    }
                }
            }
            return searchResults;
        }else if("age".equals(t)){
            double totalAmountunder20 = 0;
            double totalAmount20to29 = 0;
            double totalAmountabove30 = 0;
            if(searchResults.size()>0){
                for(NameValue nameValue :searchResults){
                    Integer age = Integer.parseInt(nameValue.getName());
                    Double value = Double.parseDouble(nameValue.getValue().toString());
                    if(age<=20){
                        totalAmountunder20+=value;
                    } else if (age<=29) {
                        totalAmount20to29+=value;
                    } else if (age >= 30) {
                        totalAmountabove30+=value;
                    }
                }
                searchResults.clear();
                searchResults.add(new NameValue("20岁以下",totalAmountunder20));
                searchResults.add(new NameValue("20-29岁",totalAmount20to29));
                searchResults.add(new NameValue("30岁以上",totalAmountabove30));
            }
            return searchResults;
        }else {
            return null;
        }
    }
    public String typeToField(String t){
        if("age".equals(t)){
            return "user_age";
        }else if("gender".equals(t)){
            return "user_gender";
        }else{
            return null;
        }
    }
}

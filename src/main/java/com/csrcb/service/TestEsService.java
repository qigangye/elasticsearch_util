package com.csrcb.service;

import com.csrcb.util.EsUtil;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Classname TestEsService
 * @Description 测试es的服务类
 * @Date 2021/6/15 18:53
 * @Created by gangye
 */
@Service
public class TestEsService {
    public Map<String, Object> testQueryEs(Map<String, Object> requestContext){
        Map<String, Object> equalCondition = new HashMap<>();
        equalCondition.put("cust_no_s",requestContext.get("custNo"));
        Map<String, Object> rangeCondition = new HashMap<>();
        rangeCondition.put("txn_amt_d","(,"+requestContext.get("rangeAmt") + "]");
        List<String> orderby = Arrays.asList("-txn_date_s");
        return EsUtil.queryForEs("trade_info",equalCondition,rangeCondition,orderby,1,10);
    }
}

package com.csrcb.service;

import com.csrcb.util.EsUtil;
import org.springframework.stereotype.Service;

import java.util.*;

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

    public Map<String, Object> testEsInsert(Map<String, Object> requestContext){
        Map<String, Object> insertMap = new HashMap<>();
        insertMap.put("id_s",requestContext.get("id"));
        insertMap.put("card_no_s",requestContext.get("cardNo"));
        insertMap.put("card_type_s",requestContext.get("cardType"));
        insertMap.put("acct_no_s",requestContext.get("acctNo"));
        insertMap.put("cust_no_s",requestContext.get("custNo"));
        insertMap.put("cust_name_s",requestContext.get("custName"));
        insertMap.put("txn_month_s",requestContext.get("txnMonth"));
        insertMap.put("txn_seqno_s",requestContext.get("txnSeqno"));
        insertMap.put("txn_date_s",requestContext.get("txnDate"));
        insertMap.put("txn_time_s",requestContext.get("txnTime"));
        insertMap.put("txn_re_type_s",requestContext.get("txnReType"));
        insertMap.put("txn_type_s",requestContext.get("txnType"));
        insertMap.put("txn_amt_d",requestContext.get("txnAmt"));
        insertMap.put("avl_bal_d",requestContext.get("avlBal"));
        insertMap.put("txn_chnl_s",requestContext.get("txnChnl"));
        insertMap.put("txn_remark_s",requestContext.get("txnRemark"));
        return EsUtil.singleInsert("income_expense_analysis",insertMap);
    }

    public Map<String, Object> testEsUpdate(Map<String, Object> requestContext){
        Map<String, Object> updateMap = new HashMap<>();
        updateMap.put("id_s",requestContext.get("id"));
        updateMap.put("txn_remark_s",requestContext.get("txnRemark"));
        return EsUtil.updateEsRecord("income_expense_analysis",updateMap);
    }

    public Map<String, Object> testEsDelete(String id){
        return EsUtil.deleteEsRecord("income_expense_analysis",id);
    }

    public Map<String, Object> testEsBulk(){
        return EsUtil.batchInsert("income_expense_analysis",getBulkData());
    }

    private List<Map<String, Object>> getBulkData(){
        List<Map<String, Object>> bulkData = new ArrayList<>();
        Map<String,Object> data1 = new HashMap<>();
        data1.put("id_s","2020-03-31318648");
        data1.put("card_no_s","6231232131123");
        data1.put("card_type_s","01");
        data1.put("acct_no_s","101010101010");
        data1.put("cust_no_s","111666");
        data1.put("cust_name_s","顾娟");
        data1.put("txn_month_s","2020-03");
        data1.put("txn_seqno_s","318648");
        data1.put("txn_date_s","2020-03-31");
        data1.put("txn_time_s","13:14:26");
        data1.put("txn_re_type_s","R");
        data1.put("txn_type_s","收入");
        data1.put("txn_amt_d",210);
        data1.put("avl_bal_d",54206.29);
        data1.put("txn_chnl_s","73");
        data1.put("txn_remark_s","03月30日商户入账");
        Map<String,Object> data2 = new HashMap<>();
        data2.put("id_s","2020-03-31584152");
        data2.put("card_no_s","6231232131123");
        data2.put("card_type_s","01");
        data2.put("acct_no_s","101010101010");
        data2.put("cust_no_s","111666");
        data2.put("cust_name_s","顾娟");
        data2.put("txn_month_s","2020-03");
        data2.put("txn_seqno_s","584152");
        data2.put("txn_date_s","2020-03-31");
        data2.put("txn_time_s","19:23:57");
        data2.put("txn_re_type_s","E");
        data2.put("txn_type_s","支出");
        data2.put("txn_amt_d",55);
        data2.put("avl_bal_d",54013.29);
        data2.put("txn_chnl_s","NA");
        data2.put("txn_remark_s","财付通支付");
        Map<String,Object> data3 = new HashMap<>();
        data3.put("id_s","2020-03-31585274");
        data3.put("card_no_s","6231232131123");
        data3.put("card_type_s","01");
        data3.put("acct_no_s","101010101010");
        data3.put("cust_no_s","111666");
        data3.put("cust_name_s","顾娟");
        data3.put("txn_month_s","2020-03");
        data3.put("txn_seqno_s","585274");
        data3.put("txn_date_s","2020-03-31");
        data3.put("txn_time_s","19:26:43");
        data3.put("txn_re_type_s","E");
        data3.put("txn_type_s","支出");
        data3.put("txn_amt_d",12);
        data3.put("avl_bal_d",54001.29);
        data3.put("txn_chnl_s","NB");
        data3.put("txn_remark_s","财付通B支付");
        Map<String,Object> data4 = new HashMap<>();
        data4.put("id_s","2020-03-31489947");
        data4.put("card_no_s","6231232131123");
        data4.put("card_type_s","01");
        data4.put("acct_no_s","101010101010");
        data4.put("cust_no_s","111666");
        data4.put("cust_name_s","顾娟");
        data4.put("txn_month_s","2020-03");
        data4.put("txn_seqno_s","489947");
        data4.put("txn_date_s","2020-03-31");
        data4.put("txn_time_s","13:14:26");
        data4.put("txn_re_type_s","E");
        data4.put("txn_type_s","支出");
        data4.put("txn_amt_d",138);
        data4.put("avl_bal_d",54068.29);
        data4.put("txn_chnl_s","NA");
        data4.put("txn_remark_s","财付通支付");
        Map<String,Object> data5 = new HashMap<>();
        data5.put("id_s","2020-04-28489953");
        data5.put("card_no_s","6231232131123");
        data5.put("card_type_s","01");
        data5.put("acct_no_s","101010101010");
        data5.put("cust_no_s","111666");
        data5.put("cust_name_s","顾娟");
        data5.put("txn_month_s","2020-04");
        data5.put("txn_seqno_s","489953");
        data5.put("txn_date_s","2020-04-28");
        data5.put("txn_time_s","10:24:26");
        data5.put("txn_re_type_s","R");
        data5.put("txn_type_s","收入");
        data5.put("txn_amt_d",520);
        data5.put("avl_bal_d",61368.29);
        data5.put("txn_chnl_s","73");
        data5.put("txn_remark_s","04月28日商户入账");
        bulkData.add(data1);
        bulkData.add(data2);
        bulkData.add(data3);
        bulkData.add(data4);
        bulkData.add(data5);
        return bulkData;
    }

    public Map<String, Object> testEsAggs(Map<String, Object> requestContext){
        Map<String,Object> equalCondition = new HashMap<>();
        equalCondition.put("cust_no_s",requestContext.get("custNo"));
        List<String> txnMonths = Arrays.asList(((String) requestContext.get("txnMonth")).split(","));
        equalCondition.put("txn_month_s",txnMonths);
        List<String> aggsFields = Arrays.asList("txn_amt_d");
        return EsUtil.aggregationQuery("income_expense_analysis",equalCondition,null,aggsFields);
    }

    public Map<String, Object> testEsGroupAggs(Map<String, Object> requestContext){
        Map<String,Object> equalCondition = new HashMap<>();
        equalCondition.put("cust_no_s",requestContext.get("custNo"));
        String groupFileds = "txn_month_s,txn_re_type_s,txn_chnl_s";
        List<String> txnMonths = Arrays.asList(groupFileds.split(","));
        return EsUtil.groupAggregationQuery("income_expense_analysis",equalCondition,null,"txn_amt_d",txnMonths);
    }

}

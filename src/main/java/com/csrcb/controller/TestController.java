package com.csrcb.controller;

import com.csrcb.service.TestEsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * @Classname TestController
 * @Description 服务暴露
 * @Date 2021/6/15 18:57
 * @Created by gangye
 */
@Slf4j
@RestController
public class TestController {
    @Autowired
    private TestEsService testEsService;

    //例如查询trade_info索引中客户号为10005，交易金额在200以内（包含200）的记录，并按交易时间倒序排序查询
    @PostMapping("/queryCust")
    public Map<String, Object> queryCust(@RequestBody Map<String,Object> requestContext){
        return testEsService.testQueryEs(requestContext);
    }

    //例如向income_expense_analysis索引中插入一条记录
    @PostMapping("/insertCust")
    public Map<String, Object> insertCust(@RequestBody Map<String,Object> requestContext){
        return testEsService.testEsInsert(requestContext);
    }

    //例如修改刚新增id为2020-03-313186482的文档的txn_remark_s字段为"03月31日商户交易入账"
    @PostMapping("/updateEsRecord")
    public Map<String, Object> updateEsRecord(@RequestBody Map<String,Object> requestContext){
        return testEsService.testEsUpdate(requestContext);
    }

    //例如删除刚新增id为2020-03-313186482的文档
    @GetMapping("/deleteEsRecord/{id}")
    public Map<String, Object> deleteEsRecord(@PathVariable("id") String id){
        return testEsService.testEsDelete(id);
    }

    //例如批量插入数据
    @GetMapping("/batchBulkEs")
    public Map<String, Object> batchBulkEs(){
        return testEsService.testEsBulk();
    }

    //例如聚合客户号为111666，交易月份为2020-03或者2020-04的交易金额字段
    @PostMapping("/aggsEsRecord")
    public Map<String, Object> aggsEsRecord(@RequestBody Map<String,Object> requestContext){
        return testEsService.testEsAggs(requestContext);
    }

    //例如获得客户号为111666，根据交易月份txn_month_s、收支类型txn_re_type_s、交易渠道txn_chnl_s分组，然后聚合求和
    @PostMapping("/groupAggsEsRecord")
    public Map<String, Object> groupAggsEsRecord(@RequestBody Map<String,Object> requestContext){
        return testEsService.testEsGroupAggs(requestContext);
    }
}

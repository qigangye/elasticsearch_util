package com.csrcb.controller;

import com.csrcb.service.TestEsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

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

}

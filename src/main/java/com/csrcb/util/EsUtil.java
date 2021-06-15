package com.csrcb.util;

import com.csrcb.common.DefineConstant;
import com.csrcb.config.EsClient;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @Classname EsUtil
 * @Description ElasticSearch工具类
 * @Date 2021/6/3 9:56
 * @Created by gangye
 */
@Slf4j
public class EsUtil {

    public EsUtil(){}

    /**
     * @Description ElasticSearch条件查询
     * @param tableName       (索引名)也可以类似的说成表名
     * @param equalsCondition 关键字等值条件
     *                        若是一个字符串以%结尾，则匹配以去掉%的字符串开头的记录
     *                        若是一个字符串以*开头或结尾，则模糊匹配去掉*的记录  类似于sql中的like '%str%'
     *                        若传入的是一个普通的字符串，则等值查询
     *                        若传入的是一个集合，则使用的是in条件查询
     * @param rangeCondition  条件范围查询
     *                        字段，字段对应值的区间，区间格式[,]/(,)/[,)/(,]，逗号的左右可以没值
     * @param orderBy         排序字段
     *                        若是字段以中划线-开头，则使用降序排序，类似于sql中的desc
     *                        若正常字段排序，则使用增序排序，类似于sql中的asc
     * @param pageNum         页数
     * @param pageSize        每页大小
     * @return
     */
    public static Map<String ,Object> queryForEs(String tableName, Map<String, Object> equalsCondition, Map<String, Object> rangeCondition, List<String> orderBy, int pageNum, int pageSize){
        Map<String, Object> resultMap = new HashMap<>(8);
        List<Map<String,Object>> queryResult = new ArrayList<>();
        long totalNum = 0;
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // and 等值查询
        // 某一field=具体的值； 也可以某一field 的值 in 具体定义集合里的值
        if (null != equalsCondition && !equalsCondition.isEmpty()){
            for (Map.Entry<String ,Object> entry : equalsCondition.entrySet()){
                String key = entry.getKey();
                //由于我创建索引的时候使用字符串不分词使用的.keyword类型
                if (key.endsWith("_s")){
                    queryValueBuild(boolQueryBuilder, key + ".keyword", entry.getValue());
                }else{
                    queryValueBuild(boolQueryBuilder, key, entry.getValue());
                }
            }
        }
        //范围查询
        if (null != rangeCondition && !rangeCondition.isEmpty()){
            rangeValueBuild(boolQueryBuilder, rangeCondition);
        }
        sourceBuilder.query(boolQueryBuilder);
        //排序
        if (null != orderBy && !orderBy.isEmpty()){
            buildSort(sourceBuilder, orderBy);
        }
        //分页(es分页查询默认是查询返回10条记录，而深度分页，默认是10000条数据,也就是一次性最多返回10000条,设置size就可以实现，但是如果实际数据量特别大，可以使用scroll游标查询，此处主要常规分页查询)
        if (pageNum > 0){
            sourceBuilder.from(pageSize * (pageNum - 1));
        } else {
            sourceBuilder.from(0);
        }
        sourceBuilder.size(pageSize);

        //执行查询
        SearchResponse response = executeSearch(tableName, sourceBuilder);
        SearchHits searchHits = response.getHits();
        SearchHit[] hits = searchHits.getHits();
        totalNum = searchHits.getTotalHits();
        for (int i = 0; i < hits.length; i++){
            SearchHit hit = hits[i];
            Map<String, Object> sourceMap= hit.getSourceAsMap();
            sourceMap.put("id_s", hit.getId());
            queryResult.add(sourceMap);
        }
        resultMap.put("pageList", queryResult);
        resultMap.put("totalNum", totalNum);
        resultMap.put("pageNum", pageNum);
        resultMap.put("pageSize", pageSize);
        return resultMap;
    }

    /**
     * @Description 查询条件组装
     * @param boolQueryBuilder
     * @param key
     * @param value
     */
    private static void queryValueBuild(BoolQueryBuilder boolQueryBuilder, String key, Object value){
        TermQueryBuilder termQueryBuilder;
        if (null != value && !"".equals(value)){
            if (value instanceof String){
                String strValue = (String) value;
                if (strValue.endsWith("%")){
                    PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery(key,strValue.replace("%",""));
                    boolQueryBuilder.must(prefixQueryBuilder);
                }else if (strValue.startsWith("*") || strValue.endsWith("*")){
                    MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(key, strValue.replace("*",""));
                    boolQueryBuilder.must(matchQueryBuilder);
                }else {
                    termQueryBuilder = QueryBuilders.termQuery(key, strValue);
                    boolQueryBuilder.must(termQueryBuilder);
                }
            } else if (value instanceof Collection){
                Collection<? extends Object> collectionValue = (Collection<? extends Object>) value;
                //此处使用了多值条件
                boolQueryBuilder.must(QueryBuilders.termsQuery(key, collectionValue));
            } else {
                termQueryBuilder = QueryBuilders.termQuery(key, value);
                boolQueryBuilder.must(termQueryBuilder);
            }
        }
    }

    /**
     * @Description 范围条件查询组装
     * @param boolQueryBuilder
     * @param rangeCondition
     */
    private static void rangeValueBuild(BoolQueryBuilder boolQueryBuilder, Map<String, Object> rangeCondition){
        for (Map.Entry<String, Object> entry : rangeCondition.entrySet()){
            Map<String, Object> range = intervalParse((String) entry.getValue());
            String key = entry.getKey();
            RangeQueryBuilder rangeQueryBuilder;
            if (key.endsWith("_s")){
                rangeQueryBuilder = QueryBuilders.rangeQuery(key + ".keyword");
            }else {
                rangeQueryBuilder = QueryBuilders.rangeQuery(key);
            }
            if (StringUtils.isEmpty(range.get("leftValue"))){
                if (DefineConstant.INTERVAL_OPEN_VALUE.equals(range.get("leftType"))){
                    rangeQueryBuilder.from(range.get("leftValue"),false);
                } else if (DefineConstant.INTERVAL_CLOSE_VALUE.equals(range.get("leftType"))){
                    rangeQueryBuilder.from(range.get("leftValue"),true);
                }
            }
            if (StringUtils.isEmpty(range.get("rightValue"))){
                if (DefineConstant.INTERVAL_OPEN_VALUE.equals(range.get("rightType"))){
                    rangeQueryBuilder.from(range.get("rightValue"),false);
                } else if (DefineConstant.INTERVAL_CLOSE_VALUE.equals(range.get("rightType"))){
                    rangeQueryBuilder.from(range.get("rightValue"),true);
                }
            }
            boolQueryBuilder.must(rangeQueryBuilder);
        }
    }

    /**
     * @Description 区间解析：[,]/(,)/[,)/(,]
     * @param interval
     * @return
     */
    private static Map<String, Object> intervalParse(String interval){
        Map<String, Object> range = new HashMap<>();
        if (interval.startsWith(DefineConstant.INTERVAL_CLOSE_LEFT)){
            range.put("leftType", DefineConstant.INTERVAL_CLOSE_VALUE);
        } else if (interval.startsWith(DefineConstant.INTERVAL_OPEN_LEFT)){
            range.put("leftType", DefineConstant.INTERVAL_OPEN_VALUE);
        } else{
            log.error("区间参数格式错误：{}",interval);
            //若实际业务相关需要，抛出异常处理throw new Exception();
        }
        if (interval.endsWith(DefineConstant.INTERVAL_CLOSE_RIGHT)){
            range.put("rightType", DefineConstant.INTERVAL_CLOSE_VALUE);
        } else if (interval.startsWith(DefineConstant.INTERVAL_OPEN_RIGHT)){
            range.put("rightType", DefineConstant.INTERVAL_OPEN_VALUE);
        } else{
            log.error("区间参数格式错误：{}",interval);
            //若实际业务相关需要，抛出异常处理throw new Exception();
        }
        int strLen = interval.length();
        String[] lr = interval.substring(1, strLen - 1).split(DefineConstant.COMMAN_SIGN, 2);
        if (lr.length > 0){
            range.put("leftValue", lr[0]);
        }
        if (lr.length > 1){
            range.put("rightValue", lr[1]);
        }
        return range;
    }

    /**
     * @Description 查询排序
     * @param sourceBuilder
     * @param orderBy
     */
    private static void buildSort(SearchSourceBuilder sourceBuilder, List<String> orderBy){
        SortBuilder<FieldSortBuilder> sortBuilder;
        for (String sortField : orderBy){
            if (sortField.startsWith("-")){
                //降序排序
                if (sortField.endsWith("_s")){
                    sortBuilder = SortBuilders.fieldSort(sortField.replace("-","") + ".keyword").order(SortOrder.DESC);
                } else {
                    sortBuilder = SortBuilders.fieldSort(sortField.replace("-","")).order(SortOrder.DESC);
                }
            } else {
                //升序排序
                if (sortField.endsWith("_s")){
                    sortBuilder = SortBuilders.fieldSort(sortField.replace("-","") + ".keyword").order(SortOrder.ASC);
                } else {
                    sortBuilder = SortBuilders.fieldSort(sortField.replace("-","")).order(SortOrder.ASC);
                }
            }
            sourceBuilder.sort(sortBuilder);
        }
    }

    /**
     * @Description 执行查询
     * @param tableName 对应的es的index名
     * @param sourceBuilder
     * @return
     */
    private static SearchResponse executeSearch(String tableName, SearchSourceBuilder sourceBuilder){
        // 获取不同系统的换行符
        String lineSeparator = System.lineSeparator();
        log.info(lineSeparator + "index:" + tableName + lineSeparator + "search:"+ sourceBuilder.toString() + lineSeparator);
        RestHighLevelClient client = EsClient.getInstance();
        SearchRequest searchRequest = new SearchRequest(tableName);
        SearchResponse response = null;
        //设置查询的文档代表的对象的类，即type(此处我写死固定值，可以自行定义)
        searchRequest.types(DefineConstant.SEARCH_REQUEST_TYPE);
        searchRequest.source(sourceBuilder);
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
            log.info("search status:{}, totalNum:{}",response.status(), response.getHits().getTotalHits());
        } catch (IOException e) {
            //异常处理，实际业务中是需要根据需求具体处理，自定义异常捕获
            log.error(e.getMessage());
        }
        return response;
    }

    /**
     * @Description 聚合查询(注意聚合的字段必须是数值类型，不然会报错)
     * @param tableName  index名
     * @param equalsCondition  等值条件
     * @param rangeCondition  范围条件
     * @param needAggrFields   需要被聚合的字段(当然，这里是不同的字段，不同的聚合)
     * @return
     */
    public static Map<String, Map<String, String>> aggregationQuery(String tableName, Map<String, Object> equalsCondition, Map<String, Object> rangeCondition, List<String> needAggrFields){
        Map<String, Map<String, String>> resultMap = new HashMap<>();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //等值查询
        if (null != equalsCondition && !equalsCondition.isEmpty()){
            for (Map.Entry<String ,Object> entry : equalsCondition.entrySet()){
                String key = entry.getKey();
                //由于我创建索引的时候使用字符串不分词使用的.keyword类型
                if (key.endsWith("_s")){
                    queryValueBuild(boolQueryBuilder, key + ".keyword", entry.getValue());
                }else{
                    queryValueBuild(boolQueryBuilder, key, entry.getValue());
                }
            }
        }
        //范围查询
        if (null != rangeCondition && !rangeCondition.isEmpty()){
            rangeValueBuild(boolQueryBuilder, rangeCondition);
        }
        sourceBuilder.query(boolQueryBuilder);

        //聚合。可以使用具体的Sum或者max等，但是stats的有总记录数，最值，总和以及平均值，更全面些
        if (!CollectionUtils.isEmpty(needAggrFields)){
            StatsAggregationBuilder statsAggr = null;
            for (String aggField : needAggrFields){
                //自定义要统计的字段的聚合名字为：字段名+_count_nums
                statsAggr = AggregationBuilders.stats(aggField + "_count_nums").field(aggField);
            }
            sourceBuilder.aggregation(statsAggr);
            SearchResponse response = executeSearch(tableName, sourceBuilder);
            for (String aggField : needAggrFields){
                ParsedStats stats = response.getAggregations().get(aggField + "_count_nums");
                Map<String, String> statMap= new HashMap<>();
                statMap.put("recordNum", String.valueOf(stats.getCount()));//总记录数
                statMap.put("sumValue", new DecimalFormat(DefineConstant.ZERP_MONEY).format(stats.getSum()));//求和
                statMap.put("avgValue", new DecimalFormat(DefineConstant.ZERP_MONEY).format(stats.getAvg()));//求平均值
                statMap.put("maxValue", new DecimalFormat(DefineConstant.ZERP_MONEY).format(stats.getMax()));//求最大值
                statMap.put("minValue", new DecimalFormat(DefineConstant.ZERP_MONEY).format(stats.getMin()));//求最小值
                resultMap.put(aggField + "_count_result", statMap);
            }
        }
        log.info("aggs result:{}",resultMap);
        return resultMap;
    }

    /**
     * @Description           根据某一字段进行分组，然后聚合
     * @param tableName       index名
     * @param equalsCondition 等值条件
     * @param rangeCondition  范围条件
     * @param needAggrField   需要被聚合的字段(此处就只聚合一个字段，多字段的分别聚合在上述中已实现)
     * @param groupByFields   分组的字段，一般业务涉及不到多字段分组，此处就实现复杂的，简单的也就更容易了
     * @return
     */
    public static Map<String, Object> groupAggregationQuery(String tableName, Map<String, Object> equalsCondition, Map<String, Object> rangeCondition, String needAggrField,List<String> groupByFields){
        Map<String, Object> result = new HashMap<>(4);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        //等值查询
        if (!CollectionUtils.isEmpty(equalsCondition)){
            for (Map.Entry<String ,Object> entry : equalsCondition.entrySet()){
                String key = entry.getKey();
                //由于我创建索引的时候使用字符串不分词使用的.keyword类型
                if (key.endsWith("_s")){
                    queryValueBuild(boolQueryBuilder, key + ".keyword", entry.getValue());
                }else{
                    queryValueBuild(boolQueryBuilder, key, entry.getValue());
                }
            }
        }
        //范围查询
        if (!CollectionUtils.isEmpty(rangeCondition)){
            rangeValueBuild(boolQueryBuilder, rangeCondition);
        }
        sourceBuilder.query(boolQueryBuilder);
        //由于这种递归是内部不停的嵌套的，所以组装查询语句的时候是由内而外的思想

        //聚合。可以使用具体的Sum或者max等，但是stats的有总记录数，最值，总和以及平均值，更全面些,自定义要统计的字段的聚合名字为：字段名+_count_nums
        StatsAggregationBuilder statsAggr = AggregationBuilders.stats(needAggrField + "_count_nums").field(needAggrField);

        //分组
        TermsAggregationBuilder termsGroupAggs = null;
        if (!CollectionUtils.isEmpty(groupByFields)){
            if (groupByFields.size() == 1){
                //此处groupByFields.get() + "_group"，是为了便于定义分组的名而自行定义的分组名，可以不同，便于区别，自己能用即可
                termsGroupAggs = AggregationBuilders.terms(groupByFields.get(0) + "_group")
                        .field(groupByFields.get(0).endsWith("_s") ? groupByFields.get(0) + ".keyword" : groupByFields.get(0)).subAggregation(statsAggr);
            } else{
                termsGroupAggs = AggregationBuilders.terms(groupByFields.get(0) + "_group")
                        .field(groupByFields.get(0).endsWith("_s") ? groupByFields.get(0) + ".keyword" : groupByFields.get(0));
                if (groupByFields.size() == 2){
                    termsGroupAggs.subAggregation(AggregationBuilders.terms(groupByFields.get(1) + "_group")
                            .field(groupByFields.get(1).endsWith("_s") ? groupByFields.get(1) + ".keyword" : groupByFields.get(1))
                            .subAggregation(statsAggr));
                }else {
                    List<TermsAggregationBuilder> termAggs = new ArrayList<>();
                    for (String groupField : groupByFields){
                        termAggs.add(AggregationBuilders.terms(groupField + "_group")
                        .field(groupField.endsWith("_s") ? groupField + ".keyword" : groupField));
                    }
                    TermsAggregationBuilder tempAggr = null;
                    for (int i = groupByFields.size()-1 ; i>1 ;i++){
                        if (null == tempAggr){
                            tempAggr = termAggs.get(i-1).subAggregation(termAggs.get(i).subAggregation(statsAggr));
                        }else {
                            tempAggr = termAggs.get(i-1).subAggregation(tempAggr);
                        }
                        if (i-2 == 0){
                            termsGroupAggs.subAggregation(tempAggr);
                        }
                    }
                }
            }
        }
        if (null == termsGroupAggs){
            sourceBuilder.aggregation(statsAggr);
        }else {
            sourceBuilder.aggregation(termsGroupAggs);
        }
        SearchResponse searchResponse = executeSearch(tableName, sourceBuilder);

        //处理返回的树形结构数据，转化成数组的平级表状数据
        List<Map<String, Object>> groupAggsMapList = new ArrayList<>();
        //定义子父级的关系
        List<Map<String,String>> fatherSonMaps = new ArrayList<>();
        if (!CollectionUtils.isEmpty(groupByFields)){
            ParsedStringTerms strTerms;
            if (groupByFields.size() == 1){
                strTerms = searchResponse.getAggregations().get(groupByFields.get(0) + "_group");
                for (Terms.Bucket bucket : strTerms.getBuckets()){
                    ParsedStats stats = bucket.getAggregations().get(needAggrField + "_count_nums");
                    Map<String, Object> statMap = new HashMap<>();
                    //返回的结果呈现，分组的字段以及字段对应的组值，还有对应组聚合的结果（需要啥取啥，此处取求和），自定义聚合结果的键名使用聚合字段加_count_result
                    statMap.put(groupByFields.get(0), bucket.getKeyAsString());
                    statMap.put(needAggrField + "_count_result", stats.getSumAsString());
                    groupAggsMapList.add(statMap);
                }
            }else {
                ParsedStringTerms strTermsFirst = searchResponse.getAggregations().get(groupByFields.get(0) + "_group");
                ParsedStringTerms strTermsTemp = null;
                List<ParsedStringTerms> strTermsTempList = new ArrayList<>();
                for (int i = 0; i < groupByFields.size(); i++) {
                    if (null == strTermsTemp){
                        strTermsTemp = strTermsFirst;
                    }else {
                        List buckets = strTermsTemp.getBuckets();
                        if (CollectionUtils.isEmpty(strTermsTempList)){
                            for (Object bucket : buckets){
                                strTermsTemp = ((Terms.Bucket) bucket).getAggregations().get(groupByFields.get(i) + "_group");
                                strTermsTempList.add(strTermsTemp);
                                /**获取子父级关系的集合**/
                                Map<String,String> fatherChildMap = new HashMap<>();
                                fatherChildMap.put("time",String.valueOf(i-1));//循环的次数
                                fatherChildMap.put(groupByFields.get(i-1),((Terms.Bucket)bucket).getKeyAsString());
                                fatherChildMap.put("childNum",String.valueOf(strTermsTemp.getBuckets().size()));
                                fatherSonMaps.add(fatherChildMap);
                                /**获取子父级关系的集合**/
                                if (i+1 == groupByFields.size()){
                                    for (Terms.Bucket statsBucket : strTermsTemp.getBuckets()){
                                        Map<String, Object> statMap = new HashMap<>();
                                        ParsedStats stats = statsBucket.getAggregations().get(needAggrField + "_count_nums");
                                        statMap.put(groupByFields.get(i), statsBucket.getKeyAsString());
                                        statMap.put(needAggrField + "_count_result", stats.getSumAsString());
                                        groupAggsMapList.add(statMap);
                                    }
                                }
                            }
                        }else {
                            List<ParsedStringTerms> tempStringTerms = new ArrayList<>();
                            tempStringTerms.addAll(strTermsTempList);
                            strTermsTempList.clear();
                            for (ParsedStringTerms stringTerms : tempStringTerms){
                                buckets = stringTerms.getBuckets();
                                for (Object bucket : buckets){
                                    strTermsTemp = ((Terms.Bucket) bucket).getAggregations().get(groupByFields.get(i) + "_group");
                                    strTermsTempList.add(strTermsTemp);
                                    /**获取子父级关系的集合**/
                                    Map<String,String> fatherChildMap = new HashMap<>();
                                    fatherChildMap.put("time",String.valueOf(i-1));//循环的次数
                                    fatherChildMap.put(groupByFields.get(i-1),((Terms.Bucket)bucket).getKeyAsString());
                                    fatherChildMap.put("childNum",String.valueOf(strTermsTemp.getBuckets().size()));
                                    fatherSonMaps.add(fatherChildMap);
                                    /**获取子父级关系的集合**/
                                    if (i+1 == groupByFields.size()){
                                        for (Terms.Bucket statsBucket : strTermsTemp.getBuckets()){
                                            Map<String, Object> statMap = new HashMap<>();
                                            ParsedStats stats = statsBucket.getAggregations().get(needAggrField + "_count_nums");
                                            statMap.put(groupByFields.get(i), statsBucket.getKeyAsString());
                                            statMap.put(needAggrField + "_count_result", stats.getSumAsString());
                                            groupAggsMapList.add(statMap);
                                        }
                                    }
                                }
                            }
                        }

                    }
                }
                //组装并平级化树
                List[] timeNodeArrays = new ArrayList[groupByFields.size()-1];
                for (int x = 0; x<groupByFields.size()-1; x++){
                    timeNodeArrays[x] = new ArrayList();
                    for (Map<String,String> fatherSonMap : fatherSonMaps){
                        if (String.valueOf(x).equals(fatherSonMap.get("time"))){
                            timeNodeArrays[x].add(fatherSonMap);
                        }
                    }
                }
                if (groupByFields.size()>2){
                    for (int n = 0; n<groupByFields.size() - 2; n++){
                        int tempNum = 0;
                        for (Object timeNodeMap : timeNodeArrays[n]){
                            int timeNodeChildNum  = Integer.valueOf(((Map<String,String>)timeNodeMap).get("childNum"));
                            for (int m = tempNum; m<tempNum+timeNodeChildNum; m++){
                                for (int j = 0;j<=n;j++){
                                    ((List<Map<String,String>>)timeNodeArrays[n+1]).get(m).put(groupByFields.get(j), (((Map<String, String>) timeNodeMap).get(groupByFields.get(j))));
                                }
                            }
                            tempNum += timeNodeChildNum;
                        }
                    }
                }
                //处理最后的结果，补齐对应的字段
                int tempNumLast = 0;
                for (Object timeNodeMap : timeNodeArrays[groupByFields.size()-2]){
                    int timeNodeChildNum  = Integer.valueOf(((Map<String,String>)timeNodeMap).get("childNum"));
                    for (int m = tempNumLast; m<tempNumLast+timeNodeChildNum; m++){
                        for (int j = 0;j<=groupByFields.size()-2;j++){
                            groupAggsMapList.get(m).put(groupByFields.get(j), (((Map<String, String>) timeNodeMap).get(groupByFields.get(j))));
                        }
                    }
                    tempNumLast += timeNodeChildNum;
                }
            }
        }
        result.put("resultAggs", groupAggsMapList);
        return result;
    }

    /**
     * @Description 游标查询
     * @param tableName
     * @param equalsCondition
     * @param rangeCondition
     * @param orderBy
     * @param pageSize  每页大小
     * @param scrollId  游标id
     * @return
     */
    public static Map<String, Object> queryEsByScroll(String tableName, Map<String, Object> equalsCondition, Map<String, Object> rangeCondition, List<String> orderBy, int pageSize, String scrollId){
        RestHighLevelClient client = EsClient.getInstance();
        List<Map<String, Object>> queryList = new ArrayList<>();
        Map<String, Object> result = new HashMap<>();
        Scroll scroll = new Scroll(TimeValue.timeValueMillis(1L));
        SearchResponse searchResponse = null;
        SearchHit[] searchHits = null;
        try {
            if (StringUtils.isEmpty(scrollId)){
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                //等值查询
                if (null != equalsCondition && !equalsCondition.isEmpty()){
                    for (Map.Entry<String ,Object> entry : equalsCondition.entrySet()){
                        String key = entry.getKey();
                        //由于我创建索引的时候使用字符串不分词使用的.keyword类型
                        if (key.endsWith("_s")){
                            queryValueBuild(boolQueryBuilder, key + ".keyword", entry.getValue());
                        }else{
                            queryValueBuild(boolQueryBuilder, key, entry.getValue());
                        }
                    }
                }
                //范围查询
                if (null != rangeCondition && !rangeCondition.isEmpty()){
                    rangeValueBuild(boolQueryBuilder, rangeCondition);
                }
                //排序
                if (!CollectionUtils.isEmpty(orderBy)){
                    buildSort(sourceBuilder, orderBy);
                }
                sourceBuilder.query(boolQueryBuilder);
                sourceBuilder.size(pageSize);
                SearchRequest searchRequest = new SearchRequest(tableName);
                searchRequest.types(DefineConstant.SEARCH_REQUEST_TYPE);
                searchRequest.source(sourceBuilder);
                searchRequest.scroll(scroll);
                searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            }else{
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            }
            if (null != searchResponse){
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
                //获取结果
                if (null != searchHits && searchHits.length > 0){
                    for (SearchHit hit : searchHits){
                        queryList .add(hit.getSourceAsMap());
                    }
                    log.info("searchStatus:" + searchResponse.status() + ", hitNum" + searchHits.length + ", searchTook:" + searchResponse.getTook());

                }
            }
        }catch (IOException ie){
            log.error(ie.getMessage());
        } finally {
            if (!StringUtils.isEmpty(scrollId)){
                if (null == searchHits || searchHits.length != pageSize){
                    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                    clearScrollRequest.addScrollId(scrollId);
                    try {
                        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
                        log.error("clearStatus:" + clearScrollResponse.status() + ", clearSucceed:" + clearScrollResponse.isSucceeded());
                    } catch (IOException e) {
                        log.error(e.getMessage());
                    }
                    scrollId = "";
                }
            }
        }
        result.put("pageList", queryList);
        result.put("scrollId",scrollId);
        return result;
    }

    /**
     * @Description 单条记录插入
     * @param indexName
     * @param sourceMap  传入的记录的键值对
     * @return
     */
    public static Map<String, Object> singleInsert (String indexName, Map<String, Object> sourceMap) {
        Map<String, Object> insertResult = new HashMap<>();
        RestHighLevelClient client = EsClient.getInstance();
        IndexRequest indexRequest = new IndexRequest(indexName, DefineConstant.SEARCH_REQUEST_TYPE);
        //es对应的索引文档的字段一定要有id_s，便于与_id相对应，且一定要逻辑控制id的唯一
        String id = StringUtils.isEmpty(sourceMap.get("id_s")) ? UUID.randomUUID().toString() : (String) sourceMap.get("id_s");
        indexRequest.id(id);
        indexRequest.source(sourceMap);
        //写入后立即刷新
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        try {
            String lineSeparator = System.lineSeparator();
            log.info(lineSeparator + "index:" + indexName + lineSeparator + "insert:"+ indexRequest.toString() + lineSeparator);
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            log.info("es insert response: {}", indexResponse.toString());
            insertResult.put("id", indexResponse.getId());
            insertResult.put("status", indexResponse.status().toString());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return insertResult;
    }

    /**
     * @Description  批量向es插入数据
     * @param indexName
     * @param sourceList
     * @return
     */
    public static Map<String, Object> batchInsert(String indexName, List<Map<String, Object>> sourceList){
        List<Map<String, Object>> insertResults = new ArrayList<>();
        Map<String, Object> result = new HashMap<>();
        RestHighLevelClient client = EsClient.getInstance();
        BulkRequest bulkRequest = new BulkRequest();
        for (Map<String, Object> source : sourceList){
            IndexRequest indexRequest = new IndexRequest(indexName,DefineConstant.SEARCH_REQUEST_TYPE);
            indexRequest.id(StringUtils.isEmpty(source.get("id_s")) ? UUID.randomUUID().toString() : (String) source.get("id_s"));
            indexRequest.source(source);
            bulkRequest.add(indexRequest);
        }
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        try {
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            BulkItemResponse[] itemResponses = bulkResponse.getItems();
            for (int i = 0;i < itemResponses.length; i++){
                BulkItemResponse response = itemResponses[i];
                Map<String, Object> item = new HashMap<>();
                item.put("id", response.getId());
                item.put("status", response.status().toString());
                item.put("failure", response.getFailureMessage());
                if (response.isFailed()){
                    IndexRequest ireq = (IndexRequest) bulkRequest.requests().get(i);
                    log.error("Failed while indexing to " + response.getIndex() + "type " +response.getType() + " " +
                            "request: [" + ireq + "]: [" + response.getFailureMessage() + "]");
                    item.put("isSuccess", "FAIL");
                }
                log.info("-------bulk---batchInsert-----", item);
                insertResults.add(item);
            }
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        result.put("insertResults", insertResults);
        return result;
    }

    /**
     * @Description 修改记录数据
     * @param indexName
     * @param updateMap
     * @return
     */
    public static Map<String, Object> updateEsRecord (String indexName, Map<String, Object> updateMap){
        Map<String, Object> updateResult = new HashMap<>();
        RestHighLevelClient client = EsClient.getInstance();
        //此时之前指定的id_s字段就起作用了
        if (StringUtils.isEmpty(updateMap.get("id_s"))){
            log.error("尚未指定修改的记录id");
            return null;
        }
        String id = (String) updateMap.get("id_s");
        UpdateRequest updateRequest = new UpdateRequest(indexName, DefineConstant.SEARCH_REQUEST_TYPE, id);

        updateRequest.doc(updateMap);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        try {
            String lineSeparator = System.lineSeparator();
            log.info(lineSeparator + "index:" + indexName + lineSeparator + "update:"+ updateRequest.toString() + lineSeparator);
            UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            log.info("es update response: {}", updateResponse.toString());
            updateResult.put("id", updateResponse.getId());
            updateResult.put("status", updateResponse.status().toString());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return updateResult;
    }

    /**
     * @Description 删除记录数据
     * @param indexName
     * @param id
     * @return
     */
    public static Map<String, Object> deleteEsRecord (String indexName, String id){
        Map<String, Object> deleteResult = new HashMap<>();
        RestHighLevelClient client = EsClient.getInstance();
        //此时之前指定的id_s字段就起作用了
        if (StringUtils.isEmpty(id)){
            log.error("尚未指定删除的记录id");
            return null;
        }
        DeleteRequest deleteRequest = new DeleteRequest(indexName, DefineConstant.SEARCH_REQUEST_TYPE, id);
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        try {
            String lineSeparator = System.lineSeparator();
            log.info(lineSeparator + "index:" + indexName + lineSeparator + "delete:"+ deleteRequest.toString() + lineSeparator);
            DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            log.info("es update response: {}", deleteResponse.toString());
            deleteResult.put("id", deleteResponse.getId());
            deleteResult.put("status", deleteResponse.status().toString());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return deleteResult;
    }
}

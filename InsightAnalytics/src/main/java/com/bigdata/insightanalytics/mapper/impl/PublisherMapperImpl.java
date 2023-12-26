package com.bigdata.insightanalytics.mapper.impl;

import com.bigdata.insightanalytics.bean.NameValue;
import com.bigdata.insightanalytics.mapper.PublisherMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j

@Repository
public class PublisherMapperImpl implements PublisherMapper {

//    public static void main(String[] args) {
//        String td = "2023-12-25";
//        LocalDate tdLd = LocalDate.parse(td);
//        LocalDate ydLd = tdLd.minusDays(1);
//        System.out.println(ydLd.toString());
//        PublisherMapperImpl publisherMapper = new PublisherMapperImpl();
//        System.out.println(publisherMapper.searchStateByItem("小米手机", "2023-12-25", "user_age"));
//    }

    @Autowired
    RestHighLevelClient esClient;
    private String dauIndexNamePrefix = "gmall_dau_info_";
    private String orderIndexNamePrefix = "gmall_order_wide_";

    /**
     *
     * @param itemName
     * @param date
     * @param field age => user_age gender => user_gender
     * @return
     */
    @Override
    public List<NameValue> searchStateByItem(String itemName, String date, String field) {

        ArrayList<NameValue> results = new ArrayList<>();
        String indexName = orderIndexNamePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        //查询
        //query
        MatchQueryBuilder matchQueryBuilder =
                QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);
        //group
        TermsAggregationBuilder termsAggregationBuilder =
                AggregationBuilders.terms("groupby" + field).field(field).size(100);
        SumAggregationBuilder sumAggregationBuilder =
                AggregationBuilders.sum("totalamount").field("split_total_amount");
        termsAggregationBuilder.subAggregation(sumAggregationBuilder);
        searchSourceBuilder.aggregation(termsAggregationBuilder);

        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest,RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTermsn = aggregations.get("groupby" + field);
            List<? extends Terms.Bucket> buckets = parsedTermsn.getBuckets();
            for(Terms.Bucket bucket : buckets){
                String key = bucket.getKeyAsString();
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedSum parsedSum = bucketAggregations.get("totalamount");
                double totalamount = parsedSum.getValue();
                results.add(new NameValue(key,totalamount));
            }
            return results;
        } catch(ElasticsearchStatusException ese){
            //es状态异常 index不存在
            if(ese.status() == RestStatus.NOT_FOUND){
                log.warn(indexName + " INEXISTENCE!!!");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to query ES ......");
        }
        return results;
    }

    @Override
    public Map<String, Object> searchDetailByItem(String date, String itemName, Integer from, Integer pageSize) {
        HashMap<String, Object> results = new HashMap<>();
        String indexName = orderIndexNamePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"create_time", "order_price", "province_name" , "sku_name",
                "sku_num", "total_amount","user_age","user_gender"}, null );
        //query
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("sku_name", itemName).operator(Operator.AND);
        searchSourceBuilder.query(matchQueryBuilder);
        //from
        searchSourceBuilder.from(from);
        //size
        searchSourceBuilder.size(pageSize);
        //highlight
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("sku_name");
        searchSourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long total = searchResponse.getHits().getTotalHits().value;
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            ArrayList<Map<String, Object>> sourceMaps = new ArrayList<>();
            for (SearchHit searchHit:searchHits){
                Map<String, Object> sourceMap = searchHit.getSourceAsMap();
                //highlight
                Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
                HighlightField highlightField = highlightFields.get("sku_name");
                Text[] fragments = highlightField.getFragments();
                String highLightSkuName = fragments[0].toString();
                //使用高亮结果覆盖原结果
                sourceMap.put("sku_name",highLightSkuName ) ;
                sourceMaps.add(sourceMap);
            }
            results.put("total",total);
            results.put("detail",sourceMaps);
            return results;
        } catch (ElasticsearchStatusException ese){
            if(ese.status() == RestStatus.NOT_FOUND){
                log.warn( indexName +" INEXISTENCE!!!");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to query ES ......");
        }
        return results;
    }


    @Override
    public Map<String, Object> searchDau(String td) {
        Map<String,Object> dauResult = new HashMap<>();
        Long dauTotal = searchDauTotal(td);
        dauResult.put("dauTotal",dauTotal);

        //今日分时
        Map<String, Long> dauTd = searchDauHr(td);
        dauResult.put("dauTd",dauTd);

        //昨日分时
        LocalDate tdLd = LocalDate.parse(td);
        LocalDate ydLd = tdLd.minusDays(1);
        Map<String, Long> dauYd = searchDauHr(ydLd.toString());
        dauResult.put("dauYd",dauYd);

        return dauResult;
    }

    public Map<String ,Long> searchDauHr(String td){
        HashMap<String, Long> dauHr = new HashMap<>();

        String indexName  = dauIndexNamePrefix + td ;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //不要明细
        searchSourceBuilder.size(0);
        //聚合
        TermsAggregationBuilder termsAggregationBuilder =
                AggregationBuilders.terms("groupbyhr").field("hr").size(24);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupbyhr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String hr = bucket.getKeyAsString();
                long hrTotal = bucket.getDocCount();

                dauHr.put(hr, hrTotal);
            }

            return dauHr ;

        } catch (ElasticsearchStatusException ese){
            if(ese.status() == RestStatus.NOT_FOUND){
                log.warn( indexName +" INEXISTENCE!!!");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to query ES ......");
        }

        return dauHr ;
    }
    public Long searchDauTotal(String td) {
        String indexName = dauIndexNamePrefix + td;
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long dauTotals = searchResponse.getHits().getTotalHits().value;
            return dauTotals;
        } catch(ElasticsearchStatusException ese){
            //es状态异常 index不存在
            if(ese.status() == RestStatus.NOT_FOUND){
                log.warn(indexName + " INEXISTENCE!!!");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to query ES ......");
        }
        return 0L;
    }
}


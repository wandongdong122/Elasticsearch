package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Map;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelSearchTest {


    private RestHighLevelClient client;

    @Test
    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.100.140:9200")
        ));
    }

    /*查询所有*/
    @Test
    void testMatchAll() throws IOException {
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        request.source().query(QueryBuilders.matchAllQuery());
        // 3.发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        // 4.解析结果
        handleResponse(search);
    }
    /*match查询*/
    @Test
    void testMatch() throws IOException {
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        // 3.发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        handleResponse(search);
    }
    /*精确查询*/
    @Test
    void testTerm() throws IOException {
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        request.source().query(QueryBuilders.termQuery("city","北京"));
        // 3.发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        handleResponse(search);
    }
    // 范围查询
    @Test
    void testRange() throws IOException {
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        request.source().query(QueryBuilders.rangeQuery("price").gte(100).lte(150));
        // 3.发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        handleResponse(search);
    }
    // 复合查询
    @Test
    void testBool() throws IOException {
        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("city","北京"));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").lte(500));
        request.source().query(boolQueryBuilder);
        // 3.发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        handleResponse(search);
    }

    // 分页查询
    @Test
    void testPageAndSort() throws IOException {
        // 页码，每页大小
        int page=2,size=10;

        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        request.source().query(QueryBuilders.matchAllQuery());
        request.source().sort("price", SortOrder.ASC);
        request.source().from((page-1)*size).size(size);
        // 3.发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        handleResponse(search);
    }

    // 高亮查询
    @Test
    void testHighLight() throws IOException {
        // 页码，每页大小
        int page=2,size=10;

        // 1.准备request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        request.source().query(QueryBuilders.termQuery("all","如家"));
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        // 3.发送请求
        SearchResponse search = client.search(request, RequestOptions.DEFAULT);
        handleResponseHighLight(search);
    }

    private void handleResponseHighLight(SearchResponse search) {
        // 4.解析结果
        SearchHits searchHits = search.getHits();
        // 4.1 查询的总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("总数据条数：" + total);
        // 4.2 查询的结果数组
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            // 4.3 得到source
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            // 处理高亮
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            if(!CollectionUtils.isEmpty(highlightFields)){
                // 获取高亮字段结果
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null){
                    // 取出高亮结果数组中的第一个，就是酒店名称
                    String name = highlightField.getFragments()[0].string();
                    hotelDoc.setName(name);
                }
            }
            // 4.4 打印
            System.out.println(hotelDoc);
        }
    }

    private void handleResponse(SearchResponse search) {
        // 4.解析结果
        SearchHits searchHits = search.getHits();
        // 4.1 查询的总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("总数据条数：" + total);
        // 4.2 查询的结果数组
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            // 4.3 得到source
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            // 4.4 打印
            System.out.println(hotelDoc);
        }
    }

    @Test
    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }


}

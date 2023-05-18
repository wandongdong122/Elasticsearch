package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            // 1.准备request
            SearchRequest request = new SearchRequest("hotel");
            // 2.准备DSL
            // 构建booleanQuery
            buildBasicQuery(params, request);
            // 排序
            String location = params.getLocation();
            if (location != null && !"".equals(location)) {
                request.source().sort(SortBuilders.geoDistanceSort("location", new GeoPoint(location)).order(SortOrder.ASC).unit(DistanceUnit.KILOMETERS));
            }
            // 分页
            request.source().from((params.getPage() - 1) * params.getSize()).size(params.getSize());
            // 3.发送请求
            SearchResponse search = client.search(request, RequestOptions.DEFAULT);
            return handleResponse(search);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        try {
            // 准备request对象
            SearchRequest request = new SearchRequest("hotel");
            // 准备DSL
            // 限定聚合范围
            buildBasicQuery(params, request);
            // 设置size
            request.source().size(0);
            // 聚合
            buildAggregation(request);
            // 发出请求
            SearchResponse responce = client.search(request, RequestOptions.DEFAULT);
            // 解析请求
            Map<String,List<String>> result = new HashMap<>();
            Aggregations aggregations = responce.getAggregations();
            // 根据名称，获取品牌结果
            List<String> brandList = getAggByName(aggregations,"brandAgg");
            result.put("brand",brandList);
            List<String> cityList = getAggByName(aggregations,"cityAgg");
            result.put("city",cityList);
            List<String> starList = getAggByName(aggregations,"starAgg");
            result.put("starName",starList);
            System.out.println(result);
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSuggestion(String key) {
        try {
            // 准备request
            SearchRequest request = new SearchRequest("hotel");
            // 准备DSL
            request.source().suggest(new SuggestBuilder().addSuggestion(
                    "suggestions",
                    SuggestBuilders.completionSuggestion("suggestion")
                            .skipDuplicates(true)
                            .prefix(key).size(10)
            ));
            // 发起请求
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            // 解析结果
            Suggest suggest = response.getSuggest();
            CompletionSuggestion suggestion =  suggest.getSuggestion("suggestions");
            List<CompletionSuggestion.Entry.Option> options = suggestion.getOptions();
            List<String> result = new ArrayList<>();
            for (CompletionSuggestion.Entry.Option option : options) {
                String text = option.getText().toString();
                result.add(text);
//                System.out.println(text);
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertById(Long id) {
        // 查询数据库
        Hotel hotel = this.getById(id);
        HotelDoc hotelDoc = new HotelDoc(hotel);
        try {
            // 准备request
            IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
            // 准备DSL
            request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            // 发送请求
            client.index(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void deleteById(Long id) {
        try {
            // 准备request
            DeleteRequest request = new DeleteRequest("hotel",id.toString());
            // 发送请求
            this.client.delete(request,RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getAggByName(Aggregations aggregations,String aggName) {
        List<String> brandList = new ArrayList<>();
        Terms brandTerms = aggregations.get(aggName);
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            // 获取key
            String key = bucket.getKeyAsString();
            brandList.add(key);
//            System.out.println(key);
        }
        return brandList;
    }

    private void buildAggregation(SearchRequest request) {
        request.source().aggregation(AggregationBuilders.terms("brandAgg").field("brand").size(10));
        request.source().aggregation(AggregationBuilders.terms("cityAgg").field("city").size(10));
        request.source().aggregation(AggregationBuilders.terms("starAgg").field("starName").size(10));
    }

    private void buildBasicQuery(RequestParams params, SearchRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 关键字搜索
        String key = params.getKey();
        if (key == null || "".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("all", key));
        }
        // 城市条件
        if (params.getCity() != null && !"".equals(params.getCity())) {
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        // 品牌条件
        if (params.getBrand() != null && !"".equals(params.getBrand())) {
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        // 星级条件
        if (params.getStarName() != null && !"".equals(params.getStarName())) {
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        // 价格条件
        if (params.getMinPrice() != null && !"".equals(params.getMinPrice()) && params.getMaxPrice() != null && !"".equals(params.getMaxPrice())) {
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(params.getMinPrice()).lte(params.getMaxPrice()));
        }

        // 算分控制
        FunctionScoreQueryBuilder functionScoreQuery =
                QueryBuilders.functionScoreQuery(
                        // 原始查询
                        boolQuery,
                        // function score的数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // 其中的一个function score元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        // 过滤条件
                                        QueryBuilders.termQuery("isAD", true),
                                        // 算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });
        // 查询
        request.source().query(functionScoreQuery);
    }

    private PageResult handleResponse(SearchResponse search) {
        // 4.解析结果
        SearchHits searchHits = search.getHits();
        // 4.1 查询的总条数
        long total = searchHits.getTotalHits().value;
//        System.out.println("总数据条数：" + total);
        // 4.2 查询的结果数组
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : hits) {
            // 4.3 得到source
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0) {
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
            // 4.4 封装
            hotels.add(hotelDoc);
        }
        return new PageResult(total, hotels);
    }

}

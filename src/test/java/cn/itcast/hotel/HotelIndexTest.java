package cn.itcast.hotel;

import cn.itcast.hotel.service.IHotelService;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelIndexTest {


    private RestHighLevelClient client;

    @Test
    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.100.140:9200")
        ));
    }

    /*创建索引库*/
    @Test
    void createHotelIndex() throws IOException {
        // 1.创建Request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        // 2.准备请求的参数：DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        // 3.发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    /*删除索引库*/
    @Test
    void testDeleteHotelIndex() throws IOException {
        // 创建request对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        // 发起请求
        client.indices().delete(request,RequestOptions.DEFAULT);
    }

    /*判断索引库是否存在*/
    @Test
    void testExistsHotelIndex() throws IOException {
        // 1.创建Request对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        // 2.发起请求
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        // 3.输出
        System.out.println(exists);
    }

    @Test
    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }


}

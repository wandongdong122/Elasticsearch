package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
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
import java.util.List;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private IHotelService iHotelService;

    private RestHighLevelClient client;

    @Test
    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.100.140:9200")
        ));
    }

    /*添加文本*/
    @Test
    void testAddDocument() throws IOException {
        // 根据id查询酒店数据
        Hotel hotel = iHotelService.getById(61083L);
        // 转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);

        // 1.准备Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        // 2.准备JSON文档
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        // 3.发送请求
        client.index(request,RequestOptions.DEFAULT);
    }

    /*根据id查询*/
    @Test
    void testGetDocument() throws IOException {
        // 1.准备Request对象
        GetRequest request = new GetRequest("hotel", "61083");
        // 2.发送请求
        GetResponse re = client.get(request, RequestOptions.DEFAULT);
        // 3.解析结果
        String json = re.getSourceAsString();

        System.out.println(json);
    }

    /*根据id修改酒店数据*/
    @Test
    void testUpdateDocumentById() throws IOException {
        // 1.创建request对象
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        // 2.准备数据，每2个参数为一对 key value
        request.doc(
                "price","952",
                "starName","四钻"
        );
        // 3.更新文档
        this.client.update(request,RequestOptions.DEFAULT);
    }

    /*删除文档*/
    @Test
    void testDeleteDocumentById() throws IOException {
        // 1.创建request对象
        DeleteRequest request = new DeleteRequest("hotel", "61083");
        // 2.删除文档
        client.delete(request,RequestOptions.DEFAULT);
    }

    /*批量添加数据*/
    @Test
    void testBulkRequest() throws IOException {
        // 批量查询酒店数据
        List<Hotel> list = iHotelService.list();

        // 1.创建request
        BulkRequest request = new BulkRequest();
        // 2.转换参数，添加多个新增的Request
        // 转换为文档类型hotelDoc
        for (Hotel hotel : list) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel").id(hotelDoc.getId().toString()).source(JSON.toJSONString(hotelDoc),XContentType.JSON));
        }
        // 3.发送请求
        client.bulk(request,RequestOptions.DEFAULT);
    }

    @Test
    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }


}

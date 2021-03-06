package mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.log4j.Log4j;
import mongo.conn.MongoDBConnection;
import mongo.dto.AccessLogDTO;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.Document;

import java.io.IOException;
import java.util.Map;

@Log4j
public class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
    //MongoDB 객체
    private MongoDatabase mongodb;
    // 저장될 MongoDB 컬렉션명
    private String colNm = "ACCESS_LOG";

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
        //mongodb 객체 생성을 통해 접속
        this.mongodb = new MongoDBConnection().getMongoDB();
        //컬렉션을 생성할지 결정할 변수(true: 생성, / false : 미생성)
        boolean create = true;

        //컬렉션이 존재하는지 체크
        //spring-data-mongo는 컬랙션 존재 여부 체크 함수를 제공하지만,
        //MongoDriver 라이브러리는 제공하지 않아, 컬랙션 존재 여부 체크 함수를 만들어서 사용해야함
        for(String s: this.mongodb.listCollectionNames()) {
            // 컬렉션 존재하면 생성하지 못하도록 create변수를 false 변경함
            if (this.colNm.equals(s)) {
                create = false;
                break;
            }
        }
        if(create) { // 컬렉션이 생성안되었으면 생성하기
            //컬렉션 생성
            this.mongodb.createCollection(this.colNm);

        }
    }
    @Override
    public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
        // 분석할 파일의 한 줄 값
        String[] fields = value.toString().split(" ");
        String ip = fields[0]; //ip추출
        String reqTime = ""; //요청일시

        if(fields[3].length() > 2) { //일부 데이터가 요청일시 값이 누락된 경우가 있어 요청일시 같이 존재하는지 체크
            reqTime = fields[3].substring(1); //요청일시 추출
        }
        String reqMethod = "";
        if (fields[5].length() > 2) { //일부 데이터가 요청일시 값이 누락된 경우가 있어 요청일시 같이 존재하는지 체크
            reqMethod = fields[5].substring(1);
        }
        String reqURI = fields[6];

        log.info("ip: " + ip);
        log.info("reqTime: " + reqTime);
        log.info("reqMethod: " + reqMethod);
        log.info("reqURI: " + reqURI);

        //추출한 정보를 저장하기 위해 pDTO 선언후, 값 저장
        //IP는 키로 사용되기 때문에 DTO에 저장하지 않음
        AccessLogDTO pDTO = new AccessLogDTO();
        pDTO.setIp(ip);
        pDTO.setReqTime(reqTime); // 요청 시간
        pDTO.setReqMethod(reqMethod); // 요청 방법
        pDTO.setReqURI(reqURI); //요청 URI

        //저장할 Mongodb 컬렉션 가져오기
        MongoCollection<Document> col = this.mongodb.getCollection(this.colNm);

        // MongoDB에 저장하기
        col.insertOne(new Document(new ObjectMapper().convertValue(pDTO, Map.class)));
        col = null;
    }
    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException{
        //MongoDB 접속 해제
        this.mongodb = null;
    }

}

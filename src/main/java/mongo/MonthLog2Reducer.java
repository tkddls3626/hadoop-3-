package mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import mongo.conn.MongoDBConnection;
import mongo.dto.AccessLogDTO;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.Document;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;




/**
 * 리듀스 역할을 수행하기 위해서 Reduce 자바파일을 상속받아야함
 * Reducer 파일의 앞의 2개 데이터타입은 suffle and sort에 보낸 데이터의 키과 값의 데이터타입
 * 보통 mapper에서 보낸 데이터타입과 동일함
 * Reducer 파일의 뒤의 2개 데이터타입은 결과핑ㄹ 생성에 사용될 키와 값
 */
@Slf4j
public class MonthLog2Reducer extends Reducer<Text, Text,Text, IntWritable> {

    private MongoDatabase mongodb;

    private Map<String,String> months = new HashMap<>();


    public MonthLog2Reducer() {

        this.months.put("Jan","LOG_01");
        this.months.put("Feb","LOG_02");
        this.months.put("Mar","LOG_03");
        this.months.put("Apr","LOG_04");
        this.months.put("May","LOG_05");
        this.months.put("Jun","LOG_06");
        this.months.put("Jul","LOG_07");
        this.months.put("Aug","LOG_08");
        this.months.put("Sep","LOG_09");
        this.months.put("Oct","LOG_10");
        this.months.put("Nov","LOG_11");
        this.months.put("Dec","LOG_12");

    }
    @Override
    protected void setup(Reducer<Text, Text,Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        //MongoDB 객체 생성을 통해 접속
        this.mongodb = new MongoDBConnection().getMongoDB();

        for(String month : this.months.keySet()) {

            String colNm =this.months.get(month);

            boolean create = true;

            //컬렉션이 존재하는지 체크
            //Spring-data-mongo는 컬렉션 존재 여부 체크함수를 제공하지만,
            //MongoDriver 라이브러리는 제공하지 않아 컬렉션 존재여부체크함수를 만들어서 사용해야함
            for (String s : this.mongodb.listCollectionNames()) {

                //컬렉션 존재하면 생성하지 못하도록 create변수를 false로 변경함
                if(colNm.equals(s)){
                    create = false;
                    break;
                }
            }
            if(create) {
                //컬렉션이 생성안되있으면 생성하기
                this.mongodb.createCollection(colNm);
            }
        }

    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

        List<Document> pList = new ArrayList<>();

        String colNm = this.months.get(key.toString());

        log.info("key : " + key);
        log.info("colNm : " + colNm);

        MongoCollection<Document> col = mongodb.getCollection(colNm);

        for(Text value : values) {

            String json = value.toString();

            AccessLogDTO pDTO = new ObjectMapper().readValue(json, AccessLogDTO.class);

            Document doc = new Document(new ObjectMapper().convertValue(pDTO, Map.class));

            pList.add(doc);

            doc = null;
        }
        int pListSize = pList.size();
        int blockSize = 50000;

        for(int i=0; i < pListSize; i+= blockSize) {
            col.insertMany(new ArrayList<>(pList.subList(i,Math.min(i + blockSize,pListSize))));
        }

        col = null;
        pList = null;
    }
    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        this.mongodb = null;
    }


}
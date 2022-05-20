package mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j;
import mongo.dto.AccessLogDTO;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
@Log4j
public class MonthLog2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    //access_log파일로부터 추출될 월 정보가 제대로 수집되었는지 확인하기위해 만듬
    List<String> months = null;
    public MonthLog2Mapper() {
        //이 변수는 만들지 않아도 되자만, 추출한값이 정상인지 체크
        // 추출한 값이 months 변수에 존재하는 값이 맞는지 체크
        this.months = Arrays.asList("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec");
    }
    @Override
    public void map (LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        //부넉할 파일의 한 줄 값
        String[] fields = value.toString().split(" ");
        String ip = fields[0];
        String reqTime = "";

        if(fields[3].length() > 2) {
            reqTime = fields[3].substring(1);
                }
        String reqMethod = "";
        if (fields[5].length() > 2) {
            reqMethod = fields[5].substring(1);
            }
        String reqURI = fields[6];
        String reqMonth = "";
        String[] dtFields = fields[3].split("/");
        if(dtFields.length > 1) {
            reqMonth = dtFields[1];
        }
        log.info("ip: " + ip);
        log.info("reqTime: " + reqTime);
        log.info("reqMethod: " + reqMethod);
        log.info("reqURI: " + reqURI);
        log.info("reqMonth :" + reqMonth);

        //추출한 정보를 저장하기 위해 pDTO 선언후, 값 저장
        //IP는 키로 사용되기 때문에 DTO에 저장하지 않음
        AccessLogDTO pDTO = new AccessLogDTO();
        pDTO.setIp(ip);
        pDTO.setReqTime(reqTime); // 요청 시간
        pDTO.setReqMethod(reqMethod); // 요청 방법
        pDTO.setReqURI(reqURI); //요청 URI

        String json = new ObjectMapper().writeValueAsString(pDTO);

        if(months.contains(reqMonth)) {
            context.write(new Text(reqMonth),new Text(json));
        }
    }
}

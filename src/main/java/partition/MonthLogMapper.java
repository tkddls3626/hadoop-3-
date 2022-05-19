package partition;

import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
@Log4j
public class MonthLogMapper extends Mapper<LongWritable, Text, Text, Text> {
    //access_log파일로부터 추출될 월 정보가 제대로 수집되었는지 확인하기위해 만듬
    List<String> months = null;
    public MonthLogMapper() {
        //이 변수는 만들지 않아도 되자만, 추출한값이 정상인지 체크
        // 추출한 값이 months 변수에 존재하는 값이 맞는지 체크
    this.months = Arrays.asList("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec");
    }
    @Override
    public void map (LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
     //부넉할 파일의 한 줄 값
     String[] fields = value.toString().split(" ");
     if(fields.length > 0) {
         String ip = fields[0];

         String[] dfFields = fields[3].split("/");

         if (dfFields.length > 1) {
             String theMonth = dfFields[1];
             if(months.contains(theMonth)) {
                 context.write(new Text(ip), new Text(theMonth));
                 log.info("theMonth :"+ theMonth);
             }
         }
     }


    }
}

package partition;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
@Log4j
public class TimeLogMapper extends Mapper<LongWritable, Text, Text, Text>{
    //access_log파일로부터 추출될 월 정보가 제대로 수집되었는지 확인하기위해 만듬
    List<String> times = null;
    public TimeLogMapper() {
        //이 변수는 만들지 않아도 되자만, 추출한값이 정상인지 체크
        // 추출한 값이 months 변수에 존재하는 값이 맞는지 체크
        this.times = Arrays.asList("00","01","02","03","04","05","06","07","08","09","10","11",
        "12","13","14","15","16","17","18","19","20","21","22","23");
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
                String time = dfFields[2].substring(5, 7);
                if(times.contains(time)) {
                    context.write(new Text(ip), new Text(time));
                    log.info("theMonth :"+ time);
                }
            }
        }


    }
}

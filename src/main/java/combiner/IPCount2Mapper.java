package combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class IPCount2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //분석할 파일의 한 줄 값
        String line = value.toString();

        String ip = ""; // 로그부터 추출된 ip
        int forCnt = 0; // 반복 횟수
        //단어 빈도수 구현은 단어가 아닌 것을 기준으로 단어로 구분함
        //분석할 한 줄 내용을 단어가 아닌 것으로 나눔
        // word 변수는 단어가 저장됨
        for(String field : line.split("\\W+")) {
            if (field.length() > 0) {
                forCnt++; // 반복횟수 증가
                ip += (field + "."); // ip값 저장함

                if (forCnt == 4) {
                    ip = ip.substring(0, ip.length()-1);
                    context.write(new Text(ip), new IntWritable(1));
                }
            }
        }
    }
}

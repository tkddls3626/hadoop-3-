package partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TimeLogReducer extends Reducer<Text, Text, Text, IntWritable> {
    //부모 Reducer 자바 파일에 작성된 reducer 함수를 덮어쓰기
    //Reducer 객체는 기본이 1개에 1개쓰레드 사용
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //ip별 빈도수를 계산하기 위한 변수
        int ipCount = 0;
        //Suffle and Sort로 인해 단어별로 데이터들의 값들이 list구조로 저장됨
        //파티셔너를 통해 같은 월에 해당되는 ip만 넘어옴
        //배열의 수는 ip호출이며, 배열의 수를 계산함
        for(Text value : values) {
            ipCount ++;
        }
        //분석 결과 파일에 데이터 저장하기
        context.write(key, new IntWritable(ipCount));
    }
}

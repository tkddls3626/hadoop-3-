package partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

//맵에서 suffle and sort로 데이터를 전달할때 실행됨
public class MonthLogPartitioner extends Partitioner<Text, Text> {
    //월 리듀서를 매칩하기 위한 객체
    //1월은 0번 리듀스 , 2월은 1번 리듀스 .. 이런 형태로 매칭
    Map<String, Integer> months = new HashMap<>();

    //생성자에 매칭 정보 저장
    public MonthLogPartitioner() {
        this.months.put("Jan", 0);
        this.months.put("Feb", 1);
        this.months.put("Mar", 2);
        this.months.put("Apr", 3);
        this.months.put("May", 4);
        this.months.put("Jun", 5);
        this.months.put("Jul", 6);
        this.months.put("Aug", 7);
        this.months.put("Sep", 8);
        this.months.put("Oct", 9);
        this.months.put("Nov", 10);
        this.months.put("Dec", 11);
    }
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        return months.get(value.toString());
    }
}


package partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class TimeLogPartitioner extends Partitioner<Text, Text> {
    //월 리듀서를 매칩하기 위한 객체
    // ex) 0시는 0번 리듀스, 1시는 1번 리듀스가 데이터 매칭
    Map<String, Integer> times = new HashMap<>();

    //생성자에 매칭 정보 저장
    public TimeLogPartitioner() {
        this.times.put("00", 0);
        this.times.put("01", 1);
        this.times.put("02", 2);
        this.times.put("03", 3);
        this.times.put("04", 4);
        this.times.put("05", 5);
        this.times.put("06", 6);
        this.times.put("07", 7);
        this.times.put("08", 8);
        this.times.put("09", 9);
        this.times.put("10", 10);
        this.times.put("11", 11);
        this.times.put("12", 12);
        this.times.put("13", 13);
        this.times.put("14", 14);
        this.times.put("15", 15);
        this.times.put("16", 16);
        this.times.put("17", 17);
        this.times.put("18", 18);
        this.times.put("19", 19);
        this.times.put("20", 20);
        this.times.put("21", 21);
        this.times.put("22", 22);
        this.times.put("23", 23);
    }
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        return times.get(value.toString());
    }
}

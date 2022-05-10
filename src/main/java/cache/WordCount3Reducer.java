package cache;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 리듀스 역활을 수행하기 위해서는 Reducer 자바 파일을 상속받아야함
 * Reducer 파일의 앞의 2개 데이터타입(Text, IntWirtable)은 Suffle and Sort애ㅔ 보내
 * 보통 Mapper에서 보낸 데이터타입과 동일함
 * Reducer 파일의 뒤에 2개 데이터 타입(Text, IntWritable)은 결과 파일 생성에 사용될 키와 값
 */

public class WordCount3Reducer  extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 부모 Reducer 자바 파일에 작성된 reduce 함수를 덮어쓰기 수행
     * reduce 함수는 suffle and Sort로 처리된 데이터마다 실행
     * 처리된 데이터의 수가 500개라면, reduce 함수는 500번 실행
     *
     * Reducer 객체는 기본값이 1개로 1개의 스레드로 처리함
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //단어별 빈도수를 계산하기 위한 변수
        int wordCount = 0;

        //suffle and Sort로 인해 단어별로 데이터들의 값들이 List 구조로 저장됨
        // 유상인 : {1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)
        // 모든 값은 1이이게 모두 더하기 해도 됨
        for (IntWritable value : values){
            //값을 모두 더하기
            wordCount += value.get();
        }
        //분석 결과 파일에 데이터 저장하기
        context.write(key, new IntWritable(wordCount));
    }

}

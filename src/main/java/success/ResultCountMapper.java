package success;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@Log4j
public class ResultCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //맵리듀스 잡 이름
    String appName = "";
    //URL전송 성공 여부 코드값
    //성공 : 200 / 실패: 200이 아닌값
    String resultCode = "";

    /**
     * Driver 파일에서 정의한 변수값을 가져와 map함수에 적응하기 위해 Sstup함수 구현
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException{
        super.setup(context);

        //사용자 정의 정보 가져오기
        Configuration conf = context.getConfiguration();

        // Driver에서 정의된 맵리듀스 잡 이름 가져오기
        this.appName = conf.get("AppName");
        this.resultCode = conf.get("resultCode", "200");
        //Driver에서 정의된 환경설정값 가져오기
        // Driver에서 정의된 환경설정값이 없다면, 200으로 설정함
        log.info("["+this.appName + "] 난 map함수 실행하기 전에 1번만 실행되는 setup함수이다.");
    }
    @Override
    protected  void cleanup(Mapper<LongWritable, Text, Text, IntWritable>. Context context)throws  IOException, InterruptedException {
        super.cleanup(context);
        log.info("[" + this.appName+"]난 에러나도 무조건 실행되는 cleanup함수이다.");
    }
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // 분석할 파일의 한 줄 값
        String line = value.toString();
        //단어 별로 나누기
        String[] arr = line.split("\\W+");
        //전송 결과코드가 존재하는 위치
        //로그의 마지막에서 2번째에 성공코드 값이 존재함
        int pos = arr.length -2;
        //전송 결과코드
        String result = arr[pos];

        log.info("["+ this.appName + "]" + result);

        //Driver 파일에서 정의한 코드값과 로그의 코드값이 일치한다면
        if (resultCode.equals(result)) {
            context.write(new Text(result), new IntWritable(1));
        }
    }
}

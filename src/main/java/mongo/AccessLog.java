package mongo;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@Log4j
public class AccessLog extends Configuration implements Tool {
    //맵리듀스 실행 함수
    public static void main(String[] args)throws Exception {
        // 피라미터는 분석할 파일(폴더)과 분석 결과가 저장될 파일(폴더)로 2개를 받음
        if (args.length != 2) {
            log.info("분석할 파일(폴더)과 분석 결과가 저장될 (폴더)를 입력해야 합니다.");
            System.exit(-1);
        }

        int exitCode = ToolRunner.run(new AccessLog(), args);

        System.exit(exitCode);
    }
    @Override
    public void setConf(Configuration configuration) {

        configuration.set("AppName", "AccessLog MongoDB Test");
    }
    @Override
    public Configuration getConf() {
        //맵리듀스 전체에 적용될 변수를 정의할때 사용
        Configuration conf = new Configuration();
        //변수 정의
        this.setConf(conf);

        return conf;
    }
    @Override
    public int run(String[] args) throws Exception{

        Configuration conf = this.getConf();
        String appName = conf.get("AppName");

        log.info("appName : " + appName);


        //호출이 발생하면, 메모리에 저장하여 캐시 처리 수행
        // 하둡분산파일시스템에 저장된 파일만 가능함
        String cacheFile = "/access_log";

        //맵리듀스 실핼을 위한 잡 객체를 가져오기
        //하둡이 실행되면, 기본적으러 잡 객체를 메모리에 올림
        Job job = Job.getInstance(conf);

        //호출이 발생하면, 메모리에 저장하여 캐시 처리 수행
        // 하둡분산파일시스템에 저장된 파일만 가능함
        job.addCacheFile(new Path(cacheFile).toUri());

        //맵리듀스 잡이 시작되는 main함수가 존재하는 파일 설정
        job.setJarByClass(AccessLog.class);

        //맵리듀스 잡 이름 설정, 리소스 매니저 등 맵리듀스 실행 결과 및 로그 확인할 때 편리함
        job.setJobName(appName);

        //분석할 폴더(파일) -- 첫번째 파라미터
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        //분석 결과가 저장되는 폴더(파일) -- 두번째 파라미터
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //맵리듀스의 맵 역활을 수행하는 Mapper 자바 파일 설정
        job.setMapperClass(AccessLogMapper.class);

        //1년을 12개월로 리듀스 12을 생성하여 1개당 1개월 처리
        // ex) 1월은 0번 리듀스, 2월은 1번 리듀스가 데이터 처리함
        job.setNumReduceTasks(0);

        //맵리듀스 실행
        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);

    }
}

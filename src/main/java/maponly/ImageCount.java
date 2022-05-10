package maponly;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@Log4j
    public class ImageCount extends Configuration implements Tool {

        public static void main(String[] args) throws Exception{

            if(args.length != 1) {
                log.info("분석결과가 저장될 폴더를 입력해야됩니다");
                System.exit(-1);
            }
            int exitCode = ToolRunner.run(new ImageCount(), args);

            System.exit(exitCode);
        }
    @Override
    public void setConf(Configuration configuration) {
        configuration.set("AppName","Map only Test");
    }

    @Override
    public Configuration getConf() {

        Configuration conf = new Configuration();

        this.setConf(conf);

        return conf;
    }

        @Override
        public int run(String[] args) throws Exception {

            //캐시 메모리에 올릴 분석파일
            String analysisFile = "/access_log";

            Configuration conf = this.getConf();
            String appName = conf.get("AppName");

            System.out.println("appName : " + appName);

            // 맵리듀스 실행을 위한 잡 객체를 가져오기
            //하둡이 실행되며 기본적으로 잡 객체를 메모리에 올림
            Job job = Job.getInstance(conf);

            //호출이 발생하면, 메모리에 저장하여 캐시처리 수행
            //하둡분산파일시스템에 저장된 파일만 가능함
            job.addCacheFile(new Path(analysisFile).toUri());

            //맵리듀스 잡이 시작되는 main함수가 존재하는 파일설정
            job.setJarByClass(ImageCount.class);

            //맵리듀스 잡 이름설정 리소스매니저등 맵리듀스 실행결과 및 로그 확인할때 편리함
            job.setJobName(appName);



            //분석할 폴더 ==첫번째 파라미터
            FileInputFormat.setInputPaths(job, new Path(analysisFile));

            //분석 결과가 저장되는 폴더 -- 두번째 파라미터
            FileOutputFormat.setOutputPath(job, new Path(args[0]));

            //맵리듀스의 맵역할을 수행하는 Mapper 자바 파일설정
            job.setMapperClass(ImageCountMapper.class);

            //리듀서 객체를 생성하지 못하도록 객체의 수를 0으로정의
            job.setNumReduceTasks(0);

            // 맵리듀스 실행
            boolean success = job.waitForCompletion(true);

            if (success) {

                //맵리듀스의 Counter는 맵리듀스 실행된 결과에 대한 보고를 위해 활용하는 영역
                //맵 분석 결과에 대한 결과를 COUNTER 영역에 저장

                //JPG를 요청한 URL수
                long jpg = job.getCounters().findCounter("imageCount","jpg").getValue();

                //gif를 요청한 URL수
                long gif = job.getCounters().findCounter("imageCount","gif").getValue();

                //jpg와 gif를 제외한 요요청한 URL수
                long other = job.getCounters().findCounter("imageCount","other").getValue();

                log.info("jpg : " + jpg);
                log.info("gif : " + gif);
                log.info("other : " + other);

                return 0;
            }else {
                return 1;
            }


        }

    }

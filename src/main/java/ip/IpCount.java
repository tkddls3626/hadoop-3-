package ip;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 맵리듀스를 실행하기 위한 main 함수가 존재하는 자바 파일
 * 드라이버 파일로 부름
 */
public class IpCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("분석할 폴더 및 분석결과가 저장될 폴더를 입력해야 합니다.");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(IpCount.class);
        job.setJobName("IP count");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setReducerClass(IpCountReducer.class);
        job.setMapperClass(IpCountMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}

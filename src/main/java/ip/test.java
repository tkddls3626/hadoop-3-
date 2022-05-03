package ip;

public class test {
    public static void main(String[] args) {

        String line = "10.153.239.5 - - [28/Jul/2009:18:11:25 -0700] \"GET /assets/js/the-associates.js HTTP/1.1\" 200 4492";


        int index = line.indexOf(" ");
        String ip = line.substring(0, index);
        String ipRegex = "(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])";
        System.out.println("ip : " + ip);
        System.out.println(ip.matches(ipRegex));
    }
}

package flinkdemo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;

/**
 * Author wenBin
 * Date 2019/6/12 14:18
 * Version 1.0
 * flink读取本地文件进行简单过滤
 */
public class FilterMovies {


    public static void main(String[] args) throws Exception {

        // create Fink execution environment
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // static code
        DataSource<String> textFile = environment.readTextFile("C:\\Users\\Administrator\\Desktop\\flinktest.txt");

        FilterOperator<String> filterResult = textFile.map(new MapFunction<String, String>() {
            public String map(String s) throws Exception {

                return s;
            }
        })
                // filter
                .filter(new FilterFunction<String>() {
                    public boolean filter(String s) throws Exception {
                        return s.length() % 2 == 0;
                    }
                });

        filterResult.print();

        filterResult.writeAsText("C:\\Users\\Administrator\\Desktop\\outputFlink");
        // Start Flink application
        environment.execute();
    }

}

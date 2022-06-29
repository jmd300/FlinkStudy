package chapter02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author guguowei 2022/6/21 周二
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件读取数据按行读取(存储的元素就是每行的文本)，转换数据格式
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = env.readTextFile("/Users/guguowei/github/FlinkStudy/src/main/resources/WordCount.txt")
            .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                String[] words = line.split(" ");
                for (String word : words) {out.collect(Tuple2.of(word, 1L));}
            })
            .returns(Types.TUPLE(Types.STRING, Types.LONG)); //当 Lambda 表达式使用 Java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息

        // 3. 按照 word 进行分组，分组内聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOne.groupBy(0).sum(1);

        // 4. 打印结果
        sum.print();
    }
}

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
//import Java.lang.Integer
//import Java.lang.Integer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by instructor on 06.11.16.
 */
public class MatrixMult extends Configured implements Tool{

    /**
     * Маппер. На вход получаем сплит (фрагмент файла размером с HDFS-блок).
     * На выходе - множество пар (слово, 1).
     */
    public int dim1;
    public int dim2;

    public static class NonSplittableTextInputFormat extends FileInputFormat<LongWritable, Text> {
        public NonSplittableTextInputFormat(){
            super();
        }

        @Override
        public RecordReader<LongWritable, Text>
        createRecordReader(InputSplit split,
                           TaskAttemptContext context) {
// By default,textinputformat.record.delimiter = ‘/n’(Set in configuration file)
            String delimiter = context.getConfiguration().get(
                    "textinputformat.record.delimiter");
            byte[] recordDelimiterBytes = null;
            if (null != delimiter)
                recordDelimiterBytes = delimiter.getBytes();
            return new LineRecordReader(recordDelimiterBytes);
        }

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }
    }

    public static class WordMapper extends Mapper<LongWritable, Text, Key, IntWritable>{
        //переменная static final т.к. будет использоваться во всех мапперах без изменения
        private Key key = new Key();
        //здесь static не пишем т.к. значение переменной будет менятся в кажом маппере, а мапперы работаю параллельно
        private IntWritable value = new IntWritable(0);

        /**
         * Мап-функция. На вход подаётся строка данных, на выходе - множество пар (слово, 1).
         * (Чтобы разбивка шла не по строкам, нужно изменть разделитель в конфигурации textinputformat.record.delimiter)
         * @param offset номер строки, начиная от начала входного сплита (не будет использован ни в этом примере, ни в ДЗ).
         * @param line строка текста.
         * @param context объект, отвечающий за сохранение результата.
         */
        public void map(LongWritable offset, Text matrix, Context context) throws IOException, InterruptedException {
            char fileName = ((FileSplit) context.getInputSplit()).getPath().getName().charAt(0);
            Configuration conf = context.getConfiguration();
            int dim1 = Integer.parseInt(conf.get("dim1"));
            int dim2 = Integer.parseInt(conf.get("dim2"));
            int i = -1;
            for(String line: matrix.toString().split("\\r|\\n|\\r\\n")){
                System.out.println("fh\n");
                i++;
                System.out.println(String.valueOf(i));
                System.out.println(fileName);
                int n = 0;
                for (String number : matrix.toString().split(",")) {
                    for (int j = 0; j < dim1; j++) {
                        if (fileName == 'A') {
                            key.set(i, j, n, 0);
                        } else {
                            key.set(j, i, n, 1);
                        }
                        int num = Integer.parseInt(number);
                        value.set(num);
                        context.write(key, value);
                    }
                    n++;
    //
                }
            }
        }
    }


    public static class LineComparator extends WritableComparator {

        protected LineComparator(){
            super(Key.class,true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            Key k1 = (Key) w1;
            Key k2 = (Key) w2;
            return k1.pos.compareTo(k2.pos);
        }
    }


    public static class IPartitioner extends Partitioner<Key, IntWritable>{
        @Override
        public int getPartition(Key key, IntWritable value, int numPartitions) {
            return key.getPos().hashCode() % numPartitions;
        }
    }

    /**
     * Редьюсер. Суммирует пары (слово, 1) по ключу (слово).
     * На выходе получаем пары (уникальн_слово, кол-во).
     * В поставке Hadoop уже есть простейшие predefined reducers. Функционал данного редьюсера реализован в IntSumReducer.
     */
    public static class CountReducer extends Reducer<Key, IntWritable, Text, IntWritable>{
        //Пустой IntWritable-объект для экономии памяти, чтоб не создавать его при каждом выполнении reduce-функции
        private IntWritable result = new IntWritable(0);
        private Text position = new Text();

        public void reduce(Key key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> it = values.iterator();
            int sum = 0;
            int i = key.getPos().getI().get();
            int j = key.getPos().getJ().get();
            int M = key.getM().get();
            int n = key.getN().get();
            position.set("blya");
            result.set(100);
            context.write(position, result);
            int a_value = 0;
            while (it.hasNext()){
//
//                if (k % 2 == 0) {
//                    a_value = it.next().get();
//                }
//                else {
//                    sum += it.next().get() * a_value;
//                }
//                k++;
                String positionString = new StringBuilder().append(String.valueOf(i))
                        .append(" ").append(String.valueOf(j))
                        .append(" ").append(String.valueOf(n))
                        .append(" ").append(String.valueOf(M))
                        .toString();
                position.set(positionString);
//                position.set("blya");
//                int res = i* 100 + j * 100 + it.next().get();
//                int res = it.next().get();
//                result.set(res);
                context.write(position, it.next());

            }
//            String positionString = new StringBuilder().append(String.valueOf(i)).append(" ").append(String.valueOf(j)).toString();
//            position.set(positionString);
////            position.set("blya");
//            result.set(sum);
//            context.write(position, result);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Path outputPath = new Path(strings[1]);
        dim1 = Integer.parseInt(strings[2]);
        dim2 = Integer.parseInt(strings[3]);
        Configuration conf = new Configuration();
        conf.set("dim1", String.valueOf(dim1));
        conf.set("dim2", String.valueOf(dim2));

        // настройка Job'ы
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(MatrixMult.class);

        job1.setMapperClass(WordMapper.class);
        job1.setReducerClass(CountReducer.class);

        job1.setGroupingComparatorClass(LineComparator.class);
        job1.setPartitionerClass(IPartitioner.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(NonSplittableTextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapOutputKeyClass(Key.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setNumReduceTasks(8); // по умолчанию задаётся 1 reducer

        NonSplittableTextInputFormat.addInputPath(job1, new Path(strings[0]));
        TextOutputFormat.setOutputPath(job1, outputPath);

        return job1.waitForCompletion(true)? 0: 1; //ждём пока закончится Job и возвращаем результат
    }

    public static void main(String[] args) throws Exception {
        new MatrixMult().run(args);
    }
}



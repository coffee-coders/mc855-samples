package br.unicamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toSet;

public class CrimesCount {

    private static final double MAX_DISTANCE = 1D;

    public static class Part1Mapper extends Mapper<Object, Text, Text, Text> {

        private Text wordKey = new Text();
        private Text wordValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");

            wordKey.set(columns[1]); // crime
            try {
                wordValue.set(Double.valueOf(columns[8]) + "_" + Double.valueOf(columns[7])); // X_Y
                context.write(wordKey, wordValue);
            } catch (Exception e) {
                //do nothing
            }
        }
    }


    public static class Point {
        double x;
        double y;
        int crimeCount;

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public Point(double x, double y, int crimeCount) {
            this.x = x;
            this.y = y;
            this.crimeCount = crimeCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Point point = (Point) o;

            if (Double.compare(point.x, x) != 0)
                return false;
            return Double.compare(point.y, y) == 0;

        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            temp = Double.doubleToLongBits(x);
            result = (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(y);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        public double distance(Point t) {
          //http://andrew.hedges.name/experiments/haversine/

          double earthRadius = 6371000; //meters
          double dLat = Math.toRadians(this.x-t.x);
          double dLng = Math.toRadians(this.y-t.y);
          double a = Math.pow(Math.sin(dLat/2), 2) +
          Math.cos(Math.toRadians(this.x)) * Math.cos(Math.toRadians(t.x)) *
          Math.pow(Math.sin(dLng/2), 2);
          double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
          float dist = (float) (earthRadius * c);

          return dist;
        }
    }

    public static class Pair<T, V> {
        T left;
        V right;

        public Pair(T left, V right) {
            this.left = left;
            this.right = right;
        }

        public T getLeft() {
            return left;
        }

        public V getRight() {
            return right;
        }
    }

    public static class Part1Reducer extends Reducer<Text, Text, Text, Text> {
        private Text wordKey = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<Point> points = StreamSupport.stream(values.spliterator(), false)
                    .map(t -> t.toString().split("_"))
                    .map(t -> new Point(Double.valueOf(t[0]), Double.valueOf(t[1])))
                    .collect(toSet());

            for (Point p : points) {
                points.stream()
                        .filter(t -> !t.equals(p))
                        .filter(t -> p.distance(t) < MAX_DISTANCE)
                        .map(t -> new Pair<>(p, 1))
                        .collect(
                            Collectors.groupingBy(Pair::getLeft, Collectors.reducing(0, Pair::getRight, Integer::sum)))
                        .forEach((point, friends) -> {

                            wordKey.set(point.x + "_" + point.y);
                            try {
                                context.write(key, new Text(wordKey + "_" + new IntWritable(friends)));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }
    }

    //Identity mapper
    public static class Part2Mapper extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class Part2Reducer extends Reducer<Text, Text, Text, Text> {
        private Text wordKey = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<Point> points = StreamSupport.stream(values.spliterator(), false)
                    .map(t -> t.toString().split("_"))
                    .map(t -> new Point(Double.valueOf(t[0]), Double.valueOf(t[1]), Integer.valueOf(t[2])))
                    .collect(toSet());

            for (Point p : points) {
                Optional<Point> max = points.stream()
                        .filter(t -> !t.equals(p))
                        .filter(t -> p.distance(t) < MAX_DISTANCE)
                        .max(new Comparator<Point>() {

							@Override
							public int compare(Point o1, Point o2) {
								return Integer.compare(o1.crimeCount, o2.crimeCount);
							}
						});
                if(max.isPresent()){
                	Point point = max.get();
	                wordKey.set(point.x + "_" + point.y);
	                try {
	                    context.write(key, new Text(wordKey + "_" + new IntWritable(point.crimeCount)));
	                } catch (Exception e) {
	                    throw new RuntimeException(e);
	                }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: crimescount <in> <intermediate> <out>");
            System.exit(2);
        }
        runPart1(conf, otherArgs[0], otherArgs[1]);
        runPart2(conf, otherArgs[1], otherArgs[2]);
        System.exit(0);
    }

    private static void runPart1(Configuration conf, String input, String output) throws Exception {
        Job job = Job.getInstance(conf, "crimes count - part 1");
        job.setJarByClass(CrimesCount.class);
        job.setMapperClass(Part1Mapper.class);
        job.setCombinerClass(Part1Reducer.class);
        job.setReducerClass(Part1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

    private static void runPart2(Configuration conf, String input, String output) throws Exception {
         Job job = Job.getInstance(conf, "crimes count - part 2");
         job.setJarByClass(CrimesCount.class);
         job.setMapperClass(Part2Mapper.class);
         job.setCombinerClass(Part2Reducer.class);
         job.setReducerClass(Part2Reducer.class);
         job.setInputFormatClass(KeyValueTextInputFormat.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
         FileInputFormat.addInputPath(job, new Path(input));
         FileOutputFormat.setOutputPath(job, new Path(output));

         job.waitForCompletion(true);
    }
}

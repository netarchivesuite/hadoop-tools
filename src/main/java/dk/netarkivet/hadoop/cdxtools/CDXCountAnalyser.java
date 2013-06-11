package dk.netarkivet.hadoop.cdxtools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * This job counts the number of cdx records pointing to every arc- or warc-file in a given collection of
 * cdx files.
 */
public class CDXCountAnalyser extends Configured implements Tool {

    static public class ArchiveFilenameCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        final private static LongWritable ONE = new LongWritable(1);
        private Text tokenValue = new Text();

        /**
         * Counts based on the last token on the cdx line being the name of an arcfile or warcfile.
         * @param offset
         * @param text
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
            String tokens[] = text.toString().split("\\s+");
            String lastToken = tokens[tokens.length -1];
            if (lastToken.endsWith("arc")) {
                tokenValue.set(lastToken);
                context.write(tokenValue, ONE);
            }
        }
    }

    static public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable total = new LongWritable();

        @Override
        protected void reduce(Text token, Iterable<LongWritable> counts, Context context)
                throws IOException, InterruptedException {
            long n = 0;
            for (LongWritable count : counts)
                n += count.get();
            total.set(n);
            context.write(token, total);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job = new Job(configuration, "CDX Count Analysis");
        job.setJarByClass(CDXCountAnalyser.class);

        int n = args.length;
        if (n > 0)
            TextInputFormat.addInputPath(job, new Path(args[0]));
        if (n > 1)
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ArchiveFilenameCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CDXCountAnalyser(), args));
    }
}

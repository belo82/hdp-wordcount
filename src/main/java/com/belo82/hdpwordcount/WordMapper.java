package com.belo82.hdpwordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Maps english meaning of each word in a dictionary to its foreign language equivalents.
 *
 * @author Peter Belko
 */
public class WordMapper extends Mapper<Text, Text, Text, Text> {

    private Text word = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer words = new StringTokenizer(value.toString(), ",");

        while (words.hasMoreTokens()) {
            word.set(words.nextToken());
            context.write(key, word);
        }
    }
}

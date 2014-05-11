package com.belo82.hdpwordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduces all translations of english word to string in format 'english word|translation-lang1|translation lang2|...'
 *
 * @author Peter Belko
 */
public class AllTranslationsReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder translations = new StringBuilder();
        for(Text value : values) {
            translations.append("|").append(value);
        }

        result.set(translations.toString());
        context.write(key, result);
    }
}

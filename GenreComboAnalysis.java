import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GenreComboAnalysis {

  // Mapper class: Filters movies and sends matching genre-period combinations
  public static class GenreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    // Identify and store desired genre combos as a set of 2 string arrays.
    private final static Set<String[]> TARGET_COMBOS = new HashSet<>(Arrays.asList(
            new String[]{"Action", "Thriller"},
            new String[]{"Adventure", "Drama"},
            new String[]{"Comedy", "Romance"}
    ));

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Split the input line by semicolons
      String[] fields = value.toString().split(";");

      String titleType = fields[1];
      String startYearStr = fields[3];
      String ratingStr = fields[4];
      String[] genres = fields[5].split(",");

      // Ignores titleTypes with string other than "movie"
      if (!"movie".equalsIgnoreCase(titleType)) return;

      float rating;
      int year;
      try {
        rating = Float.parseFloat(ratingStr);
        year = Integer.parseInt(startYearStr);
      } catch (NumberFormatException e) {
        return;
      }

      if (rating < 7.0) return; // Skip tuple where rating is below threchold

      String timePeriod = getTimePeriod(year); // Use helper function to classify time into ranges
      if (timePeriod == null) return;  // Skip tuples outside of specified time ranges

      // Convert genres to a set for easy membership checking
      Set<String> genreSet = new HashSet<>(Arrays.asList(genres));

      // Check each tuple by looping through the 3 desired genre combos,
      // checking if the combo appears in the tuples genre set
      // If a genre set contains a combo then generate a (key,value) pair for that tuple.
      for (String[] combo : TARGET_COMBOS) {
        if (genreSet.contains(combo[0]) && genreSet.contains(combo[1])) {
          String genreKey = combo[0] + ";" + combo[1];
          context.write(new Text("[" + timePeriod + "]," + genreKey), one);
        }
      }
    }

    // Helper method to classify data by specified year range
    private String getTimePeriod(int year) {
      if (year >= 1991 && year <= 2000) return "1991-2000";
      if (year >= 2001 && year <= 2010) return "2001-2010";
      if (year >= 2011 && year <= 2020) return "2011-2020";
      return null;
    }
  }

  /*  Reducer:
      input key: genre combo and time period
      input value: 1s
      output key: genre and time period
      output value: sum of input values as total
  */
  public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
      int total = 0;
      for (IntWritable val : values) {
        total += val.get();
      }
      context.write(key, new IntWritable(total));
    }
  }

// Sets up and configures the MapReduce job
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Genre Combo Analysis");
    job.setJarByClass(GenreComboAnalysis.class);
    job.setMapperClass(GenreMapper.class);
    job.setCombinerClass(CountReducer.class);
    job.setReducerClass(CountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

//[1991-2000],Action;Thriller	55
//[1991-2000],Adventure;Drama	56
//[1991-2000],Comedy;Romance	215
//[2001-2010],Action;Thriller	76
//[2001-2010],Adventure;Drama	141
//[2001-2010],Comedy;Romance	400
//[2011-2020],Action;Thriller	208
//[2011-2020],Adventure;Drama	343
//[2011-2020],Comedy;Romance	590
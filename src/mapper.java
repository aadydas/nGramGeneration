import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class mapper extends MapReduceBase implements
    org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);

  public void map(LongWritable _key, Text value, OutputCollector<Text, IntWritable> output,
      Reporter reporter) throws IOException {

    String thisLine = value.toString().trim();
    ArrayList<String> allGram = new ArrayList<String>();
    StringTokenizer st = new StringTokenizer(thisLine, " ");
    String str2 = "";
    while (st.hasMoreTokens()) {
      String temp = (String) st.nextElement();
      String temp1 = temp;
      // a computer to 5647 be learned
      /*
       * if (temp.contains("'") || temp.contains("_")) { String parts[] = temp.split("(_)|(')"); int
       * length = parts.length; for (int i = 0; i < length; i++) { temp1 =
       * parts[i].replaceAll("[^a-zA-Z]+", ""); if (temp1 != null && !("".equals(temp1)) &&
       * !(" ".equals(temp1))) str2 = str2 + " " + temp1.toLowerCase(); } } else {
       */
      temp1 = temp.replaceAll("[^a-zA-Z]+", " ").trim();
      StringTokenizer st2 = new StringTokenizer(temp1, " ");

      while (st2.hasMoreTokens()) {
        str2 = str2 + " " + st2.nextToken().toLowerCase();
      }
      /*
       * System.out.println(temp1); if (temp1 != null && !("".equals(temp1)) &&
       * !(" ".equals(temp1))) { System.out.println(temp1 + " " + "Inside"); str2 = str2 + " " +
       * temp1.toLowerCase();
       */
    }

    // System.out.println(str2);
    thisLine = str2.trim();
    // System.out.println(thisLine);
    // 1-Gram
    if (!"".equals(thisLine)) {
      String[] nGrams = thisLine.split(" ");

      for (int j = 0; j <= nGrams.length - 1; j++) {
        allGram.add(nGrams[j]);
      }
      // 2-Gram
      for (int j = 0; j <= nGrams.length - 2; j++) {
        allGram.add(nGrams[j] + " " + nGrams[j + 1]);
      }
      // 3-Gram
      for (int j = 0; j <= nGrams.length - 3; j++) {
        allGram.add(nGrams[j] + " " + nGrams[j + 1] + " " + nGrams[j + 2]);
      }
      // 4-Gram
      for (int j = 0; j <= nGrams.length - 4; j++) {
        allGram.add(nGrams[j] + " " + nGrams[j + 1] + " " + nGrams[j + 2] + " " + nGrams[j + 3]);
      }
      // 5-Gram
      for (int j = 0; j <= nGrams.length - 5; j++) {
        allGram.add(nGrams[j] + " " + nGrams[j + 1] + " " + nGrams[j + 2] + " " + nGrams[j + 3]
            + " " + nGrams[j + 4]);
      }
    }
    for (String str : allGram)
      output.collect(new Text(str), one);
  }
}
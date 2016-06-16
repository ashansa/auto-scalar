package se.kth.honeytap.stat;

import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class RandomWorkloadGenerator {
  public static void main(String[] args) {
    //outliers will be selected with a probability of 15%
    //26 elements
    String[] candidates = {"1:2.2", "1:3.3", "1:6.8", "1:3.3", "1:10", "1:6.8", "1:3.3", "1:6.8", "1:3.3", "1:6.8",
            "1:3.3", "1:6.8", "1:3.3", "1:6.8", "1:3.3", "1:6.8", "1:3.3", "1:6.8","1:10", "1:3.3", "1:6.8", "1:3.3",
            "1:2.2", "1:6.8", "1:3.3", "1:6.8"};
    StringBuilder workload = new StringBuilder("1:2.2, ");
    String prev = "0.8:2.2";
    int no = 1;
    for (int i = 0; i < 24; ++i) {
      String newCandidate = candidates[new Random().nextInt(25)];
      if (newCandidate.equals(prev)) {
        if (no >= 2) {
          i--;
          continue;
        } else {
          workload.append(newCandidate).append(", ");
          no ++;
        }
      } else {
        workload.append(newCandidate).append(", ");
        prev = newCandidate;
        no = 1;
      }
    }
    System.out.println(workload);
    String ramWorkload = workload.substring(0, workload.lastIndexOf(","));
    System.out.println(ramWorkload);
  }
}

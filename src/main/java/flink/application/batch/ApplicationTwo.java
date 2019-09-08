/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package flink.application.batch;

import org.apache.flink.api.java.ExecutionEnvironment;

import com.google.common.io.Resources;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.val;

/*
 * Group by ratings
 */
public class ApplicationTwo {
    
    @Getter
    @AllArgsConstructor
    @ToString
    static class MovieRating {
        private Long userId;
        private Long movieId;
        private Double rating;
        private Long timestamp;
        private int stupidCounter;
    }
    
    public static void main(String[] args) throws Exception {
        
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        val list = env.readCsvFile(Resources.getResource("movielens/ratings.csv").getPath())
            .ignoreFirstLine()
            .ignoreInvalidLines()
            .parseQuotedStrings('"')
            .types(Long.class, Long.class, Double.class, Long.class)
            .map(tuple -> new MovieRating(tuple.f0, tuple.f1, tuple.f2, tuple.f3, 1))
            .groupBy(movieRating -> {
                val rating = movieRating.getRating();
                if(rating >= 9.0) return ">=9.0";
                if(rating >= 8.0) return ">=8.0";
                if(rating >= 7.0) return ">=7.0";
                return "<7.0";
            })
            .reduce((mr1, mr2) -> {
                return new MovieRating(
                    mr1.getUserId(), 
                    mr1.getMovieId(), 
                    mr1.getRating(),
                    mr1.getTimestamp(), 
                    mr1.getStupidCounter() + mr2.getStupidCounter());
            })
            .collect();
            
        System.out.println(list);
    }
}

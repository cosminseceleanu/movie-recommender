package com.movierecommender.main.jobs;

import com.google.inject.Inject;
import com.movierecommender.main.io.CassandraIo;
import com.movierecommender.main.model.RawRating;
import org.apache.spark.api.java.JavaRDD;

public class UserCountJob implements Job {
    private CassandraIo<RawRating> cassandraIo;

    @Inject
    public UserCountJob(CassandraIo<RawRating> cassandraIo) {
        this.cassandraIo = cassandraIo;
    }

    @Override
    public void execute() {
        JavaRDD<RawRating> ratingRdd = cassandraIo.readInput();
        long count = ratingRdd.map(rawRating -> rawRating.getUser())
                .distinct().count();

        System.out.println("in ratings data exists " + count + " distinct users");
    }

    @Override
    public String getName() {
        return "ratings.user.count";
    }
}

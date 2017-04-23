package com.movierecommender.main;

import org.apache.log4j.Logger;

import java.util.Date;

public class TimeKeeper {
    private Date startDate;
    private Date endDate;


    public void start() {
        startDate = new Date();
    }

    public TimeKeeper end() {
        endDate = new Date();

        return this;
    }

    public TimeKeeper reset() {
        endDate = null;
        startDate = null;

        return this;
    }

    public TimeKeeper print(Logger logger, String message) {
        long diff = endDate.getTime() - startDate.getTime();
        long diffSeconds = diff / 1000 % 60;
        logger.info(String.format("%s ms:%d s:%d", message, diff, diffSeconds));

        return this;
    }

    public TimeKeeper print(Logger logger) {
        return print(logger, "");
    }
}

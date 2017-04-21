package com.movierecommender.main.jobs;

import com.google.inject.Inject;
import org.apache.log4j.Logger;

import java.util.Set;

public class JobExecutor {
    private Set<Job> jobs;
    private Logger logger = Logger.getLogger(JobExecutor.class);

    @Inject
    public JobExecutor(Set<Job> jobs) {
        this.jobs = jobs;
    }

    public void execute(String commandName) {
        jobs.forEach(command -> {
            if (!command.getName().equals(commandName)) {
                return;
            }
            logger.info("Executing command " + commandName);
            command.execute();
        });
    }
}

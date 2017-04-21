package com.movierecommender.main;

import com.beust.jcommander.Parameter;
import com.movierecommender.main.jobs.Job;

public class Args {

    @Parameter(names = {"--c", "-commandName"}, description = "Job name to be executed")
    private String commandName = null;

    public String getCommandName() {
        return commandName == null ? Job.DEFAULT_COMMAND : commandName;
    }
}

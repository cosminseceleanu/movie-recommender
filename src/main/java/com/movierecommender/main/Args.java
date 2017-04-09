package com.movierecommender.main;

import com.beust.jcommander.Parameter;
import com.movierecommender.main.commands.Command;

public class Args {

    @Parameter(names = {"--c", "-commandName"}, description = "Command name to be executed")
    private String commandName = null;

    public String getCommandName() {
        return commandName == null ? Command.DEFAULT_COMMAND : commandName;
    }
}

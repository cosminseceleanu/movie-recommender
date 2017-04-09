package com.movierecommender.main.commands;

import com.google.inject.Inject;
import org.apache.log4j.Logger;

import java.util.Set;

public class CommandExecutor {
    private Set<Command> commands;
    private Logger logger = Logger.getLogger(CommandExecutor.class);

    @Inject
    public CommandExecutor(Set<Command> commands) {
        this.commands = commands;
    }

    public void execute(String commandName) {
        commands.forEach(command -> {
            if (!command.getName().equals(commandName)) {
                return;
            }
            logger.info("Executing command " + commandName);
            command.execute();
        });
    }
}

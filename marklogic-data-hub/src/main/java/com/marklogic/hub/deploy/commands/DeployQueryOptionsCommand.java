package com.marklogic.hub.deploy.commands;

import com.marklogic.appdeployer.command.AbstractCommand;
import com.marklogic.appdeployer.command.CommandContext;
import com.marklogic.hub.EntityManager;
import com.marklogic.hub.HubConfig;
import com.marklogic.hub.impl.EntityManagerImpl;

public class DeployQueryOptionsCommand extends AbstractCommand {

    private EntityManager entityManager;

    public DeployQueryOptionsCommand(HubConfig hubConfig) {
        this.entityManager = new EntityManagerImpl(hubConfig);
        setExecuteSortOrder(new LoadUserModulesCommand().getExecuteSortOrder() + 1);
    }

    @Override
    public void execute(CommandContext context) {
        entityManager.deployQueryOptions();
    }
}

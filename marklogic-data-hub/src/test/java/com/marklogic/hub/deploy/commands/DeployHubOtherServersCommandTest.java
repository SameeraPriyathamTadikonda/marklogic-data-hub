package com.marklogic.hub.deploy.commands;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.appdeployer.command.CommandContext;
import com.marklogic.hub.AbstractHubCoreTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeployHubOtherServersCommandTest extends AbstractHubCoreTest {

    @Test
    void addServerVersionToCustomTokens() {
        JsonNode node = readJsonObject(getHubConfig().getManageClient().getJson("/manage/v2"));
        final String serverVersion = node.iterator().next().get("version").asText();

        DeployHubOtherServersCommand command = new DeployHubOtherServersCommand(getHubConfig());
        CommandContext context = newCommandContext();
        command.addServerVersionToCustomTokens(context);

        String tokenValue = context.getAppConfig().getCustomTokens().get("%%mlServerVersion%%");
        assertTrue(serverVersion.startsWith(tokenValue), "Expected the token value to be at the start of the " +
            "version reported by ML; server version: " + serverVersion + "; token: " + tokenValue);
    }
}

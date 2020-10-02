package com.marklogic.hub.spark.sql.sources.v2;

import com.fasterxml.jackson.databind.JsonNode;
import com.marklogic.client.document.JSONDocumentManager;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.ext.util.DocumentPermissionsParser;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WriteDataWithOptionsTest extends AbstractSparkConnectorTest {

    @Test
    void ingestDocsWithCollection() throws IOException {
        String collections = "fruits";
        DataWriter<InternalRow> dataWriter = buildDataWriter(new Options(getHubPropertiesAsMap()).withBatchSize(1).withUriPrefix("/testFruit").withCollections(collections));
        dataWriter.write(buildRow("pineapple", "green"));
        String uriQuery = "cts.uriMatch('/testFruit**')";

        EvalResultIterator uriQueryResult = getHubClient().getStagingClient().newServerEval().javascript(uriQuery).eval();
        String uri = uriQueryResult.next().getString();

        JSONDocumentManager jd = getHubClient().getStagingClient().newJSONDocumentManager();
        DocumentMetadataHandle docMetaData = jd.readMetadata(uri, new DocumentMetadataHandle());

        assertEquals(collections, docMetaData.getCollections().toArray()[0].toString());
    }

    @Test
    void ingestDocsWithSourceName() throws IOException {
        String sourceName = "spark";
        DataWriter<InternalRow> dataWriter = buildDataWriter(new Options(getHubPropertiesAsMap()).withBatchSize(1).withUriPrefix("/testFruit").withSourceName(sourceName));
        dataWriter.write(buildRow("pineapple", "green"));
        String uriQuery = "cts.uriMatch('/testFruit**')";

        EvalResultIterator uriQueryResult = getHubClient().getStagingClient().newServerEval().javascript(uriQuery).eval();
        String uri = uriQueryResult.next().getString();

        JSONDocumentManager jd = getHubClient().getStagingClient().newJSONDocumentManager();
        JsonNode doc = jd.read(uri, new JacksonHandle()).get();

        assertEquals(sourceName, doc.get("envelope").get("headers").get("sources").get(0).get("name").asText());
    }

    @Test
    void ingestDocsWithSourceType() throws IOException {
        String sourceType = "fruits";
        DataWriter<InternalRow> dataWriter = buildDataWriter(new Options(getHubPropertiesAsMap()).withBatchSize(1).withUriPrefix("/testFruit").withSourceType(sourceType));
        dataWriter.write(buildRow("pineapple", "green"));
        String uriQuery = "cts.uriMatch('/testFruit**')";

        EvalResultIterator uriQueryResult = getHubClient().getStagingClient().newServerEval().javascript(uriQuery).eval();
        String uri = uriQueryResult.next().getString();

        JSONDocumentManager jd = getHubClient().getStagingClient().newJSONDocumentManager();
        JsonNode doc = jd.read(uri, new JacksonHandle()).get();

        assertEquals(sourceType, doc.get("envelope").get("headers").get("sources").get(0).get("datahubSourceType").asText());
    }

    @Test
    void ingestDocsWithPermissions() throws IOException {
        String permissions = "data-hub-common,read,data-hub-operator,update";
        DataWriter<InternalRow> dataWriter = buildDataWriter(new Options(getHubPropertiesAsMap()).withBatchSize(1).withUriPrefix("/testFruit").withPermissions(permissions));
        dataWriter.write(buildRow("pineapple", "green"));
        String uriQuery = "cts.uriMatch('/testFruit**')";

        EvalResultIterator uriQueryResult = getHubClient().getStagingClient().newServerEval().javascript(uriQuery).eval();
        String uri = uriQueryResult.next().getString();

        JSONDocumentManager jd = getHubClient().getStagingClient().newJSONDocumentManager();
        DocumentMetadataHandle docMetaData = jd.readMetadata(uri, new DocumentMetadataHandle());

        DocumentPermissionsParser documentPermissionParser = new DefaultDocumentPermissionsParser();
        DocumentMetadataHandle permissionMetadata = new DocumentMetadataHandle();
        documentPermissionParser.parsePermissions(permissions, permissionMetadata.getPermissions());

        Assert.assertEquals(permissionMetadata.getPermissions(), docMetaData.getPermissions());
    }

}

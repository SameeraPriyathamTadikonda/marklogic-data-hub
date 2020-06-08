/*
 * Copyright (c) 2020 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.marklogic.hub.flow.impl;

import com.marklogic.bootstrap.Installer;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.eval.EvalResult;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.DeleteQueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.hub.ApplicationConfig;
import com.marklogic.hub.FlowManager;
import com.marklogic.hub.HubConfig;
import com.marklogic.hub.HubTestBase;
import com.marklogic.hub.flow.RunFlowResponse;
import com.marklogic.hub.job.JobStatus;
import com.marklogic.hub.step.RunStepResponse;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = ApplicationConfig.class)
public class FlowRunnerTest extends HubTestBase {

    @Autowired
    FlowRunnerImpl flowRunner;

    @BeforeAll
    public static void setup() {
        XMLUnit.setIgnoreWhitespace(true);
        new Installer().deleteProjectDir();
    }

    @AfterAll
    public static void cleanUp(){
        new Installer().deleteProjectDir();
    }

    @BeforeEach
    public void setupEach() {
        setupProjectForRunningTestFlow();
    }

    @Test
    public void testRunFlow() {
        runAsDataHubOperator();
        RunFlowResponse resp = runFlow("testFlow", null, null, null, null);
        flowRunner.awaitCompletion();

        System.out.println("Logging response to help with debugging this failure on Jenkins: " + resp);
        verifyCollectionCountsFromRunningTestFlow();
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));
        XMLDocumentManager docMgr = stagingClient.newXMLDocumentManager();
        DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
        docMgr.readMetadata("/ingest-xml.xml", metadataHandle);
        DocumentMetadataHandle.DocumentPermissions permissions = metadataHandle.getPermissions();
        Assertions.assertEquals(2, permissions.get("data-hub-operator").size());
        Assertions.assertTrue(permissions.get("data-hub-operator").contains(DocumentMetadataHandle.Capability.READ));
        Assertions.assertTrue(permissions.get("data-hub-operator").contains(DocumentMetadataHandle.Capability.UPDATE));
        RunStepResponse stepResp = resp.getStepResponses().get("1");
        Assertions.assertNotNull(stepResp.getStepStartTime());
        Assertions.assertNotNull(stepResp.getStepEndTime());
        EvalResultIterator itr = runInDatabase("fn:collection(\"csv-coll\")[1]/envelope/headers/createdUsingFile", HubConfig.DEFAULT_STAGING_NAME);
        EvalResult res = itr.next();
        StringHandle sh = new StringHandle();
        res.get(sh);
        String file = sh.get();
        Assertions.assertNotNull(file);
        Assertions.assertTrue(file.contains("ingest.csv"));
    }

    @Test
    void customStepReferencesModulePathThatDoesntExist() {
        // Delete the module that the value-step step-definition points to
        runAsDataHubDeveloper();
        adminHubConfig.newModulesDbClient().newDocumentManager().delete("/custom-modules/custom/value-step/main.sjs");

        runAsDataHubOperator();
        Map<String, Object> options = new HashMap<>();
        options.put("collections", Arrays.asList("collector-test-output"));
        options.put("sourceQuery", "cts.collectionQuery('shouldnt-return-anything')");
        RunFlowResponse resp = runFlow("testValuesFlow", "1", UUID.randomUUID().toString(), options, null);
        flowRunner.awaitCompletion();

        List<String> errors = resp.getStepResponses().get("1").getStepOutput();
        assertEquals(1, errors.size(), "Expecting an error due to the missing module");
        assertTrue(errors.get(0).contains("Unable to access module: /custom-modules/custom/value-step/main.sjs. " +
            "Verify that this module is in your modules database and that your user account has a role that grants read permission to this module."),
            "Did not find expected message in error; error: " + errors.get(0));
    }

    /**
     * This test demonstrates multiple ways of expressing the sourceQuery as a script that returns values.
     */
    @Test
    public void sourceQueryReturnsScript() {
        // These indexes are removed by some other test, so need to ensure they're here for this test
        applyDatabasePropertiesForTests(adminHubConfig);

        final String flowName = "testValuesFlow";

        // Write a couple test documents that have range indexed values on them
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.getCollections().add("collector-test-input");
        metadata.getPermissions().add("data-hub-operator", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE);
        finalDocMgr.write("/collector-test1.json", metadata, new BytesHandle("{\"PersonGivenName\":\"Jane\", \"PersonSurName\":\"Smith\"}".getBytes()).withFormat(Format.JSON));
        finalDocMgr.write("/collector-test2.json", metadata, new BytesHandle("{\"PersonGivenName\":\"John\", \"PersonSurName\":\"Smith\"}".getBytes()).withFormat(Format.JSON));

        Map<String, Object> options = new HashMap<>();
        options.put("collections", Arrays.asList("collector-test-output"));

        runAsDataHubOperator();

        // cts.values with multiple index references and references to the options object too
        options.put("firstQName", "PersonGivenName");
        options.put("secondQName", "PersonSurName");
        options.put("sourceQuery", "cts.values([cts.elementReference(options.firstQName), cts.elementReference(options.secondQName)], null, null, cts.collectionQuery('collector-test-input'))");
        RunFlowResponse resp = runFlow(flowName, "1", UUID.randomUUID().toString(), options, null);
        flowRunner.awaitCompletion();
        assertEquals(JobStatus.FINISHED.toString(), resp.getJobStatus());
        assertEquals(3, getDocCount(HubConfig.DEFAULT_FINAL_NAME, "collector-test-output"),
            "There are 3 unique values in the PersonGivenName and PersonSurName indexes, so 3 documents should have been created");
        deleteCollectorTestOutput();

        // cts.elementValueCoOccurrences
        options.put("sourceQuery", "cts.elementValueCoOccurrences(xs.QName('PersonGivenName'), xs.QName('PersonSurName'), null, cts.collectionQuery('collector-test-input'))");
        resp = runFlow(flowName, "1", UUID.randomUUID().toString(), options, null);
        flowRunner.awaitCompletion();
        assertEquals(JobStatus.FINISHED.toString(), resp.getJobStatus());
        assertEquals(2, getDocCount(HubConfig.DEFAULT_FINAL_NAME, "collector-test-output"),
            "Both test documents should return a co-occurrence. Note that this array will be passed as a string to the " +
                "endpoint for running a flow. It can be converted into an array via xdmp.eval .");
        deleteCollectorTestOutput();

        // cts.valueTuples
        options.put("sourceQuery", "cts.valueTuples([cts.elementReference('PersonGivenName'), cts.elementReference('PersonSurName')], null, cts.collectionQuery('collector-test-input'))");
        resp = runFlow(flowName, "1", UUID.randomUUID().toString(), options, null);
        flowRunner.awaitCompletion();
        assertEquals(JobStatus.FINISHED.toString(), resp.getJobStatus());
        assertEquals(2, getDocCount(HubConfig.DEFAULT_FINAL_NAME, "collector-test-output"),
            "Each of the 2 test documents should return a tuple with 2 items in it");
    }

    private void deleteCollectorTestOutput() {
        final QueryManager queryManager = finalClient.newQueryManager();
        final DeleteQueryDefinition deleteQueryDefinition = queryManager.newDeleteDefinition();
        deleteQueryDefinition.setCollections("collector-test-output");
        queryManager.delete(deleteQueryDefinition);
    }

    @Test
    public void testIngestCSVasXML() throws Exception {
        //prov docs cannot be read by "flow-developer-user", so creating a client using 'secUser' which is 'admin'
        DatabaseClient client = getClient(host,jobPort, HubConfig.DEFAULT_JOB_NAME, secUser, secPassword, jobAuthMethod);
        //don't have 'admin' certs, so excluding from cert-auth tests
        if(! isCertAuth() ) {
            client.newServerEval().xquery("cts:uris() ! xdmp:document-delete(.)").eval();
        }
        Map<String,Object> opts = new HashMap<>();
        opts.put("outputFormat","xml");

        Map<String,Object> stepConfig = new HashMap<>();
        Map<String,String> stepDetails = new HashMap<>();

        stepDetails.put("outputURIPrefix" ,"/prefix-output");
        stepConfig.put("fileLocations", stepDetails);
        stepDetails.put("outputURIReplacement" ,null);

        runAsDataHubOperator();
        RunFlowResponse resp = runFlow("testFlow", "3", UUID.randomUUID().toString(),opts, stepConfig);
        flowRunner.awaitCompletion();
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_STAGING_NAME, "csv-coll") == 25);
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));
        EvalResultIterator resultItr = runInDatabase("fn:count(cts:uri-match(\"/prefix-output/*.xml\"))", HubConfig.DEFAULT_STAGING_NAME);
        EvalResult res = resultItr.next();
        long count = Math.toIntExact((long) res.getNumber());
        Assertions.assertEquals(count, 25);
        if(! isCertAuth() ) {
           EvalResultIterator itr = client.newServerEval().xquery("xdmp:estimate(fn:collection('http://marklogic.com/provenance-services/record'))").eval();
           if(itr != null && itr.hasNext()) {
               Assertions.assertEquals(25, itr.next().getNumber().intValue());
           }
           else {
               Assertions.fail("Server response was null or empty");
           }
        }
     }

     protected RunFlowResponse runFlow(String flowName, String commaDelimitedSteps, String jobId, Map<String,Object> options, Map<String, Object> stepConfig) {
        List<String> steps = commaDelimitedSteps != null ? Arrays.asList(commaDelimitedSteps.split(",")) : null;
         return flowRunner.runFlow(flowName, steps, jobId, options, stepConfig);
     }

    @Test
    public void testIngestCSVasXMLCustomDelimiter(){
        Map<String,Object> opts = new HashMap<>();
        opts.put("outputFormat","xml");

        Map<String,Object> stepConfig = new HashMap<>();
        Map<String,String> stepDetails = new HashMap<>();

        stepDetails.put("outputURIReplacement" ,".*/input,'/output'");
        stepDetails.put("separator" ,"\t");
        stepConfig.put("fileLocations", stepDetails);

        runAsDataHubOperator();
        RunFlowResponse resp = runFlow("testFlow", "4", UUID.randomUUID().toString(),opts, stepConfig);
        flowRunner.awaitCompletion();
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_STAGING_NAME, "csv-tab-coll") == 25);
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));
        EvalResultIterator resultItr = runInDatabase("fn:count(cts:uri-match(\"/output/*.xml\"))", HubConfig.DEFAULT_STAGING_NAME);
        EvalResult res = resultItr.next();
        long count = Math.toIntExact((long) res.getNumber());
        Assertions.assertEquals(count, 25);
    }

    @Test
    public void testIngestTextAsJson(){
        Map<String,Object> opts = new HashMap<>();
        opts.put("outputFormat","json");
        List<String> coll = new ArrayList<>();
        coll.add("text-collection");
        opts.put("targetDatabase", HubConfig.DEFAULT_STAGING_NAME);
        opts.put("collections", coll);

        Map<String,Object> stepConfig = new HashMap<>();
        Map<String,String> stepDetails = new HashMap<>();
        stepDetails.put("inputFileType","text");
        stepDetails.put("outputURIReplacement" ,".*/input,'/output'");
        stepConfig.put("fileLocations", stepDetails);

        runAsDataHubOperator();
        RunFlowResponse resp = runFlow("testFlow", "2", UUID.randomUUID().toString(),opts, stepConfig);
        flowRunner.awaitCompletion();
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_STAGING_NAME, "text-collection") == 1);
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));
    }

    @Test
    public void testEmptyCollector(){
        runAsDataHubOperator();

        Map<String,Object> opts = new HashMap<>();
        opts.put("sourceQuery", "cts.collectionQuery('non-existent-collection')");
        RunFlowResponse resp = runFlow("testFlow", "1,6", UUID.randomUUID().toString(), opts, null);
        flowRunner.awaitCompletion();
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_STAGING_NAME, "xml-coll") == 1);
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_FINAL_NAME, "xml-map") == 0);
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));
    }

    @Test
    public void testInvalidQueryCollector(){
        runAsDataHubOperator();

        Map<String,Object> opts = new HashMap<>();
        opts.put("sourceQuery", "cts.collectionQuer('xml-coll')");
        //Flow finishing with "finished_with_errors" status
        RunFlowResponse resp = runFlow("testFlow", "1,6", UUID.randomUUID().toString(), opts, null);
        flowRunner.awaitCompletion();
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_STAGING_NAME, "xml-coll") == 1);
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_FINAL_NAME, "xml-map") == 0);
        Assertions.assertTrue(JobStatus.FINISHED_WITH_ERRORS.toString().equalsIgnoreCase(resp.getJobStatus()));
        RunStepResponse stepResp = resp.getStepResponses().get("6");
        Assertions.assertTrue(stepResp.getStatus().equalsIgnoreCase("failed step 6"));

        //Flow finishing with "failed" status
        resp = runFlow("testFlow", "6", UUID.randomUUID().toString(), opts, null);
        flowRunner.awaitCompletion();
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_FINAL_NAME, "xml-map") == 0);
        Assertions.assertTrue(JobStatus.FAILED.toString().equalsIgnoreCase(resp.getJobStatus()));
        stepResp = resp.getStepResponses().get("6");
        Assertions.assertTrue(stepResp.getStatus().equalsIgnoreCase("failed step 6"));
    }

    @Test
    public void testRunFlowOptions(){
        runAsDataHubOperator();

        Map<String,Object> opts = new HashMap<>();
        List<String> coll = new ArrayList<>();
        coll.add("test-collection");
        opts.put("targetDatabase", HubConfig.DEFAULT_FINAL_NAME);
        opts.put("collections", coll);
        opts.put("sourceQuery", "cts.collectionQuery('test-collection')");
        RunFlowResponse resp = runFlow("testFlow", "1,2", UUID.randomUUID().toString(), opts, null);
        flowRunner.awaitCompletion();
        Assertions.assertEquals(2, getDocCount(HubConfig.DEFAULT_FINAL_NAME, "test-collection"));
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));

        opts.put("targetDatabase", HubConfig.DEFAULT_STAGING_NAME);
        opts.put("sourceDatabase", HubConfig.DEFAULT_FINAL_NAME);
        resp = runFlow("testFlow", "5", UUID.randomUUID().toString(), opts, null);
        flowRunner.awaitCompletion();
        Assertions.assertEquals(2, getDocCount(HubConfig.DEFAULT_STAGING_NAME, "test-collection"));
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));
    }

    @Test
    public void testRunFlowStepConfig(){
        runAsDataHubOperator();

        Map<String,Object> stepConfig = new HashMap<>();
        Map<String,Object> opts = new HashMap<>();
        List<String> coll = new ArrayList<>();
        coll.add("test-collection");
        Map<String,String> stepDetails = new HashMap<>();
        stepDetails.put("inputFileType","json");

        stepConfig.put("fileLocations", stepDetails);
        stepConfig.put("batchSize", "1");
        opts.put("targetDatabase", HubConfig.DEFAULT_FINAL_NAME);
        opts.put("collections", coll);

        RunFlowResponse resp = runFlow("testFlow", "1,2", UUID.randomUUID().toString(), opts, stepConfig);
        flowRunner.awaitCompletion();
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_FINAL_NAME, "test-collection") == 1);
        // Assert that a Job document is created
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_JOB_NAME, "Job") == 1);
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));
    }

    @Test
    public void testDisableJobOutput(){
        runAsDataHubOperator();

        Map<String,Object> opts = new HashMap<>();
        List<String> coll = new ArrayList<>();
        coll.add("test-collection");
        opts.put("targetDatabase", HubConfig.DEFAULT_FINAL_NAME);
        opts.put("collections", coll);
        opts.put("sourceQuery", "cts.collectionQuery('test-collection')");
        opts.put("disableJobOutput", Boolean.TRUE);

        RunFlowResponse resp = runFlow("testFlow", "1,2", UUID.randomUUID().toString(), opts, null);
        flowRunner.awaitCompletion();
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_FINAL_NAME, "test-collection") == 2);
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));
        // Assert that no Jobs documents were created
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_JOB_NAME, "Jobs") == 0);
    }

    @Test
    public void testRunFlowStopOnError(){
        runAsDataHubOperator();

        Map<String,Object> opts = new HashMap<>();

        opts.put("targetDatabase", HubConfig.DEFAULT_STAGING_NAME);
        opts.put("sourceDatabase", HubConfig.DEFAULT_STAGING_NAME);
        Map<String,String> mapping = new HashMap<>();
        mapping.put("name", "non-existent-mapping");
        mapping.put("version", "1");
        opts.put("mapping", mapping);

        RunFlowResponse resp = runFlow("testFlow", "1,6", UUID.randomUUID().toString(), opts, null);
        flowRunner.awaitCompletion();
        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_STAGING_NAME, "xml-coll") == 1);
        Assertions.assertTrue(JobStatus.STOP_ON_ERROR.toString().equalsIgnoreCase(resp.getJobStatus()));
    }

    @Test
    public void testIngestBinaryAndTxt(){
        runAsDataHubOperator();

        Map<String,Object> stepConfig = new HashMap<>();
        Map<String,Object> opts = new HashMap<>();
        List<String> coll = new ArrayList<>();
        coll.add("binary-text-collection");
        Map<String,String> stepDetails = new HashMap<>();
        stepDetails.put("inputFileType","binary");

        stepConfig.put("fileLocations", stepDetails);
        stepConfig.put("batchSize", "1");
        opts.put("outputFormat", "binary");
        opts.put("collections", coll);
        RunFlowResponse resp = runFlow("testFlow","1", UUID.randomUUID().toString(), opts, stepConfig);
        flowRunner.awaitCompletion();

        coll = new ArrayList<>();
        coll.add("text-collection");
        stepDetails = new HashMap<>();
        stepDetails.put("inputFileType","text");

        stepConfig.put("fileLocations", stepDetails);
        stepConfig.put("batchSize", "1");
        opts.put("outputFormat", "text");
        RunFlowResponse resp1 = runFlow("testFlow","1", UUID.randomUUID().toString(), opts, stepConfig);
        flowRunner.awaitCompletion();

        assertEquals(2, getDocCount(HubConfig.DEFAULT_STAGING_NAME, "binary-text-collection"));
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp1.getJobStatus()));
    }

    @Test
    public void testUnsupportedFileType(){
        runAsDataHubOperator();

        Map<String,Object> stepConfig = new HashMap<>();
        Map<String,Object> opts = new HashMap<>();
        List<String> coll = new ArrayList<>();
        coll.add("binary-text-collection");
        Map<String,String> stepDetails = new HashMap<>();
        stepDetails.put("inputFileType","unsupported");

        stepConfig.put("fileLocations", stepDetails);
        stepConfig.put("batchSize", "1");
        opts.put("outputFormat", "Binary");
        opts.put("collections", coll);
        RunFlowResponse resp = runFlow("testFlow","1", UUID.randomUUID().toString(), opts, stepConfig);
        flowRunner.awaitCompletion();

        Assertions.assertTrue(getDocCount(HubConfig.DEFAULT_STAGING_NAME, "binary-text-collection") == 0);
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()));
    }

    @Test
    public void testStopJob() {
        runAsDataHubOperator();

        final String jobId = "testStopJob";
        final long enoughTimeForTheJobToStartButNotYetFinish = 300;

        Runnable jobStoppingThread = ()->{
            sleep(enoughTimeForTheJobToStartButNotYetFinish);
            flowRunner.stopJob(jobId);
        };

        RunFlowResponse resp = runFlow("testFlow", null, jobId, null, null);
        jobStoppingThread.run();
        flowRunner.awaitCompletion();

        final String status = resp.getJobStatus() != null ? resp.getJobStatus().toLowerCase() : "";
        assertEquals(JobStatus.CANCELED.toString().toLowerCase(), status,
            "Expected the response status to be canceled, since the job was stopped before it finished, but was instead: " + status
         + ". If this failed in Jenkins, it likely can be ignored because we don't have a firm idea of how long the " +
                "thread that stops the job should wait until it stops the job.");
    }

    @Test
    public void testRunMultipleJobs() {
        runAsDataHubOperator();

        Map<String,Object> stepConfig = new HashMap<>();
        Map<String,Object> opts = new HashMap<>();
        List<String> coll = new ArrayList<>();
        coll.add("test-collection");
        Map<String,String> stepDetails = new HashMap<>();
        stepDetails.put("inputFileType","json");

        stepConfig.put("fileLocations", stepDetails);
        opts.put("collections", coll);

        Map<String,Object> stepConfig1 = new HashMap<>();
        Map<String,Object> opts1 = new HashMap<>();
        List<String> coll1 = new ArrayList<>();
        coll1.add("test-collection1");
        Map<String,String> stepDetails1 = new HashMap<>();
        stepDetails1.put("inputFileType","xml");

        stepConfig1.put("fileLocations", stepDetails1);
        opts1.put("collections", coll1);


        RunFlowResponse resp = runFlow("testFlow", "2", UUID.randomUUID().toString(), opts, stepConfig);
        RunFlowResponse resp1 = runFlow("testFlow", "2", UUID.randomUUID().toString(), opts1, stepConfig1);
        flowRunner.awaitCompletion();
        assertEquals(1, getDocCount(HubConfig.DEFAULT_STAGING_NAME, "test-collection"));
        assertEquals(1, getDocCount(HubConfig.DEFAULT_STAGING_NAME, "test-collection1"));
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp.getJobStatus()),
            "Expected job status of first response to be 'finished', but was: " + resp.getJobStatus());
        Assertions.assertTrue(JobStatus.FINISHED.toString().equalsIgnoreCase(resp1.getJobStatus()),
            "Expected job status of second response to be 'finished', but was: " + resp1.getJobStatus());
    }
}

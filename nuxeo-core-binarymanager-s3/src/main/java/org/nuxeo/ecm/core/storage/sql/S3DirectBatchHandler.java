/*
 * (C) Copyright 2011-2018 Nuxeo (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Luís Duarte
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core.storage.sql;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.nuxeo.ecm.core.storage.sql.S3Utils.NON_MULTIPART_COPY_MAX_SIZE;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.nuxeo.ecm.automation.server.jaxrs.batch.Batch;
import org.nuxeo.ecm.automation.server.jaxrs.batch.handler.AbstractBatchHandler;
import org.nuxeo.ecm.automation.server.jaxrs.batch.handler.BatchFileInfo;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.blob.binary.BinaryBlob;
import org.nuxeo.ecm.core.blob.binary.LazyBinary;
import org.nuxeo.ecm.core.transientstore.api.TransientStore;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Credentials;

/**
 * Batch Handler allowing direct S3 upload.
 *
 * @since 10.1
 */
public class S3DirectBatchHandler extends AbstractBatchHandler {

    public static final String DEFAULT_S3_HANDLER_NAME = "s3direct";

    protected static final Pattern REGEX_MULTIPART_ETAG = Pattern.compile("-\\d+$");

    // properties passed at initialization time from extension point

    public static final String PROP_AWS_SECRET_KEY_ID = "awsSecretKeyId";

    public static final String PROP_AWS_SECRET_ACCESS_KEY = "awsSecretAccessKey";

    public static final String PROP_AWS_REGION = "awsRegion";

    public static final String PROP_AWS_ROLE_ARN = "roleArn";

    public static final String PROP_USE_S3_ACCELERATION = "useS3Acceleration";

    public static final String PROP_AWS_BUCKET = "awsBucket";

    public static final String PROP_AWS_BUCKET_BASE_KEY = "baseKey";

    protected static final List<String> MANDATORY_PROPERTIES = Arrays.asList( //
            PROP_AWS_SECRET_KEY_ID, //
            PROP_AWS_SECRET_ACCESS_KEY, //
            PROP_AWS_ROLE_ARN, //
            PROP_AWS_REGION, //
            PROP_AWS_BUCKET //
    );

    // keys in the batch extra info, returned to the client

    public static final String INFO_AWS_SECRET_KEY_ID = "awsSecretKeyId";

    public static final String INFO_AWS_SECRET_ACCESS_KEY = "awsSecretAccessKey";

    public static final String INFO_AWS_SESSION_TOKEN = "awsSessionToken";

    public static final String INFO_BUCKET = "bucket";

    public static final String INFO_BASE_KEY = "baseKey";

    public static final String INFO_EXPIRATION = "expiration";

    public static final String INFO_AWS_REGION = "region";

    public static final String INFO_USE_S3_ACCELERATE = "useS3Accelerate";

    /** Transient store key for the batch handler name of a batch. */
    protected static final String PROVIDER_KEY = "provider";

    protected AWSSecurityTokenService stsClient;

    protected AmazonS3 amazonS3;

    protected String roleArnToAssume;

    protected String awsRegion;

    protected boolean s3AccelerationSupported;

    protected String bucket;

    protected String baseBucketKey;

    public S3DirectBatchHandler() {
        super(DEFAULT_S3_HANDLER_NAME);
    }

    @Override
    protected void initialize(Map<String, String> properties) {
        super.initialize(properties);
        for (String property : MANDATORY_PROPERTIES) {
            if (isEmpty(properties.get(property))) {
                throw new NuxeoException("Missing configuration property: " + property);
            }
        }
        awsRegion = properties.get(PROP_AWS_REGION);
        roleArnToAssume = properties.get(PROP_AWS_ROLE_ARN);
        bucket = properties.get(PROP_AWS_BUCKET);
        baseBucketKey = properties.getOrDefault(PROP_AWS_BUCKET_BASE_KEY, "/");
        s3AccelerationSupported = Boolean.parseBoolean(properties.get(PROP_USE_S3_ACCELERATION));
        String awsSecretKeyId = properties.get(PROP_AWS_SECRET_KEY_ID);
        String awsSecretAccessKey = properties.get(PROP_AWS_SECRET_ACCESS_KEY);
        AWSCredentialsProvider awsCredentialsProvider = S3Utils.getAWSCredentialsProvider(awsSecretKeyId,
                awsSecretAccessKey);
        stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                                                        .withRegion(awsRegion)
                                                        .withCredentials(awsCredentialsProvider)
                                                        .build();
        amazonS3 = AmazonS3ClientBuilder.standard()
                                        .withRegion(awsRegion)
                                        .withCredentials(awsCredentialsProvider)
                                        .withAccelerateModeEnabled(s3AccelerationSupported)
                                        .build();
    }

    @Override
    public Batch newBatch() {
        return initBatch();
    }

    @Override
    public Batch newBatch(String batchId) {
        return initBatch(batchId);
    }

    @Override
    public Batch getBatch(String batchId) {
        TransientStore transientStore = getTransientStore();
        Map<String, Serializable> batchEntryParams = transientStore.getParameters(batchId);
        if (batchEntryParams == null) {
            if (isEmpty(batchId) || !transientStore.exists(batchId)) {
                return null;
            }
            batchEntryParams = new HashMap<>();
        }

        // check that this batch is for this handler
        String batchProvider = (String) batchEntryParams.remove(PROVIDER_KEY);
        if (batchProvider != null && !batchProvider.equals(getName())) {
            throw new NuxeoException("Batch " + batchId + " is for handler " + batchProvider
                    + " but was requested to handler " + getName());
        }

        // create the batch
        Batch batch = new Batch(transientStore, getName(), batchId, batchEntryParams, this);
        AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest().withRoleSessionName(batch.getKey())
                                                                     .withRoleArn(roleArnToAssume);
        Credentials credentials = stsClient.assumeRole(assumeRoleRequest).getCredentials();
        Map<String, Object> extraInfo = batch.getBatchExtraInfo();
        extraInfo.put(INFO_AWS_SECRET_KEY_ID, credentials.getAccessKeyId());
        extraInfo.put(INFO_AWS_SECRET_ACCESS_KEY, credentials.getSecretAccessKey());
        extraInfo.put(INFO_AWS_SESSION_TOKEN, credentials.getSessionToken());
        extraInfo.put(INFO_BUCKET, bucket);
        extraInfo.put(INFO_BASE_KEY, baseBucketKey);
        extraInfo.put(INFO_EXPIRATION, credentials.getExpiration().toInstant().toEpochMilli());
        extraInfo.put(INFO_AWS_REGION, awsRegion);
        extraInfo.put(INFO_USE_S3_ACCELERATE, s3AccelerationSupported);

        return batch;
    }

    @Override
    public boolean completeUpload(String batchId, String fileIndex, BatchFileInfo fileInfo) {
        String fileKey = fileInfo.getKey();
        ObjectMetadata metadata = amazonS3.getObjectMetadata(bucket, fileKey);
        if (metadata == null) {
            return false;
        }
        String etag = metadata.getETag();
        if (isEmpty(etag)) {
            return false;
        }
        String mimeType = metadata.getContentType();
        String encoding = null;

        ObjectMetadata newMetadata;
        if (metadata.getContentLength() > NON_MULTIPART_COPY_MAX_SIZE) {
            newMetadata = S3Utils.copyBigFile(amazonS3, metadata, bucket, fileKey, etag, true);
        } else {
            newMetadata = S3Utils.copyFile(amazonS3, metadata, bucket, fileKey, etag, true);
            boolean isMultipartUpload = REGEX_MULTIPART_ETAG.matcher(etag).find();
            if (isMultipartUpload) {
                String previousEtag = etag;
                etag = newMetadata.getETag();
                newMetadata = S3Utils.copyFile(amazonS3, metadata, bucket, previousEtag, etag, true);
            }
        }

        String blobKey = transientStoreName + ':' + etag;
        String filename = fileInfo.getName();
        long length = newMetadata.getContentLength();
        String digest = newMetadata.getContentMD5();
        String blobProviderId = transientStoreName; // TODO decouple this
        Blob blob = new BinaryBlob(new LazyBinary(blobKey, blobProviderId, null), blobKey, filename, mimeType, encoding,
                digest, length);
        Batch batch = getBatch(batchId);
        batch.addFile(fileIndex, blob, filename, mimeType);

        return true;
    }

}

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
package org.nuxeo.ecm.core.storage.sql.aws;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.server.jaxrs.batch.Batch;
import org.nuxeo.ecm.automation.server.jaxrs.batch.handler.AbstractBatchHandler;
import org.nuxeo.ecm.automation.server.jaxrs.batch.handler.BatchFileInfo;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.blob.BlobInfo;
import org.nuxeo.ecm.core.blob.binary.BinaryBlob;
import org.nuxeo.ecm.core.blob.binary.LazyBinary;
import org.nuxeo.ecm.core.storage.sql.aws.util.Constants;
import org.nuxeo.ecm.core.transientstore.api.TransientStore;
import org.nuxeo.ecm.core.transientstore.api.TransientStoreService;
import org.nuxeo.runtime.api.Framework;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

public class S3DirectBatchHandler extends AbstractBatchHandler {

    private static final Log log = LogFactory.getLog(S3DirectBatchHandler.class);

    public static final String DEFAULT_S3_HANDLER_NAME = "s3direct";

    private static final Pattern REGEX_MULTIPART_ETAG = Pattern.compile("-\\d+$");

    private static final long FIVE_GB = 5_368_709_120L;

    private static final String PROVIDER_KEY = "provider";

    private String transientStoreName;

    private TransientStore transientStore;

    private AWSSecurityTokenService stsClient;
    private AmazonS3 s3Client;

    private String roleArnToAssume;
    private String awsRegion;
    private boolean s3AccelerationSupported;
    private String bucket;
    private String baseBucketKey;

    public S3DirectBatchHandler() {
        super(DEFAULT_S3_HANDLER_NAME);
    }

    @Override
    public Batch newBatch() {
        return initBatch();
    }

    @Override
    public Batch getBatch(String batchId) {
        TransientStore transientStore = getTransientStore();
        Map<String, Serializable> batchEntryParams = transientStore.getParameters(batchId);

        if (batchEntryParams == null) {
            if (!hasBatch(batchId)) {
                return null;
            }
            batchEntryParams = new HashMap<>();
        }

        String batchProvider = batchEntryParams.getOrDefault(PROVIDER_KEY, getName()).toString();
        batchEntryParams.remove(PROVIDER_KEY);

        if (!getName().equals(batchProvider)) {
            return null;
        }

        Batch batch = new Batch(transientStore, getName(), batchId, batchEntryParams, this);

        Map<String, Object> batchExtraInfo = batch.getBatchExtraInfo();
        AssumeRoleResult assumeRoleResult = stsClient.assumeRole(
                new AssumeRoleRequest().withRoleSessionName(batch.getKey()).withRoleArn(roleArnToAssume));
        batchExtraInfo
                .put(Constants.BatchExtraInfo.AWS_SECRET_KEY_ID, assumeRoleResult.getCredentials().getAccessKeyId());
        batchExtraInfo.put(Constants.BatchExtraInfo.AWS_SECRET_ACCESS_KEY, assumeRoleResult.getCredentials()
                                                                                           .getSecretAccessKey());
        batchExtraInfo
                .put(Constants.BatchExtraInfo.AWS_SESSION_TOKEN, assumeRoleResult.getCredentials().getSessionToken());
        batchExtraInfo.put(Constants.BatchExtraInfo.BUCKET, bucket);
        batchExtraInfo.put(Constants.BatchExtraInfo.BASE_KEY, baseBucketKey);
        batchExtraInfo
                .put(Constants.BatchExtraInfo.EXPIRATION, assumeRoleResult.getCredentials().getExpiration().toInstant()
                                                                          .toEpochMilli());
        batchExtraInfo.put(Constants.BatchExtraInfo.AWS_REGION, awsRegion);
        batchExtraInfo.put(Constants.BatchExtraInfo.USE_S3_ACCELERATE, s3AccelerationSupported);

        return batch;
    }

    private boolean hasBatch(String batchId) {
        return StringUtils.isNotEmpty(batchId) && transientStore.exists(batchId);
    }

    @Override
    public Batch newBatch(String batchId) {
        return initBatch(batchId);
    }

    @Override
    protected void init(Map<String, String> configProperties) {
        if (!containsRequired(configProperties)) {
            throw new NuxeoException();
        }

        transientStoreName = configProperties.get(Constants.S3TransientStoreConfig.Properties.TRANSIENT_STORE_NAME);

        String awsSecretKeyId = configProperties.get(Constants.S3TransientStoreConfig.Properties.AWS_SECRET_KEY_ID);
        String awsSecretAccessKey = configProperties
                .get(Constants.S3TransientStoreConfig.Properties.AWS_SECRET_ACCESS_KEY);
        awsRegion = configProperties.get(Constants.S3TransientStoreConfig.Properties.AWS_REGION);
        roleArnToAssume = configProperties.get(Constants.S3TransientStoreConfig.Properties.AWS_ROLE_ARN);
        bucket = configProperties.get(Constants.S3TransientStoreConfig.Properties.AWS_BUCKET);
        baseBucketKey = configProperties
                .getOrDefault(Constants.S3TransientStoreConfig.Properties.AWS_BUCKET_BASE_KEY, "/");

        s3AccelerationSupported = Boolean
                .parseBoolean(configProperties.get(Constants.S3TransientStoreConfig.Properties.USE_S3_ACCELERATION));

        stsClient = initClient(awsSecretKeyId, awsSecretAccessKey, awsRegion);
        s3Client = initS3Client(awsSecretKeyId, awsSecretAccessKey, awsRegion, s3AccelerationSupported);

        super.init(configProperties);
    }

    @Override
    public boolean completeUpload(String batchId, String fileIndex, BatchFileInfo fileInfo) {
        Batch batch = getBatch(batchId);

        ObjectMetadata s3ClientObjectMetadata = s3Client.getObjectMetadata(bucket, fileInfo.getKey());

        if (s3ClientObjectMetadata == null) {
            return false;
        }

        String etag = s3ClientObjectMetadata.getETag();
        String mimeType = s3ClientObjectMetadata.getContentType();

        if (StringUtils.isEmpty(etag)) {
            return false;
        }

        boolean isMultipartUpload = REGEX_MULTIPART_ETAG.matcher(etag).find();

        ObjectMetadata updatedObjectMetadata;

        if (s3ClientObjectMetadata.getContentLength() > FIVE_GB) {
            updatedObjectMetadata = AWSUtils
                    .copyBigFile(s3Client, s3ClientObjectMetadata, bucket, fileInfo.getKey(), etag, true);
        } else {
            updatedObjectMetadata = AWSUtils
                    .copyFile(s3Client, s3ClientObjectMetadata, bucket, fileInfo.getKey(), etag, true);
            if (isMultipartUpload) {
                String previousEtag = etag;
                etag = updatedObjectMetadata.getETag();
                updatedObjectMetadata = AWSUtils
                        .copyFile(s3Client, s3ClientObjectMetadata, bucket, previousEtag, etag, true);
            }
        }

        BlobInfo blobInfo = new BlobInfo();
        blobInfo.key = MessageFormat.format("{0}:{1}", transientStoreName, etag);
        blobInfo.filename = fileInfo.getName();
        blobInfo.length = updatedObjectMetadata.getContentLength();
        blobInfo.digest = updatedObjectMetadata.getContentMD5();
        blobInfo.mimeType = mimeType;

        Blob blob = new BinaryBlob(new LazyBinary(blobInfo.key, transientStoreName, null),
                                   blobInfo.key,
                                   blobInfo.filename,
                                   blobInfo.mimeType,
                                   blobInfo.encoding,
                                   blobInfo.digest,
                                   blobInfo.length
        );

        try {
            batch.addFile(fileIndex, blob, blobInfo.filename, blobInfo.mimeType);
        } catch (Exception e) {
            throw new NuxeoException(e);
        }

        return true;
    }

    protected boolean containsRequired(Map<String, String> configProperties) {

        String missingProperties = Stream.of(Constants.S3TransientStoreConfig.Properties.AWS_SECRET_KEY_ID,
                                             Constants.S3TransientStoreConfig.Properties.AWS_SECRET_ACCESS_KEY,
                                             Constants.S3TransientStoreConfig.Properties.AWS_ROLE_ARN,
                                             Constants.S3TransientStoreConfig.Properties.AWS_REGION,
                                             Constants.S3TransientStoreConfig.Properties.AWS_BUCKET,
                                             Constants.S3TransientStoreConfig.Properties.TRANSIENT_STORE_NAME
        )
                                         .filter(property -> isEmpty(configProperties.get(property)))
                                         .collect(Collectors.joining(","));

        if (!missingProperties.isEmpty()) {
            throw new NuxeoException(MessageFormat.format("Properties {0} are missing", missingProperties));
        }

        return true;
    }

    @Override
    protected TransientStore getTransientStore() {
        if (transientStore == null) {
            TransientStoreService service = Framework.getService(TransientStoreService.class);
            transientStore = service.getStore(transientStoreName);
        }

        return transientStore;
    }

    private AWSSecurityTokenService initClient(String awsSecretKeyId, String awsSecretAccessKey, String region) {
        return AWSSecurityTokenServiceClientBuilder.standard()
                                                   .withRegion(region)
                                                   .withCredentials(new AWSStaticCredentialsProvider(
                                                           new BasicAWSCredentials(awsSecretKeyId, awsSecretAccessKey))
                                                   )
                                                   .build()
                ;
    }

    protected AmazonS3 initS3Client(String awsSecretKeyId, String awsSecretAccessKey, String region, boolean useAccelerate) {
        AWSCredentialsProvider awsCredentialsProvider = getAwsCredentialsProvider(awsSecretKeyId, awsSecretAccessKey);
        return AmazonS3ClientBuilder.standard()
                                    .withRegion(region)
                                    .withCredentials(awsCredentialsProvider)
                                    .withAccelerateModeEnabled(useAccelerate)
                                    .build()
                ;
    }

    protected AWSCredentialsProvider getAwsCredentialsProvider(String awsSecretKeyId, String awsSecretAccessKey) {
        AWSCredentialsProvider result;
        if (isBlank(awsSecretKeyId) || isBlank(awsSecretAccessKey)) {
            result = InstanceProfileCredentialsProvider.getInstance();
            try {
                result.getCredentials();
            } catch (AmazonClientException e) {
                throw new RuntimeException("Missing AWS credentials and no instance role found", e);
            }
        } else {
            result = new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsSecretKeyId, awsSecretAccessKey));
        }
        return result;
    }
}

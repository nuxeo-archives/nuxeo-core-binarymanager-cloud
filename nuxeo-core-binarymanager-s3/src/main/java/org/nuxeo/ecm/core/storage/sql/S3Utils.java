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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.nuxeo.ecm.core.api.NuxeoException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;

/**
 * AWS S3 utilities.
 *
 * @since 10.1
 */
public class S3Utils {

    /** The maximum size of a file that can be copied without using multipart: 5 GB */
    public static final long NON_MULTIPART_COPY_MAX_SIZE = 5L * 1024 * 1024 * 1024;

    private S3Utils() {
        // utility class
    }

    /** Copies a file using multipart upload. */
    public static ObjectMetadata copyBigFile(AmazonS3 amazonS3, ObjectMetadata objectMetadata, String sourceBucket,
            String sourceKey, String targetBucket, String targetKey, boolean deleteSource) {
        List<CopyPartResult> copyResponses = new LinkedList<>();
        if (isEmpty(targetBucket)) {
            targetBucket = sourceBucket;
        }

        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(sourceBucket,
                targetKey);
        InitiateMultipartUploadResult initiateMultipartUploadResult = amazonS3.initiateMultipartUpload(
                initiateMultipartUploadRequest);
        String uploadId = initiateMultipartUploadResult.getUploadId();

        long objectSize = objectMetadata.getContentLength(); // in bytes

        // Step 4. Copy parts.
        long partSize = 5L * 1024 * 1024; // 5 MB
        long bytePosition = 0;
        for (int i = 1; bytePosition < objectSize; ++i) {
            // Step 5. Save copy response.
            long lastByte = Math.min(bytePosition + partSize - 1, objectSize - 1);
            CopyPartRequest copyRequest = new CopyPartRequest().withSourceBucketName(sourceBucket)
                                                               .withSourceKey(sourceKey)
                                                               .withDestinationBucketName(targetBucket)
                                                               .withDestinationKey(targetKey)
                                                               .withFirstByte(bytePosition)
                                                               .withLastByte(lastByte)
                                                               .withUploadId(uploadId)
                                                               .withPartNumber(i);
            copyResponses.add(amazonS3.copyPart(copyRequest));
            bytePosition += partSize;
        }
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(targetBucket, targetKey,
                uploadId, responsesToETags(copyResponses));
        amazonS3.completeMultipartUpload(completeRequest);
        if (deleteSource) {
            amazonS3.deleteObject(sourceBucket, sourceKey);
        }
        return amazonS3.getObjectMetadata(targetBucket, targetKey);
    }

    protected static List<PartETag> responsesToETags(List<CopyPartResult> responses) {
        return responses.stream().map(response -> new PartETag(response.getPartNumber(), response.getETag())).collect(
                Collectors.toList());
    }

    /** Copies a file using multipart upload in the same bucket. */
    public static ObjectMetadata copyBigFile(AmazonS3 amazonS3, ObjectMetadata objectMetadata, String bucket,
            String sourceKey, String targetKey, boolean deleteSource) {
        return copyBigFile(amazonS3, objectMetadata, bucket, sourceKey, bucket, targetKey, deleteSource);
    }

    /** Copies a file. */
    public static ObjectMetadata copyFile(AmazonS3 amazonS3, ObjectMetadata objectMetadata, String sourceBucket,
            String sourceKey, String targetBucket, String targetKey, boolean deleteSource) {
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(sourceBucket, sourceKey, targetBucket, targetKey);
        amazonS3.copyObject(copyObjectRequest);
        if (deleteSource) {
            amazonS3.deleteObject(sourceBucket, sourceKey);
        }
        return amazonS3.getObjectMetadata(targetBucket, targetKey);
    }

    /** Copies a file in the same bucket. */
    public static ObjectMetadata copyFile(AmazonS3 amazonS3, ObjectMetadata objectMetadata, String bucket,
            String sourceKey, String targetKey, boolean deleteSource) {
        return copyFile(amazonS3, objectMetadata, bucket, sourceKey, bucket, targetKey, deleteSource);
    }

    /** Gets the credentials providers. */
    public static AWSCredentialsProvider getAWSCredentialsProvider(String awsSecretKeyId, String awsSecretAccessKey) {
        AWSCredentialsProvider awsCredentialsProvider;
        if (isBlank(awsSecretKeyId) || isBlank(awsSecretAccessKey)) {
            awsCredentialsProvider = InstanceProfileCredentialsProvider.getInstance();
            try {
                awsCredentialsProvider.getCredentials();
            } catch (AmazonClientException e) {
                throw new NuxeoException("Missing AWS credentials and no instance role found", e);
            }
        } else {
            awsCredentialsProvider = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(awsSecretKeyId, awsSecretAccessKey));
        }
        return awsCredentialsProvider;
    }

}

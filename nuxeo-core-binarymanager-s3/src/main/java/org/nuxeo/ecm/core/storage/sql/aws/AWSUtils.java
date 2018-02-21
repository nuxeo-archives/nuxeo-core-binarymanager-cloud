package org.nuxeo.ecm.core.storage.sql.aws;


import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.nuxeo.ecm.core.api.NuxeoException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;

public class AWSUtils {
    public static final long FIVE_GB = 5_368_709_120L;

    private AWSUtils() {

    }

    public static ObjectMetadata copyBigFile(AmazonS3 s3Client, ObjectMetadata objectMetadata, String bucket, String sourceKey, String targetBucket, String targetKey, boolean deleteSource) {
        if (StringUtils.isEmpty(targetBucket)) {
            targetBucket = bucket;
        }

        List<CopyPartResult> copyResponses = new LinkedList<>();

        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucket, targetKey);

        InitiateMultipartUploadResult initiateMultipartUploadResult = s3Client.initiateMultipartUpload(initiateMultipartUploadRequest);

        String uploadId = initiateMultipartUploadResult.getUploadId();

        try {
            long objectSize = objectMetadata.getContentLength(); // in bytes

            // Step 4. Copy parts.
            long partSize = 20 * (long) Math.pow(2.0, 20.0); // 5 MB
            long bytePosition = 0;
            for (int i = 1; bytePosition < objectSize; ++i) {
                // Step 5. Save copy response.
                CopyPartRequest copyRequest = new CopyPartRequest()
                        .withDestinationBucketName(targetBucket)
                        .withDestinationKey(targetKey)
                        .withSourceBucketName(bucket)
                        .withSourceKey(sourceKey)
                        .withUploadId(uploadId)
                        .withFirstByte(bytePosition)
                        .withLastByte(bytePosition + partSize - 1 >= objectSize ? objectSize - 1 : bytePosition + partSize - 1)
                        .withPartNumber(i);

                copyResponses.add(s3Client.copyPart(copyRequest));
                bytePosition += partSize;
            }

            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(bucket, targetKey, uploadId, responsesToETags(copyResponses));

            CompleteMultipartUploadResult completeUploadResponse =
                    s3Client.completeMultipartUpload(completeRequest);


            if (deleteSource) {
                s3Client.deleteObject(bucket, sourceKey);
            }

            return s3Client.getObjectMetadata(bucket, targetKey);
        } catch (Exception e) {
            throw new NuxeoException(e);
        }

    }

    public static ObjectMetadata copyBigFile(AmazonS3 s3Client, ObjectMetadata objectMetadata, String bucket, String sourceKey, String targetKey, boolean deleteSource) {
        return copyBigFile(s3Client, objectMetadata, bucket, sourceKey, null, targetKey, deleteSource);
    }

    public static ObjectMetadata copyFile(AmazonS3 s3Client, ObjectMetadata objectMetadata, String bucket, String sourceKey, String targetBucket, String targetKey, boolean deleteSource) {
        if (StringUtils.isEmpty(targetBucket)) {
            targetBucket = bucket;
        }
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucket, sourceKey, targetBucket, targetKey);
        CopyObjectResult copyObjectResult = s3Client.copyObject(copyObjectRequest);

        if (deleteSource) {
            s3Client.deleteObject(bucket, sourceKey);
        }

        return s3Client.getObjectMetadata(targetBucket, targetKey);
    }

    public static ObjectMetadata copyFile(AmazonS3 s3Client, ObjectMetadata objectMetadata, String bucket, String sourceKey, String targetKey, boolean deleteSource) {
        return copyFile(s3Client, objectMetadata, bucket, sourceKey, null, targetKey, deleteSource);
    }

    private static List<PartETag> responsesToETags(List<CopyPartResult> responses) {
        return responses.stream().map(response -> new PartETag(response.getPartNumber(), response.getETag())).collect(Collectors.toList());
    }
}

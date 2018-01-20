/*
 * (C) Copyright 2011-2014 Nuxeo SA (http://nuxeo.com/) and others.
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
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core.storage.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.nuxeo.ecm.core.blob.binary.CachingBinaryManager.DEBUG_READ_CACHED_BINARY;
import static org.nuxeo.ecm.core.blob.binary.CachingBinaryManager.DEBUG_WRITE_CACHED_BINARY;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.core.api.Blobs;
import org.nuxeo.ecm.core.blob.binary.Binary;
import org.nuxeo.ecm.blob.AbstractTestCloudBinaryManager;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Features;
import org.nuxeo.runtime.test.runner.FeaturesRunner;
import org.nuxeo.runtime.test.runner.RuntimeFeature;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * ***** NOTE THAT THE TESTS WILL REMOVE ALL FILES IN THE BUCKET!!! *****
 * <p>
 * This test must be run with at least the following system properties set:
 * <ul>
 * <li>nuxeo.s3storage.bucket</li>
 * <li>nuxeo.s3storage.awsid (or AWS_ACCESS_KEY_ID environment variable)</li>
 * <li>nuxeo.s3storage.awssecret (or AWS_SECRET_ACCESS_KEY environment variable)</li>
 * </ul>
 * <p>
 * ***** NOTE THAT THE TESTS WILL REMOVE ALL FILES IN THE BUCKET!!! *****
 */
@RunWith(FeaturesRunner.class)
@Features(RuntimeFeature.class)
public class TestS3BinaryManager extends AbstractS3BinaryTest<S3BinaryManager> {

    @BeforeClass
    public static void beforeClass() {
        PROPERTIES = new HashMap<>();
        // this also checks in system properties for the configuration
        String bucketName = Framework.getProperty("nuxeo.s3storage.bucket");
        if (bucketName == null) {
            // NOTE THAT THE TESTS WILL REMOVE ALL FILES IN THE BUCKET!!!
            // ********** NEVER COMMIT THE SECRET KEYS !!! **********
            bucketName = "CHANGETHIS";
            String idKey = "CHANGETHIS";
            String secretKey = "CHANGETHIS";
            // ********** NEVER COMMIT THE SECRET KEYS !!! **********
            PROPERTIES.put(S3BinaryManager.BUCKET_NAME_PROPERTY, bucketName);
            PROPERTIES.put(S3BinaryManager.BUCKET_PREFIX_PROPERTY, "testfolder/");
            PROPERTIES.put(S3BinaryManager.AWS_ID_PROPERTY, idKey);
            PROPERTIES.put(S3BinaryManager.AWS_SECRET_PROPERTY , secretKey);
            boolean useKeyStore = false;
            if (useKeyStore) {
                // keytool -genkeypair -keystore /tmp/keystore.ks -alias unittest -storepass unittest -keypass unittest
                // -dname "CN=AWS S3 Key, O=example, DC=com" -keyalg RSA
                String keyStoreFile = "/tmp/keystore.ks";
                String keyStorePassword = "unittest";
                String privKeyAlias = "unittest";
                String privKeyPassword = "unittest";
                PROPERTIES.put(S3BinaryManager.KEYSTORE_FILE_PROPERTY , keyStoreFile);
                PROPERTIES.put(S3BinaryManager.KEYSTORE_PASS_PROPERTY , keyStorePassword);
                PROPERTIES.put(S3BinaryManager.PRIVKEY_ALIAS_PROPERTY , privKeyAlias);
                PROPERTIES.put(S3BinaryManager.PRIVKEY_PASS_PROPERTY , privKeyPassword);
            }
        }
        boolean disabled = bucketName.equals("CHANGETHIS");
        assumeTrue("No AWS credentials configured", !disabled);
    }

    @After
    public void tearDown() throws Exception {
        removeObjects();
    }

    @Override
    public boolean isStorageSizeSameAsOriginalSize() {
        return !binaryManager.isEncrypted;
    }

    @Test
    public void testS3BinaryManagerOverwrite() throws Exception {
        // store binary
        Framework.getProperties().remove(DEBUG_WRITE_CACHED_BINARY);
        Framework.getProperties().remove(DEBUG_READ_CACHED_BINARY);
        Binary binary = binaryManager.getBinary(Blobs.createBlob(CONTENT));
        assertNotNull(binary);
        assertEquals(CONTENT, toString(binary.getStream()));
        // check that the file was not in cache on write
        assertNull(Framework.getProperty(DEBUG_WRITE_CACHED_BINARY));
        assertEquals(binary.getDigest(), Framework.getProperty(DEBUG_READ_CACHED_BINARY));

        // store the same content again
        Framework.getProperties().remove(DEBUG_WRITE_CACHED_BINARY);
        Framework.getProperties().remove(DEBUG_READ_CACHED_BINARY);
        Binary binary2 = binaryManager.getBinary(Blobs.createBlob(CONTENT));
        assertNotNull(binary2);
        assertEquals(CONTENT, toString(binary.getStream()));
        // check that file was accessed from cache
        assertEquals(binary2.getDigest(), Framework.getProperty(DEBUG_WRITE_CACHED_BINARY));
        assertEquals(binary2.getDigest(), Framework.getProperty(DEBUG_READ_CACHED_BINARY));
    }

    @Test
    public void testS3MaxConnections() throws Exception {
        PROPERTIES.put(S3BinaryManager.CONNECTION_MAX_PROPERTY, "1");
        PROPERTIES.put(S3BinaryManager.CONNECTION_RETRY_PROPERTY, "0");
        PROPERTIES.put(S3BinaryManager.CONNECTION_TIMEOUT_PROPERTY, "5000"); // 5s
        try {
            binaryManager = new S3BinaryManager();
            binaryManager.initialize("repo", PROPERTIES);
            doTestS3MaxConnections();
        } finally {
            PROPERTIES.remove(S3BinaryManager.CONNECTION_MAX_PROPERTY);
            PROPERTIES.remove(S3BinaryManager.CONNECTION_RETRY_PROPERTY);
            PROPERTIES.remove(S3BinaryManager.CONNECTION_TIMEOUT_PROPERTY);
        }
    }

    protected void doTestS3MaxConnections() throws Exception {
        // store binary
        binaryManager.getBinary(Blobs.createBlob(CONTENT));

        String key = binaryManager.bucketNamePrefix + CONTENT_MD5;
        S3Object o = binaryManager.amazonS3.getObject(binaryManager.bucketName, key);
        try {
            binaryManager.amazonS3.getObject(binaryManager.bucketName, key);
            fail("Should throw AmazonClientException");
        } catch (AmazonClientException e) {
            Throwable c = e.getCause();
            assertTrue(c.getClass().getName(), c instanceof ConnectionPoolTimeoutException);
        }
        o.close();
    }

    @Override
    @Test
    public void testStoreFile() throws Exception {
        // Run normal test
        super.testStoreFile();
        // Run corruption test
        String key = binaryManager.bucketNamePrefix + CONTENT_MD5;
        binaryManager.amazonS3.putObject(binaryManager.bucketName, key, "Georges Abitbol");
        binaryManager.fileCache.clear();
        Boolean exceptionOccured = false;
        try {
            binaryManager.getBinary(CONTENT_MD5).getStream();
        } catch (RuntimeException e) {
            // Should not be wrapped in a RuntimeException as it declare the IOException
            if (e.getCause() instanceof IOException) {
                exceptionOccured = true;
            }
        }
        assertTrue("IOException should occured as content is corrupted", exceptionOccured);
    }

    @Override
    @Test
    public void testBinaryManagerGC() throws Exception {
        if (binaryManager.bucketNamePrefix.isEmpty()) {
            // no additional test if no bucket name prefix
            super.testBinaryManagerGC();
            return;
        }

        // create a md5-looking extra file at the root
        String digest = "12345678901234567890123456789012";
        try (InputStream in = new ByteArrayInputStream(new byte[] { '0' })) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(1);
            binaryManager.amazonS3.putObject(binaryManager.bucketName, digest, in, metadata);
        }
        assertEquals(Collections.singleton(digest), listAllObjects());

        // run base test with the bucket name prefix
        super.testBinaryManagerGC();

        // check that the extra file is still here
        Set<String> res = listAllObjects();
        assertTrue(res.contains(digest));
    }

    @Override
    protected S3BinaryManager getBinaryManager() throws IOException {
        S3BinaryManager binaryManager = new S3BinaryManager();
        binaryManager.initialize("repo", PROPERTIES);
        return binaryManager;
    }
}

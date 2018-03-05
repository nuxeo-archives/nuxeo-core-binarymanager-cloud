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
package org.nuxeo.ecm.core.storage.sql.aws.util;

public class Constants {
    private Constants() {

    }

    public static class S3TransientStoreConfig {
        public static class Properties {
            public static final String TRANSIENT_STORE_NAME = "transientStore";
            public static final String AWS_SECRET_KEY_ID = "awsSecretKeyId";
            public static final String AWS_SECRET_ACCESS_KEY = "awsSecretAccessKey";
            public static final String AWS_REGION = "awsRegion";
            public static final String AWS_ROLE_ARN = "roleArn";
            public static final String USE_S3_ACCELERATION = "useS3Acceleration";

            public static final String AWS_BUCKET = "awsBucket";
            public static final String AWS_BUCKET_BASE_KEY = "baseKey";
        }
    }

    public static class BatchExtraInfo {
        public static final String AWS_SECRET_KEY_ID = "awsSecretKeyId";
        public static final String AWS_SECRET_ACCESS_KEY = "awsSecretAccessKey";
        public static final String AWS_SESSION_TOKEN = "awsSessionToken";
        public static final String BUCKET = "bucket";
        public static final String BASE_KEY = "baseKey";
        public static final String EXPIRATION = "expiration";
        public static final String AWS_REGION = "region";
        public static final String USE_S3_ACCELERATE = "useS3Accelerate";
    }
}

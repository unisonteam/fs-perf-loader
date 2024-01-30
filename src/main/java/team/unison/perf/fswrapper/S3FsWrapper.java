/*
 *  Copyright (C) 2024 Unison LLC - All Rights Reserved
 *  You may use, distribute and modify this code under the
 *  terms of the License.
 *  For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 *
 */

package team.unison.perf.fswrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import team.unison.remote.WorkerException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class S3FsWrapper implements FsWrapper {
  private static final Logger log = LoggerFactory.getLogger(S3FsWrapper.class);
  private final S3Client s3Client;
  private static final byte[] DEVNULL = new byte[128 * 1024 * 1024];

  public S3FsWrapper(Map<String, String> conf) {
    AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(conf.get("s3.key"),
            conf.get("s3.secret"));

    StaticCredentialsProvider staticCredentialsProvider = StaticCredentialsProvider.create(awsBasicCredentials);

    Region region = Region.US_EAST_1;
    try {
      log.info("Create S3 client for uri {}", conf.get("s3.uri"));
      URI s3URI = new URI(conf.get("s3.uri"));

      s3Client = S3Client.builder()
              .credentialsProvider(staticCredentialsProvider)
              .region(region)
              .endpointOverride(s3URI)
              .forcePathStyle(true)
              .httpClientBuilder(ApacheHttpClient.builder()
                      .connectionTimeout(Duration.ofMinutes(10))
                      .socketTimeout(Duration.ofMinutes(10))).build();
    } catch (URISyntaxException e) {
      throw WorkerException.wrap(e);
    }
  }

  @Override
  public boolean create(String bucket, String path, long length, byte[] data, boolean useTmpFile) {
    String[] bucketAndKey = toBucketAndKey(bucket, path);
    PutObjectResponse putObjectResponse = s3Client.putObject(PutObjectRequest.builder()
                    .bucket(bucketAndKey[0])
                    .key(bucketAndKey[1])
                    .build(),
            RequestBody.fromInputStream(new EndlessInputStream(data), length));
    return putObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public boolean copy(String sourceBucket, String bucket, String path) {
    String[] sourceBucketAndKey = toBucketAndKey(sourceBucket, path);
    String[] destinationBucketAndKey = toBucketAndKey(bucket, path);
    CopyObjectResponse copyObjectResponse = s3Client.copyObject(CopyObjectRequest.builder()
            .sourceBucket(sourceBucketAndKey[0])
            .destinationBucket(destinationBucketAndKey[1])
            .sourceKey(sourceBucketAndKey[1])
            .destinationKey(destinationBucketAndKey[1])
            .build());

    return copyObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public boolean get(String bucket, String path) {
    String[] bucketAndKey = toBucketAndKey(bucket, path);
    ResponseInputStream<GetObjectResponse> getObjectResponse = s3Client.getObject(GetObjectRequest.builder()
            .bucket(bucketAndKey[0])
            .key(bucketAndKey[1])
            .build());

    if (!getObjectResponse.response().sdkHttpResponse().isSuccessful()) {
      return false;
    }

    try {
      while (getObjectResponse.read(DEVNULL) >= 0) {
        // nop
      }
    } catch (IOException e) {
      throw WorkerException.wrap(e);
    }

    return true;
  }

  @Override
  public boolean head(String bucket, String path) {
    String[] bucketAndKey = toBucketAndKey(bucket, path);
    HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder()
            .bucket(bucketAndKey[0])
            .key(bucketAndKey[1])
            .build());
    return headObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public boolean delete(String bucket, String path) {
    String[] bucketAndKey = toBucketAndKey(bucket, path);
    DeleteObjectResponse deleteObjectResponse = s3Client.deleteObject(DeleteObjectRequest.builder()
            .bucket(bucketAndKey[0])
            .key(bucketAndKey[1])
            .build());

    return deleteObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public List<String> list(String bucket, String path) {
    String[] bucketAndKey = toBucketAndKey(bucket, path);
    ListObjectsResponse listObjectsResponse = s3Client.listObjects(ListObjectsRequest.builder()
            .bucket(bucketAndKey[0])
            .prefix(bucketAndKey[1])
            .maxKeys(Integer.MAX_VALUE)
            .build());

    return listObjectsResponse.contents().stream().map(S3Object::key).collect(Collectors.toList());
  }

  public static String[] toBucketAndKey(String bucket, String path) {
    String pathNoLeadingSlash = (path.charAt(0) == '/') ? path.substring(1) : path;
    if (bucket != null) {
      return new String[]{bucket,
              pathNoLeadingSlash};
    }
    // should always contain slash because it's a full path with leaf file name
    return pathNoLeadingSlash.split("/", 2);
  }
}
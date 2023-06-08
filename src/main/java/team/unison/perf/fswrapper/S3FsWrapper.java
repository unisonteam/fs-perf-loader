package team.unison.perf.fswrapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import team.unison.remote.WorkerException;

public class S3FsWrapper implements FsWrapper {
  private static final Logger log = LoggerFactory.getLogger(S3FsWrapper.class);
  private final S3Client s3Client;
  private final String bucket;
  private static final byte[] DEVNULL = new byte[128 * 1024 * 1024];

  public S3FsWrapper(Map<String, String> conf) {
    this.bucket = conf.get("s3.bucket");

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
          .build();
    } catch (URISyntaxException e) {
      throw WorkerException.wrap(e);
    }
  }

  @Override
  public boolean create(String bucket, String path, long length, byte[] data, boolean useTmpFile) {
    PutObjectResponse putObjectResponse = s3Client.putObject(PutObjectRequest.builder()
                                                                 .bucket(toBucket(bucket))
                                                                 .key(toKey(path))
                                                                 .build(),
                                                             RequestBody.fromInputStream(new EndlessInputStream(data), length));
    return putObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public boolean copy(String sourceBucket, String bucket, String path) {
    CopyObjectResponse copyObjectResponse = s3Client.copyObject(CopyObjectRequest.builder()
                                                                    .sourceBucket(sourceBucket)
                                                                    .destinationBucket(toBucket(bucket))
                                                                    .sourceKey(toKey(path))
                                                                    .destinationKey(toKey(path))
                                                                    .build());

    return copyObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public boolean get(String bucket, String path) {
    ResponseInputStream<GetObjectResponse> getObjectResponse = s3Client.getObject(GetObjectRequest.builder()
                                                                                      .bucket(toBucket(bucket))
                                                                                      .key(toKey(path))
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
    HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder()
                                                                    .bucket(toBucket(bucket))
                                                                    .key(toKey(path))
                                                                    .build());
    return headObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public boolean delete(String bucket, String path) {
    DeleteObjectResponse deleteObjectResponse = s3Client.deleteObject(DeleteObjectRequest.builder()
                                                                          .bucket(toBucket(bucket))
                                                                          .key(toKey(path))
                                                                          .build());

    return deleteObjectResponse.sdkHttpResponse().isSuccessful();
  }

  private String toKey(String path) {
    return (path.charAt(0) == '/') ? path.substring(1) : path;
  }

  private String toBucket(String bucket) {
    return bucket != null ? bucket : this.bucket;
  }
}
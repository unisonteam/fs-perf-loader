package team.unison.perf.fswrapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
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
  private final S3Client s3Client;
  private final String bucket;

  public S3FsWrapper(Map<String, String> conf) {
    this.bucket = conf.get("s3.bucket");

    AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(conf.get("s3.key"),
                                                                         conf.get("s3.secret"));

    StaticCredentialsProvider staticCredentialsProvider = StaticCredentialsProvider.create(awsBasicCredentials);

    Region region = Region.US_EAST_1;
    try {
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
  public boolean create(String path, long length, byte[] data, boolean useTmpFile) {
    return create(bucket, path, length, data);
  }

  @Override
  public boolean create(String bucket, String path, long length, byte[] data) {
    PutObjectResponse putObjectResponse = s3Client.putObject(PutObjectRequest.builder()
                                                                 .bucket(bucket)
                                                                 .key(toS3Key(path))
                                                                 .build(),
                                                             RequestBody.fromInputStream(new EndlessInputStream(data), length));
    return putObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public boolean copy(String sourceBucket, String bucket, String path) {
    CopyObjectResponse copyObjectResponse = s3Client.copyObject(CopyObjectRequest.builder()
                                                                    .sourceBucket(sourceBucket)
                                                                    .destinationBucket(bucket)
                                                                    .sourceKey(toS3Key(path))
                                                                    .destinationKey(toS3Key(path))
                                                                    .build());

    return copyObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public boolean get(String bucket, String path) {
    ResponseInputStream<GetObjectResponse> getObjectResponse = s3Client.getObject(GetObjectRequest.builder()
                                                                                      .bucket(bucket)
                                                                                      .key(toS3Key(path))
                                                                                      .build());

    if (!getObjectResponse.response().sdkHttpResponse().isSuccessful()) {
      return false;
    }

    byte[] barr = new byte[128 * 1024 * 1024];

    try {
      while (getObjectResponse.read(barr) >= 0) {
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
                                                                    .bucket(bucket)
                                                                    .key(toS3Key(path))
                                                                    .build());
    return headObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public boolean delete(String bucket, String path) {
    DeleteObjectResponse deleteObjectResponse = s3Client.deleteObject(DeleteObjectRequest.builder()
                                                                          .bucket(bucket)
                                                                          .key(toS3Key(path))
                                                                          .build());

    return deleteObjectResponse.sdkHttpResponse().isSuccessful();
  }

  private String toS3Key(String path) {
    return (path.charAt(0) == '/') ? path.substring(1) : path;
  }
}
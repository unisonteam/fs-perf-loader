/*
 * Copyright (C) 2024 Unison LLC - All Rights Reserved
 * You may use, distribute and modify this code under the
 * terms of the License.
 * For full text of License visit : https://www.apache.org/licenses/LICENSE-2.0
 */

package team.unison.perf.fswrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import team.unison.remote.WorkerException;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class S3FsWrapper implements FsWrapper {
  private static final Logger log = LoggerFactory.getLogger(S3FsWrapper.class);

  private static final String CONF_KEY_MULTIPART_THRESHOLD = "s3.multipart_threshold";
  private static final String CONF_KEY_MAX_CONCURRENT_REQUESTS = "s3.max_concurrent_requests";

  private static final long DEFAULT_MULTIPART_THRESHOLD = 8L * 1024 * 1024; // 8M
  private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 10;

  private final S3Client s3Client;
  private final S3AsyncClient s3AsyncClient;
  private final long multipartThreshold;
  private final int maxConcurrentRequests;
  private static final byte[] DEVNULL = new byte[128 * 1024 * 1024];

  public S3FsWrapper(Map<String, String> conf) {
    AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(conf.get("s3.key"),
            conf.get("s3.secret"));

    StaticCredentialsProvider staticCredentialsProvider = StaticCredentialsProvider.create(awsBasicCredentials);

    Region region = Region.US_EAST_1;
    if (conf.containsKey(CONF_KEY_MULTIPART_THRESHOLD)) {
      multipartThreshold = Long.parseLong(conf.get(CONF_KEY_MULTIPART_THRESHOLD));
    } else {
      multipartThreshold = DEFAULT_MULTIPART_THRESHOLD;
    }
    maxConcurrentRequests = conf.containsKey(CONF_KEY_MAX_CONCURRENT_REQUESTS) ?
        Integer.parseInt(conf.get(CONF_KEY_MAX_CONCURRENT_REQUESTS)) : DEFAULT_MAX_CONCURRENT_REQUESTS;
    try {
      log.info("Create S3 client for uri {}", conf.get("s3.uri"));
      URI s3URI = new URI(conf.get("s3.uri"));

      ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration.builder()
              .putAdvancedOption(SdkAdvancedClientOption.SIGNER, AwsS3V4Signer.create())
              .build();

      s3Client = S3Client.builder()
              .credentialsProvider(staticCredentialsProvider)
              .region(region)
              .endpointOverride(s3URI)
              .forcePathStyle(true)
              .overrideConfiguration(clientOverrideConfiguration)
              .httpClientBuilder(ApacheHttpClient.builder()
                      .maxConnections(maxConcurrentRequests)
                      .connectionTimeout(Duration.ofMinutes(10))
                      .socketTimeout(Duration.ofMinutes(10))).build();

      s3AsyncClient = S3AsyncClient.builder()
          .endpointOverride(s3URI)
          .credentialsProvider(staticCredentialsProvider)
          .region(region)
          .forcePathStyle(true)
          .overrideConfiguration(clientOverrideConfiguration)
          .httpClient(NettyNioAsyncHttpClient.builder()
                  .maxConcurrency(maxConcurrentRequests)
                  .connectionTimeout(Duration.ofMinutes(10))
                  .readTimeout(Duration.ofMinutes(10))
                  .connectionAcquisitionTimeout(Duration.ofMinutes(1))
                  .build())
          .build();
    } catch (URISyntaxException e) {
      throw WorkerException.wrap(e);
    }
  }

  @Override
  public boolean create(String bucket, String path, long length, byte[] data, boolean useTmpFile) {
    String[] bucketAndKey = toBucketAndKey(bucket, path);
    if (length >= multipartThreshold) {
      String uploadId = null;
      try {
        // 1. Initiate multipart upload
        CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
            .bucket(bucketAndKey[0])
            .key(bucketAndKey[1])
            .build();
        CreateMultipartUploadResponse createResponse = getS3Client().createMultipartUpload(createRequest);
        uploadId = createResponse.uploadId();

        // 2. Split byte[] and upload parts in parallel
        List<CompletableFuture<CompletedPart>> uploadFutures = new ArrayList<>();
        EndlessInputStream endlessInputStream = new EndlessInputStream(data);
        Semaphore semaphore = new Semaphore(maxConcurrentRequests);

        int partNumber = 1;
        long previousLen = 0;
        AsyncRequestBody bodyToSent = null;
        for (long offset = 0; offset < length; offset += multipartThreshold) {
          long len = Math.min(multipartThreshold, length - offset);
          byte[] partBytes = new byte[(int) len];
          try {
            semaphore.acquire();
            if (previousLen != len) {
              endlessInputStream.read(partBytes, 0, (int) len);
              bodyToSent = AsyncRequestBody.fromBytes(partBytes);
            }
          } catch (Exception e) {
            throw WorkerException.wrap(e);
          }

          UploadPartRequest uploadRequest = UploadPartRequest.builder()
              .bucket(bucketAndKey[0])
              .key(bucketAndKey[1])
              .uploadId(uploadId)
              .partNumber(partNumber)
              .contentLength(len)
              .build();

          int currentPartNumber = partNumber;
          uploadFutures.add(
              getS3AsyncClient().uploadPart(uploadRequest, bodyToSent)
                  .whenComplete((resp, err) -> semaphore.release())
                  .thenApply(uploadResponse -> {
                    log.info("Finished uploading part #{}", currentPartNumber);
                    return CompletedPart.builder()
                        .partNumber(currentPartNumber)
                        .eTag(uploadResponse.eTag())
                        .build();
                  }));
          partNumber++;
          previousLen = len;
        }

        // Wait for all parts to finish uploading
        CompletableFuture.allOf(uploadFutures.toArray(new CompletableFuture[0])).join();

        List<CompletedPart> completedParts = uploadFutures.stream()
            .map(CompletableFuture::join)
            .collect(Collectors.toList());
        // 3. Complete multipart upload
        CompletedMultipartUpload completedUpload = CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build();

        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
            .bucket(bucketAndKey[0])
            .key(bucketAndKey[1])
            .uploadId(uploadId)
            .multipartUpload(completedUpload)
            .build();

        getS3Client().completeMultipartUpload(completeRequest);
        return true;
      } catch (Exception ex) {
        if (uploadId != null) {
            getS3Client().abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucketAndKey[0])
                    .key(bucketAndKey[1])
                    .uploadId(uploadId)
                    .build());
        }
        log.error("Multipart upload failed for key {} into bucket {}!", bucketAndKey[1], bucketAndKey[0], ex);
        return false;
      }
    } else {
      PutObjectResponse putObjectResponse = getS3Client().putObject(PutObjectRequest.builder()
              .bucket(bucketAndKey[0])
              .key(bucketAndKey[1])
              .build(),
          RequestBody.fromInputStream(new EndlessInputStream(data), length));
      return putObjectResponse.sdkHttpResponse().isSuccessful();
    }
  }

  @Override
  public boolean copy(String sourceBucket, String bucket, String path) {
    String[] sourceBucketAndKey = toBucketAndKey(sourceBucket, path);
    String[] destinationBucketAndKey = toBucketAndKey(bucket, path);
    CopyObjectResponse copyObjectResponse = getS3Client().copyObject(CopyObjectRequest.builder()
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
    try {
      long contentLength;
      try {
        HeadObjectResponse headObjectResponse = getS3Client().headObject(HeadObjectRequest.builder()
            .bucket(bucketAndKey[0])
            .key(bucketAndKey[1])
            .build());

        contentLength = headObjectResponse.contentLength();
      } catch (Exception e) {
        log.error("Failed to get head object for key {} in bucket {}", bucketAndKey[1], bucketAndKey[0], e);
        return false;
      }

      GetObjectRequest.Builder getObjectRequestBuilder = GetObjectRequest.builder()
          .bucket(bucketAndKey[0])
          .key(bucketAndKey[1]);

      if (contentLength <= multipartThreshold) {
        try (InputStream ignored = getS3Client().getObject(getObjectRequestBuilder.build())) {
          log.info("Finished downloading key {} from bucket {}", bucketAndKey[1], bucketAndKey[0]);
          return true;
        } catch (Exception e) {
          log.error("Failed to download key {} from bucket {}", bucketAndKey[1], bucketAndKey[0], e);
          return false;
        }
      }

      List<CompletableFuture<?>> downloadFutures = new ArrayList<>();
      Semaphore semaphore = new Semaphore(maxConcurrentRequests);

      for (long offset = 0; offset < contentLength; offset += multipartThreshold) {
        long end = Math.min(offset + multipartThreshold - 1, contentLength - 1);
        String range = "bytes=" + offset + "-" + end;

        GetObjectRequest getObjectRequest = getObjectRequestBuilder.range(range).build();

        try {
          semaphore.acquire();
          CompletableFuture<?> future = getS3AsyncClient()
              .getObject(getObjectRequest, AsyncResponseTransformer.toPublisher())
              .thenCompose(responsePublisher -> responsePublisher.subscribe(b -> {
              }))
              .whenComplete((resp, err) -> {
                if (err == null) {
                  log.info("Finished downloading range {} for key {}", range, bucketAndKey[1]);
                }
                semaphore.release();
              });
          downloadFutures.add(future);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Download interrupted", e);
        }
      }

      CompletableFuture.allOf(downloadFutures.toArray(new CompletableFuture[0])).join();

    } catch (Exception ex) {
      log.error("Something went wrong during ranged download", ex);
      return false;
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
    DeleteObjectResponse deleteObjectResponse = getS3Client().deleteObject(DeleteObjectRequest.builder()
            .bucket(bucketAndKey[0])
            .key(bucketAndKey[1])
            .build());

    return deleteObjectResponse.sdkHttpResponse().isSuccessful();
  }

  @Override
  public List<String> list(String bucket, String path) {
    String[] bucketAndKey = toBucketAndKey(bucket, path);
    ListObjectsResponse listObjectsResponse = getS3Client().listObjects(ListObjectsRequest.builder()
            .bucket(bucketAndKey[0])
            .prefix(bucketAndKey[1])
            .maxKeys(Integer.MAX_VALUE)
            .build());

    return listObjectsResponse.contents().stream().map(S3Object::key).collect(Collectors.toList());
  }

  @Override
  public boolean allowSnapshot(String path) {
    throw new UnsupportedOperationException("Making snapshottable is not supported on S3");
  }

  @Override
  public boolean createSnapshot(String path, String snapshotName) {
    throw new UnsupportedOperationException("Creating snapshot is not supported on S3");
  }

  @Override
  public boolean renameSnapshot(String path, String snapshotOldName, String snapshotNewName) {
    throw new UnsupportedOperationException("Renaming snapshot is not supported on S3");
  }

  @Override
  public boolean deleteSnapshot(String path, String snapshotName) {
    throw new UnsupportedOperationException("Deleting snapshot is not supported on S3");
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

  public S3Client getS3Client() {
    return s3Client;
  }

  public S3AsyncClient getS3AsyncClient() {
    return s3AsyncClient;
  }

}
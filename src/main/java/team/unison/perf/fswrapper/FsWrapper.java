package team.unison.perf.fswrapper;

public interface FsWrapper {
  boolean create(String bucket, String path, long length, byte[] data, boolean useTmpFile);

  boolean copy(String s, String bucket, String path);

  boolean get(String bucket, String path);

  boolean head(String bucket, String path);

  boolean delete(String bucket, String path);
}
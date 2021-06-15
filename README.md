# boto_s3_bucket_download
Downlaod and zip a bucket

# Dependencies
1. Version: Python 3.8.5
2. Module Names:
   * boto3 (external)
   * json (internal)
   * os (internal)
   * tarfile (internal)
   * glob (internal)
   * shutil (internal)
   * datetime (internal)
   * zlib (internal)
   * bz2 (internal)
   * lzma (internal)
3. If module is missing download using command:
   * python3 -m pip install ModuleName
4. Dependencies of shutil make_archive: (Info Source: https://docs.python.org/3/library/shutil.html)
   * zip: ZIP file (if the zlib module is available).
   * tar: Uncompressed tar file. Uses POSIX.1-2001 pax format for new archives.
   * gztar: gzip’ed tar-file (if the zlib module is available).
   * bztar: bzip2’ed tar-file (if the bz2 module is available).
   * xztar: xz’ed tar-file (if the lzma module is available).

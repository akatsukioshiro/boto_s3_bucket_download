# boto_s3_bucket_download
Downlaod and zip a bucket

# Dependencies (test.py)
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

#OLD
1. test_0.py is old.
2. It is file specific downlaods, where it downlaods only those zip files which are inside said folder, here "data" within chosen bucket.
3. Kept here for perspective and future uses.

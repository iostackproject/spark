/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.swift.snative;

import org.apache.commons.io.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.exceptions.SwiftConnectionClosedException;
import org.apache.hadoop.fs.swift.exceptions.SwiftException;
import org.apache.hadoop.fs.swift.exceptions.SwiftInternalStateException;
import org.apache.hadoop.fs.swift.util.SwiftUtils;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

/**
 * Output stream, buffers data on local disk.
 * Writes to Swift on the close() method, unless the
 * file is significantly large that it is being written as partitions.
 * In this case, the first partition is written on the first write that puts
 * data over the partition, as may later writes. The close() then causes
 * the final partition to be written, along with a partition manifest.
 */
class SwiftNativeOutputStream extends OutputStream {
  public static final int ATTEMPT_LIMIT = 3;
  private String storletType;
  private boolean useStorlets;
  private long filePartSize;
  private static final Log LOG =
          LogFactory.getLog(SwiftNativeOutputStream.class);
  private Configuration conf;
  private String key;
  private File backupFile;
  private OutputStream backupStream;
  private SwiftNativeFileSystemStore nativeStore;
  private boolean closed;
  private int partNumber;
  private long blockOffset;
  private long bytesWritten;
  private long bytesUploaded;
  private boolean partUpload = false;
  final byte[] oneByte = new byte[1];
  private int column;

  /**
   * Create an output stream
   * @param conf configuration to use
   * @param nativeStore native store to write through
   * @param key the key to write
   * @param partSizeKB the partition size
   * @throws IOException
   */
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  public SwiftNativeOutputStream(Configuration conf,
                                 SwiftNativeFileSystemStore nativeStore,
                                 String key,
                                 long partSizeKB) throws IOException {
    this.conf = conf;
    this.key = key;
    this.backupFile = newBackupFile();
    this.nativeStore = nativeStore;
    this.backupStream = new BufferedOutputStream(new FileOutputStream(backupFile));
    this.partNumber = 1;
    this.blockOffset = 0;
    this.filePartSize = 1024L * partSizeKB;
    this.useStorlets = conf.getBoolean("iostack.clientSideStorlet",false);
    this.column = conf.getInt("iostack.sqlcolumn", 0);
    this.storletType = conf.get("iostack.storlet.type", "words");
    LOG.info("CAMAMILLA SWIFT SwiftNativeOutputStream useStorlets="+useStorlets+ " column="+column);        // TODO TODO log
  }

  private File newBackupFile() throws IOException {
    File dir = new File(conf.get("hadoop.tmp.dir"));
    if (!dir.mkdirs() && !dir.exists()) {
      throw new SwiftException("Cannot create Swift buffer directory: " + dir);
    }
    File result = File.createTempFile("output-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  /**
   * Flush the local backing stream.
   * This does not trigger a flush of data to the remote blobstore.
   * @throws IOException
   */
  @Override
  public void flush() throws IOException {
    backupStream.flush();
  }

  /**
   * check that the output stream is open
   *
   * @throws SwiftException if it is not
   */
  private synchronized void verifyOpen() throws SwiftException {
    if (closed) {
      LOG.info("CAMAMILLA SwiftNativeOutputStream ERROR");       // TODO TODO log
      throw new SwiftConnectionClosedException();
    }
  }

  /**
   * Close the stream. This will trigger the upload of all locally cached
   * data to the remote blobstore.
   * @throws IOException IO problems uploading the data.
   */
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    try {
      closed = true;
      //formally declare as closed.
      backupStream.close();
      backupStream = null;
      Path keypath = new Path(key);
      if (partUpload) {
        partUpload(true);
        nativeStore.createManifestForPartUpload(keypath);
      } else {
        uploadOnClose(keypath);
      }
    } finally {
      delete(backupFile);
      backupFile = null;
    }
    assert backupStream == null: "backup stream has been reopened";
  }

  /**
   * Upload a file when closed, either in one go, or, if the file is
   * already partitioned, by uploading the remaining partition and a manifest.
   * @param keypath key as a path
   * @throws IOException IO Problems
   */
  private void uploadOnClose(Path keypath) throws IOException {
    boolean uploadSuccess = false;
    int attempt = 0;
    while (!uploadSuccess) {
      try {
        ++attempt;
        bytesUploaded += uploadFileAttempt(keypath, attempt);
        uploadSuccess = true;
      } catch (IOException e) {
        LOG.info("Upload failed " + e, e);
        if (attempt > ATTEMPT_LIMIT) {
          throw e;
        }
      }
    }
}

  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  private long uploadFileAttempt(Path keypath, int attempt) throws IOException {
    long uploadLen = backupFile.length();
    SwiftUtils.debug(LOG, "Closing write of file %s;" +
                          " localfile=%s of length %d - attempt %d",
                     key,
                     backupFile,
                     uploadLen,
                     attempt);
    if (useStorlets) {
      File auxFile = newBackupFile();
      BufferedOutputStream auxStreamStream = new BufferedOutputStream(new FileOutputStream(auxFile));

      if (storletType.equalsIgnoreCase("wordcount")) {
        uploadLen = invokeWordCount(new FileInputStream(backupFile), auxStreamStream, column);
      } else {
        uploadLen = invokeWords(new FileInputStream(backupFile), auxStreamStream, column);
      }

      LOG.info("CAMAMILLA SWIFT SwiftNativeOutputStream.uploadFileAttempt length Storlet = "+uploadLen+" auxFile length = "+auxFile.length());        // TODO TODO log
      nativeStore.uploadFile(keypath,
        new FileInputStream(auxFile),
        uploadLen);

      delete(auxFile);
    } else {
      LOG.info("CAMAMILLA SWIFT SwiftNativeOutputStream.uploadFileAttempt sense Storlet");        // TODO TODO log
      nativeStore.uploadFile(keypath,
        new FileInputStream(backupFile),
        uploadLen);
    }

    return uploadLen;
  }


  //////////////////////////////////////////////////// STORLET CLIENT SIDE /////////////////////////////////////////////
  private HashMap<String, Integer> mapWords = new HashMap<String, Integer>();
  private void addword(String w) {
    Integer count = 0;

    if (mapWords.containsKey(w)) {
      count = mapWords.get(w);
    }
    int value = count;
    value++;
    mapWords.put(w, value);
  }
  public int invokeWordCount(InputStream in, OutputStream out, int column) {
    LOG.info("CAMAMILLA SWIFT Storlets.invokeWordCount");        // TODO TODO log
    Scanner scanner = new Scanner(in);
    String line;
    String[] words;
    while(scanner.hasNextLine()){
      line = scanner.nextLine();
      words = line.split(",");
      if (words.length > column) {
        addword(words[column]);
      }
    }
    scanner.close();
    try {
      in.close();
    } catch (IOException e) {
      LOG.info("CAMAMILLA SWIFT Storlets.invoke after map phase ERROR " + e.getMessage());        // TODO TODO log
    }

    LOG.info("CAMAMILLA SWIFT Storlets.invoke after map phase");        // TODO TODO log
    int length = 0;
    try {
      String outLine;
      byte[] bytes;
      for (String w : mapWords.keySet()) {
        outLine = w + "," +mapWords.get(w)+"\n";
        bytes = outLine.getBytes(Charsets.UTF_8);
        length += bytes.length;
        out.write(bytes);
      }
      out.close();
    } catch (IOException e) {
      LOG.info("CAMAMILLA SWIFT Storlets.invoke after reduce phase ERROR " + e.getMessage());        // TODO TODO log
    }
    LOG.info("CAMAMILLA SWIFT Storlets.invoke after reduce phase");        // TODO TODO log
    return length;
  }
  public int invokeWords(InputStream in, OutputStream out, int column) {
    LOG.info("CAMAMILLA SWIFT Storlets.invokeWords");        // TODO TODO log
    Scanner scanner = new Scanner(in);
    String line;
    String[] words;
    List<String> list = new LinkedList<String>();
    while(scanner.hasNextLine()){
      line = scanner.nextLine();
      words = line.split(",");
      if (words.length > column) {
        list.add(words[column]);
      }
    }
    scanner.close();
    try {
      in.close();
    } catch (IOException e) {
      LOG.info("CAMAMILLA SWIFT Storlets.invoke after map phase ERROR " + e.getMessage());        // TODO TODO log
    }

    LOG.info("CAMAMILLA SWIFT Storlets.invoke after map phase");        // TODO TODO log
    int length = 0;
    try {
      String outLine;
      byte[] bytes;
      for (String w : list) {
        outLine = w + "\n";
        bytes = outLine.getBytes(Charsets.UTF_8);
        length += bytes.length;
        out.write(bytes);
      }
      out.close();
    } catch (IOException e) {
      LOG.info("CAMAMILLA SWIFT Storlets.invoke after reduce phase ERROR " + e.getMessage());        // TODO TODO log
    }
    LOG.info("CAMAMILLA SWIFT Storlets.invoke after reduce phase");        // TODO TODO log
    return length;
  }
  //////////////////////////////////////////////////// STORLET CLIENT SIDE /////////////////////////////////////////////




  @Override
  protected void finalize() throws Throwable {
    if(!closed) {
      LOG.warn("stream not closed");
    }
    if (backupFile != null) {
      LOG.warn("Leaking backing file " + backupFile);
    }
  }

  private void delete(File file) {
    if (file != null) {
      SwiftUtils.debug(LOG, "deleting %s", file);
      if (!file.delete()) {
        LOG.warn("Could not delete " + file);
      }
    }
  }

  @Override
  public void write(int b) throws IOException {
    //insert to a one byte array
    oneByte[0] = (byte) b;
    //then delegate to the array writing routine
    write(oneByte, 0, 1);
  }

  @Override
  public synchronized void write(byte[] buffer, int offset, int len) throws
                                                                     IOException {
    //validate args
    if (offset < 0 || len < 0 || (offset + len) > buffer.length) {
      throw new IndexOutOfBoundsException("Invalid offset/length for write");
    }
    //validate the output stream
    verifyOpen();
    SwiftUtils.debug(LOG, " write(offset=%d, len=%d)", offset, len);

    // if the size of file is greater than the partition limit
    while (blockOffset + len >= filePartSize) {
      // - then partition the blob and upload as many partitions
      // are needed.
      //how many bytes to write for this partition.
      int subWriteLen = (int) (filePartSize - blockOffset);
      if (subWriteLen < 0 || subWriteLen > len) {
        throw new SwiftInternalStateException("Invalid subwrite len: "
                                              + subWriteLen
                                              + " -buffer len: " + len);
      }
      writeToBackupStream(buffer, offset, subWriteLen);
      //move the offset along and length down
      offset += subWriteLen;
      len -= subWriteLen;
      //now upload the partition that has just been filled up
      // (this also sets blockOffset=0)
      partUpload(false);
    }
    //any remaining data is now written
    writeToBackupStream(buffer, offset, len);
  }

  /**
   * Write to the backup stream.
   * Guarantees:
   * <ol>
   *   <li>backupStream is open</li>
   *   <li>blockOffset + len &lt; filePartSize</li>
   * </ol>
   * @param buffer buffer to write
   * @param offset offset in buffer
   * @param len length of write.
   * @throws IOException backup stream write failing
   */
  private void writeToBackupStream(byte[] buffer, int offset, int len) throws
                                                                       IOException {
    assert len >= 0  : "remainder to write is negative";
    SwiftUtils.debug(LOG," writeToBackupStream(offset=%d, len=%d)", offset, len);
    if (len == 0) {
      //no remainder -downgrade to noop
      return;
    }

    //write the new data out to the backup stream
    backupStream.write(buffer, offset, len);
    //increment the counters
    blockOffset += len;
    bytesWritten += len;
  }

  /**
   * Upload a single partition. This deletes the local backing-file,
   * and re-opens it to create a new one.
   * @param closingUpload is this the final upload of an upload
   * @throws IOException on IO problems
   */
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  private void partUpload(boolean closingUpload) throws IOException {
    if (backupStream != null) {
      backupStream.close();
    }

    if (closingUpload && partUpload && backupFile.length() == 0) {
      //skipping the upload if
      // - it is close time
      // - the final partition is 0 bytes long
      // - one part has already been written
      SwiftUtils.debug(LOG, "skipping upload of 0 byte final partition");
      delete(backupFile);
    } else {
      partUpload = true;
      boolean uploadSuccess = false;
      int attempt = 0;
      while(!uploadSuccess) {
        try {
          ++attempt;
          bytesUploaded += uploadFilePartAttempt(attempt);
          uploadSuccess = true;
        } catch (IOException e) {
          LOG.info("Upload failed " + e, e);
          if (attempt > ATTEMPT_LIMIT) {
            throw e;
          }
        }
      }
      delete(backupFile);
      partNumber++;
      blockOffset = 0;
      if (!closingUpload) {
        //if not the final upload, create a new output stream
        backupFile = newBackupFile();
        backupStream =
          new BufferedOutputStream(new FileOutputStream(backupFile));
      }
    }
  }

  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  private long uploadFilePartAttempt(int attempt) throws IOException {
    long uploadLen = backupFile.length();
    SwiftUtils.debug(LOG, "Uploading part %d of file %s;" +
                          " localfile=%s of length %d  - attempt %d",
                     partNumber,
                     key,
                     backupFile,
                     uploadLen,
                     attempt);
    nativeStore.uploadFilePart(new Path(key),
                               partNumber,
                               new FileInputStream(backupFile),
                               uploadLen);
    return uploadLen;
  }

  /**
   * Get the file partition size
   * @return the partition size
   */
  long getFilePartSize() {
    return filePartSize;
  }

  /**
   * Query the number of partitions written
   * This is intended for testing
   * @return the of partitions already written to the remote FS
   */
  synchronized int getPartitionsWritten() {
    return partNumber - 1;
  }

  /**
   * Get the number of bytes written to the output stream.
   * This should always be less than or equal to bytesUploaded.
   * @return the number of bytes written to this stream
   */
  long getBytesWritten() {
    return bytesWritten;
  }

  /**
   * Get the number of bytes uploaded to remote Swift cluster.
   * bytesUploaded -bytesWritten = the number of bytes left to upload
   * @return the number of bytes written to the remote endpoint
   */
  long getBytesUploaded() {
    return bytesUploaded;
  }

  @Override
  public String toString() {
    return "SwiftNativeOutputStream{" +
           ", key='" + key + '\'' +
           ", backupFile=" + backupFile +
           ", closed=" + closed +
           ", filePartSize=" + filePartSize +
           ", partNumber=" + partNumber +
           ", blockOffset=" + blockOffset +
           ", partUpload=" + partUpload +
           ", nativeStore=" + nativeStore +
           ", bytesWritten=" + bytesWritten +
           ", bytesUploaded=" + bytesUploaded +
           '}';
  }
}

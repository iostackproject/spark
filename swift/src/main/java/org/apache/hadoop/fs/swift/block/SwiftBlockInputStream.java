package org.apache.hadoop.fs.swift.block;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3.Block;
import org.apache.hadoop.fs.s3.FileSystemStore;
import org.apache.hadoop.fs.s3.INode;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Wrapper of InputStream for Block FS implementation
 */
public class SwiftBlockInputStream extends FSInputStream {
  private static final Log LOG = LogFactory.getLog(SwiftBlockInputStream.class);

  /**
   * FS store instance
   */
  private FileSystemStore store;

  /**
   * file blocks
   */
  private Block[] blocks;

  /**
   * indicator whether stream is closed
   */
  private boolean closed;

  /**
   * file length
   */
  private long fileLength;

  /**
   * position in file
   */
  private long pos = 0;

  /**
   * current block
   */
  private File blockFile;

  /**
   * Wrapped stream blockFile
   */
  private DataInputStream blockStream;

  /**
   * block end marker
   */
  private long blockEnd = -1;

  /**
   * Hadoop statistics
   */
  private FileSystem.Statistics stats;

  /**
   * @param store instance
   * @param inode of file
   * @param stats Hadoop statistics
   */
  public SwiftBlockInputStream(FileSystemStore store,
                               INode inode, FileSystem.Statistics stats) {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.constructor");        // TODO TODO log
    this.store = store;
    this.stats = stats;
    this.blocks = inode.getBlocks();
    for (Block block : blocks) {
      this.fileLength += block.getLength();
    }
  }

  @Override
  public synchronized long getPos() throws IOException {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.getPos pos="+pos);        // TODO TODO log
    return pos;
  }

  @Override
  public synchronized int available() throws IOException {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.available");        // TODO TODO log
    return (int) (fileLength - pos);
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.seek pos="+targetPos);      // TODO TODO log
    if (targetPos > fileLength) {
      throw new IOException("Cannot seek after EOF");
    }
    pos = targetPos;
    blockEnd = -1;
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos)
          throws IOException {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.seekToNewSource");        // TODO TODO log
    return false;
  }

  @Override
  public synchronized int read() throws IOException {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.read");        // TODO TODO log
    if (closed) {
      throw new IOException("Stream closed");
    }
    int result = -1;
    if (pos < fileLength) {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      result = blockStream.read();
      if (result >= 0) {
        pos++;
      }
    }
    if (stats != null && result >= 0) {
      stats.incrementBytesRead(1);
    }
    return result;
  }

  @Override
  public synchronized int read(byte buf[], int off, int len)
          throws IOException {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.read off ="+off+" len="+len);        // TODO TODO log
    if (closed) {
      throw new IOException("Stream closed");
    }
    if (pos < fileLength) {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      int realLen = Math.min(len, (int) (blockEnd - pos + 1));
      int result = blockStream.read(buf, off, realLen);
      if (result >= 0) {
        pos += result;
      }
      if (stats != null && result > 0) {
        stats.incrementBytesRead(result);
      }
      return result;
    }
    return -1;
  }

  /**
   * Seeks and retrieves need block of data from Swift
   *
   * @param target position in file
   * @throws IOException
   */
  private synchronized void blockSeekTo(long target)
          throws IOException {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.blockSeekTo target = "+target);        // TODO TODO log
    //
    // Compute desired block
    //
    int targetBlock = -1;
    long targetBlockStart = 0;
    long targetBlockEnd = 0;
    for (int i = 0; i < blocks.length; i++) {
      long blockLength = blocks[i].getLength();
      targetBlockEnd = targetBlockStart + blockLength - 1;

      if (target >= targetBlockStart && target <= targetBlockEnd) {
        targetBlock = i;
        break;
      } else {
        targetBlockStart = targetBlockEnd + 1;
      }
    }
    if (targetBlock < 0) {
      throw new IOException("Impossible situation: could not " +
              "find target position " + target);
    }
    long offsetIntoBlock = target - targetBlockStart;

    this.blockFile = store.retrieveBlock(blocks[targetBlock], offsetIntoBlock);

    this.pos = target;
    this.blockEnd = targetBlockEnd;
    this.blockStream = new DataInputStream(new FileInputStream(blockFile));
  }

  @Override
  public void close() throws IOException {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.close");        // TODO TODO log
    if (closed) {
      return;
    }
    if (blockStream != null) {
      blockStream.close();
      blockStream = null;
    }
    if (blockFile != null) {
      blockFile.delete();
    }
    super.close();
    closed = true;
  }

  /**
   * We don't support marks.
   */
  @Override
  public boolean markSupported() {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.markSupported false");        // TODO TODO log
    return false;
  }

  @Override
  public void mark(int readLimit) {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.mark not supported");        // TODO TODO log
    // marks not supported
  }

  @Override
  public void reset() throws IOException {
    LOG.info("CAMAMILLA BLOCK SwiftBlockInputStream.reset not supported");        // TODO TODO log
    throw new IOException("Mark not supported");
  }
}
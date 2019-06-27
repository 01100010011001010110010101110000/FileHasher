package org.gregory.utils;

import org.apache.beam.sdk.io.FileIO;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.MessageDigest;

public class FileUtils {
  // Most OSs allow a 2MB page size, allocate this for efficiency
  private static int pageSize = 2 * 1024 * 1024 * 32;

  /**
   * @param digest The digest algorithm to run over the file
   * @param file   The file object for which to compute a digest
   * @return The message digest as a hexadecimal string
   * @throws IOException if an error occurs while scanning the file
   */
  public static String fileDigest(MessageDigest digest, FileIO.ReadableFile file) throws IOException {

    try (ReadableByteChannel byteChannel = file.open()) {
      ByteBuffer bytes = ByteBuffer.allocate(pageSize);
      int bytesRead;

      // Scan the file's byte channel, updating the hash with those bytes, until we are at EOF
      while ((bytesRead = byteChannel.read(bytes)) != -1) {
        digest.update(bytes.array(), 0, bytesRead);
        bytes.rewind(); // Prepares the buffer to be written to again
      }
    }

    // Get the computed hash's bytes
    byte[] digestBytes = digest.digest();

    StringBuilder digestString = new StringBuilder();
    for (byte digestByte : digestBytes) {
      // Convert each byte to two hex characters to get the hash's hex string representation
      digestString.append(Integer.toString((digestByte & 0xff) + 0x100, 16).substring(1));
    }

    return digestString.toString();
  }
}

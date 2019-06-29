package org.gregory.utils;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;

public class FileUtilsTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  private static MessageDigest digest = DigestUtils.getSha256Digest();
  private static String testFile = Paths.get("resources/hashTest.txt").toAbsolutePath().toString();
  private static String testFileHash = "dca69306dac30c407ce5a474f655ab0ac72713720b28b3b4ae8b9217bba57f8f";

  private static class DigestTester extends DoFn<FileIO.ReadableFile, String> {
    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
      FileIO.ReadableFile file = context.element();
      String hash = FileUtils.fileDigest(digest, file);
      context.output(hash);
    }
  }

  @Test
  public void fileDigestTest() {
    PCollection<String> collection = p.apply("Search for matching files in source", FileIO.match().filepattern(testFile))
        .apply("Read matching files", FileIO.readMatches())
        .apply(ParDo.of(new DigestTester()));
    PAssert.that(collection).containsInAnyOrder(testFileHash);
    p.run().waitUntilFinish();
  }
}
package org.gregory;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.HashMap;

public class FileHasherTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  private static String testFile = Paths.get("resources/hashTest.txt").toAbsolutePath().toString();
  private static String testFileHash = "dca69306dac30c407ce5a474f655ab0ac72713720b28b3b4ae8b9217bba57f8f";

  @Test
  public void HashFilesTest() {
    HashMap<String, String> results = new HashMap<>();
    results.put("path", testFile);
    results.put("hash", testFileHash);

    PCollection<HashMap<String, String>> collection = p.apply("Search for matching files in source", FileIO.match().filepattern(testFile))
        .apply("Read matching files", FileIO.readMatches())
        .apply(ParDo.of(new FileHasher.HashFiles()));
    PAssert.that(collection).containsInAnyOrder(results);
    p.run().waitUntilFinish();
  }

  @Test(expected = FileNotFoundException.class)
  public void emptySourceTest() {
    p.apply("Search for matching files in source", FileIO.match().filepattern(""));
  }

  @Test
  public void mainTest() {
    String outputFile = "resources/testResults.json";
    p.apply("Search for matching files in source", FileIO.match().filepattern(testFile))
        .apply("Read matching files", FileIO.readMatches())
        .apply("Hash each file", ParDo.of(new FileHasher.HashFiles()))
        .apply("Serialize path and hash to JSON", ParDo.of(new FileHasher.ToJson()))
        .apply("Write results to a single JSON array", TextIO.write().to(outputFile)
            .withHeader("[")
            .withFooter("]")
            .withoutSharding());
    p.run().waitUntilFinish();
    File resultFile = new File(outputFile);
    assertTrue(resultFile.exists());
  }
}
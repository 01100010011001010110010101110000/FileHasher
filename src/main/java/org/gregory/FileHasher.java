/*
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
package org.gregory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.codec.digest.DigestUtils;
import org.gregory.utils.FailureObject;
import org.gregory.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashMap;

public class FileHasher {
  public interface FileHasherOptions extends PipelineOptions {
    @Description("Input file glob")
    @Default.String("./input/*")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the output directory")
    @Default.String("./output")
    @Validation.Required
    String getOutputDirectory();

    void setOutputDirectory(String value);

    @Description("Whether to output to a single file")
    @Default.Boolean(true)
    Boolean getOutputSingleFile();

    void setOutputSingleFile(Boolean value);
  }

  /**
   * Converts a {@code FileIO.ReadableFile} into a {@code HashMap<String, String>} where the
   * key-value pairs of {@code {"path": <fully qualified file path>, "hash": <file's SHA-256 hash>}}
   */
  static class HashFiles extends DoFn<FileIO.ReadableFile, HashMap<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      // Fetch the file from context
      FileIO.ReadableFile file = context.element();
      try {
        LOG.info("Hashing {}", fileToPath(file));

        // Compute the file's digest
        MessageDigest digest = DigestUtils.getSha256Digest();
        String hash = FileUtils.fileDigest(digest, file);

        // Construct HashMap containing the file's path and hash
        HashMap<String, String> result = new HashMap<>();
        result.put("path", fileToPath(file));
        result.put("hash", hash);
        context.output(result);

      } catch (IOException exception) {
        FailureObject failure = new FailureObject(file, exception);
        LOG.error("Error hashing {}:\n{}", fileToPath(file),
            failure.toString());
      }
    }
  }

  /**
   * Converts a {@code HashMap<String, String>} to an equivalent JSON string
   */
  static class ToJson extends DoFn<HashMap<String, String>, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      HashMap<String, String> map = context.element();
      ObjectMapper mapper = new ObjectMapper();
      try {
        // Serialize the map to a JSON object
        String serialized = mapper.writeValueAsString(map);
        context.output(serialized);

      } catch (JsonProcessingException exception) {
        FailureObject failure = new FailureObject(map, exception);
        LOG.error("Error processing JSON\n{}", failure.toString());
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileHasher.class);

  private static String fileToPath(FileIO.ReadableFile file) {
    return file.getMetadata().resourceId().getCurrentDirectory() +
        file.getMetadata().resourceId().getFilename();
  }

  public static void main(String[] args) {
    FileHasherOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(FileHasherOptions.class);
    Pipeline p = Pipeline.create(options);
    FileSystems.setDefaultPipelineOptions(options);

    // Configure output options
    Boolean singleFileOutput = options.getOutputSingleFile();
    Path outputPath;
    TextIO.Write jsonArrayWriter = TextIO.write().withHeader("[").withFooter("]");
    if (singleFileOutput) {
      outputPath = Paths.get(options.getOutputDirectory(), "manifest.json");
      jsonArrayWriter = jsonArrayWriter
          .to(outputPath.toString())
          .withoutSharding();
    } else {
      outputPath = Paths.get(options.getOutputDirectory());
      jsonArrayWriter = jsonArrayWriter
          .to(outputPath.toString());
    }

    Path inputPath = Paths.get(options.getInputFile());
    p.apply("Search for matching files in source", FileIO.match().filepattern(inputPath.toString()))
        .apply("Read matching files", FileIO.readMatches())
        .apply("Hash each file", ParDo.of(new HashFiles()))
        .apply("Serialize path and hash to JSON", ParDo.of(new ToJson()))
        .apply("Write results to a single JSON array", jsonArrayWriter);
    p.run();
  }
}


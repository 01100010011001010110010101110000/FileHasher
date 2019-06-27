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
import jdk.management.resource.ResourceId;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.gregory.utils.*;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.HashMap;

public class FileHasher {
  public interface FileHasherOptions extends PipelineOptions {
    @Description("Path of the file to read")
    @Default.String("gs://censys-interviews/data-engineer/testdata.zip")
    String getInputFile();

    void setInputFile(String value);

    /**
     * Specifies where to write the output
     */
    @Description("Path of the file to write to")
    @Default.String("./manifest.json")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }

  static class HashFiles extends DoFn<FileIO.ReadableFile, HashMap<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      // Fetch the file from context
      FileIO.ReadableFile file = context.element();
      try {
        LOG.info("Hashing {}", file.getMetadata().resourceId().getFilename());

        // Compute the file's digest
        MessageDigest digest = DigestUtils.getSha256Digest();
        String hash = FileUtils.fileDigest(digest, file);

        // Construct HashMap containing the file's path and hash
        HashMap<String, String> result = new HashMap<>();
        result.put("path", fileToPath(file));
        result.put("hash", hash);
        context.output(result);

      } catch (IOException exception) {
        LOG.error("Error hashing {}: {}", file.getMetadata().resourceId().getFilename(),
            exception.getLocalizedMessage());
      }
    }
  }

  static class ToJson extends DoFn<HashMap<String, String>, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        // Serialize the map to a JSON object
        HashMap<String, String> map = context.element();
        ObjectMapper mapper = new ObjectMapper();
        String serialized = mapper.writeValueAsString(map);
        context.output(serialized);

      } catch (JsonProcessingException exception) {
        LOG.error("Error processing JSON: {}", exception.getLocalizedMessage());
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(FileHasher.class);

  private static String fileToPath(FileIO.ReadableFile file) {
    return file.getMetadata().resourceId().getCurrentDirectory() +
        file.getMetadata().resourceId().getFilename();
  }

  public static void main(String[] args) {
    FileHasherOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FileHasherOptions.class);
    Pipeline p = Pipeline.create(options);
    FileSystems.setDefaultPipelineOptions(options);

    p.apply("Search for matching files in source", FileIO.match().filepattern(options.getInputFile()))
        .apply("Read matching files", FileIO.readMatches())
        .apply("Hash each file", ParDo.of(new HashFiles()))
        .apply("Serialize path and hash to JSON", ParDo.of(new ToJson()))
        .apply("Write results to a single JSON array", TextIO.write().to(options.getOutput())
            .withoutSharding() // Conducts all write operations on a single worker, yielding a single file
            .withHeader("[")
            .withFooter("]"));
    p.run();
  }
}


# Apache Beam File Hasher

`FileHasher` is a fairly straightforward apache beam class which takes one or more files from a source 
directory, computes their SHA-256 hashes, and writes out a JSON array containing their fully
qualified paths and hashes

## Configuration Options:

| Option | Effect | Default |
|---|---|---|
| `--outputDirectory=output/suboutput/` | The directory where the JSON output will be placed | `./output` |
| `--inputFile=/Users/tgregory/Downloads/testdata/*` | The file glob to use to select files | `./input/*` |
| `--outputSingleFile` | If true, write all JSON output to a single file containing a single array; if false, write multiple files each containing a single array  | `true`

## Testing instructions
The test suite is written using JUnit4, and may be executed by executing `mvn clean test` in the project root
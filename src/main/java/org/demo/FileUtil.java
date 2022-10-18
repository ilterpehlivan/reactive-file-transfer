package org.demo;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileUtil {

  public static final String PROJECT_BASE = System.getenv("BASE_PATH");
  public static final String RESOURCES_PATH = PROJECT_BASE + "/src/main/resources";
  public static final String DOWNLOAD_PATH = RESOURCES_PATH + "/" + "downloads";
  public static final String UPLOAD_PATH = RESOURCES_PATH + "/" + "uploads";
  public static final String GENERATED_PATH = RESOURCES_PATH + "/" + "generated";

  private static final int CHUNK_SIZE_IN_KB = 12;
  static int fileSizeInKB = 120 * 1024;

  public static String createRandomFolder(String resourcesPath) throws IOException {
    String folder =
        Files.createDirectory(Paths.get(resourcesPath + "/video" + new Random().nextInt(10)))
            .toString();
    log.info("created folder {}", folder);
    return folder;
  }

  // generate random files with random contents
  public static Set<Path> generateFiles(int count) {
    String parentPath = GENERATED_PATH;
    log.info("creating folder {} and random files size {}", parentPath,count);
    // create folder and files
    String folderPath = parentPath;

    try {
      //      for (int i = 0; i < 5; i++) {
      //        folderPath = createRandomFolder(parentPath);
      for (int j = 0; j < count; j++) {
        File file = createRandomFile(folderPath);
        fillInDummy(file);
      }
      //      }
      return listFilesInDirectory(GENERATED_PATH);
    } catch (IOException e) {
      log.error("error while generating random files", e);
    }

    return Collections.emptySet();
  }

  public static void createRandomFile(String path, String offset) throws IOException {
    Path filePath = Paths.get(path + "/" + UUID.randomUUID() + "_" + offset + ".txt");
    log.info("creating file {}", filePath);
    File newFile = Files.createFile(filePath).toFile();
    // write some stuff inside
    Files.writeString(filePath, "hello-" + offset);
  }

  // put dummy bytes size of X KB
  private static Path fillInDummy(File file) throws IOException {
    byte[] bytes = new byte[fileSizeInKB];
    new Random().nextBytes(bytes);
    // write some stuff inside
    return Files.write(file.toPath(), bytes);
  }

  public static File createRandomFile(String path) throws IOException {
    Path filePath = Paths.get(path + "/FILE-" + UUID.randomUUID() + ".dat");
    log.info("creating file {}", filePath);
    File newFile = Files.createFile(filePath).toFile();
    return newFile;
  }

  public static Set<Path> listFilesInDirectory(String dir) throws IOException {
    try (Stream<Path> stream = Files.list(Paths.get(dir))) {
      return stream
          .filter(file -> !Files.isDirectory(file))
          //                    .map(Path::getFileName)
          //                    .map(Path::toString)
          .collect(Collectors.toSet());
    }
  }

  public static Set<Path> listDirs(String dir) throws IOException {
    try (Stream<Path> stream = Files.list(Paths.get(dir))) {
      return stream
          .filter(file -> Files.isDirectory(file))
          //                    .map(Path::getFileName)
          //                    .map(Path::toString)
          .collect(Collectors.toSet());
    }
  }

  public static List<ChunkRecord> splitFile(Path file) throws IOException {
    log.info("splitting file {} into chunks {}", file, CHUNK_SIZE_IN_KB);
    List<ChunkRecord> files = new ArrayList<>();
    int sizeOfChunk = 1024 * CHUNK_SIZE_IN_KB;
    try (BufferedReader br = new BufferedReader(new FileReader(file.toFile()))) {
      char[] chunk = new char[sizeOfChunk];
      long fileSizeInKB = file.toFile().length();
      for (int i = 0, j = 1; i < fileSizeInKB; i += sizeOfChunk, j++) {
        br.read(chunk, i, sizeOfChunk);
        files.add(new ChunkRecord(file.toString(), j, charArrayToBytes(chunk)));
      }
    }
    log.info("splitted chunks {}", files);
    return files;
  }

  public static void deleteFolder(String path) {
    try {
      log.info("DELETE:folder {} as completed", path);
      Files.deleteIfExists(Paths.get(path));
    } catch (IOException e) {
      log.warn("error while deleting folder ", e);
    }
  }

  public static List<ChunkRecord> splitFileStream(Path file) throws IOException {
    log.info("splitting file {} into chunks {}", file, CHUNK_SIZE_IN_KB);
    List<ChunkRecord> files = new ArrayList<>();
    int sizeOfChunk = 1024 * CHUNK_SIZE_IN_KB;
    try (FileInputStream fis = new FileInputStream(file.toFile())) {
      byte[] buffer = new byte[sizeOfChunk];
      int len;
      int order = 1;
      while ((len = fis.read(buffer)) > 0) {
        files.add(new ChunkRecord(file.toString(), order, buffer));
        order++;
      }
      }
    log.info("splitted chunks {}", files);
    return files;
  }

  private static byte[] charArrayToBytes(char[] value) {
    byte[] result = new byte[value.length];
    for (int i = 0; i < value.length; i++) result[i] = (byte) value[i];
    return result;
  }
}

record ChunkRecord(String parentName, int order, byte[] content) {}

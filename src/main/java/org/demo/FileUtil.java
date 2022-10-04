package org.demo;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileUtil {

  public static final String RESOURCES_PATH =
      "/Users/eiltpeh/Development/test-area/reactive-grpc/demo-react-mvn/src/main/resources";
  public static final String DOWNLOAD_PATH = RESOURCES_PATH + "/" + "downloads";
  public static final String UPLOAD_PATH = RESOURCES_PATH + "/" + "uploads";

  public static String createRandomFolder(String resourcesPath) throws IOException {
    String folder =
        Files.createDirectory(Paths.get(resourcesPath + "/video" + new Random().nextInt(10)))
            .toString();
    log.info("created folder {}", folder);
    return folder;
  }

  public static void createRandomFile(String path, String offset) throws IOException {
    Path filePath = Paths.get(path + "/" + UUID.randomUUID().toString() + "_" + offset + ".txt");
    log.info("creating file {}", filePath);
    File newFile = Files.createFile(filePath).toFile();
    // write some stuff inside
    Files.writeString(filePath,"hello-"+offset);

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
}

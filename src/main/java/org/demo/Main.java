package org.demo;

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import demo.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.demo.FileUtil.*;

@Slf4j(topic = "HelloDemo")
public class Main {

  protected static ReactorGreeterGrpc.ReactorGreeterStub client;

  AtomicBoolean uploadInProgress = new AtomicBoolean(false);

  public static void main(String[] args) throws IOException, InterruptedException {
    Server server =
        InProcessServerBuilder.forName("ResumeStreamReactorDemo")
            .addService(new HelloServiceImpl())
            .build()
            .start();
    ManagedChannel channel =
        InProcessChannelBuilder.forName("ResumeStreamReactorDemo").usePlaintext().build();
    client = ReactorGreeterGrpc.newReactorStub(channel);
    Main instance = new Main();
    instance.runUploadTask();
    Thread.sleep(13000);
    channel.shutdownNow();
    server.shutdownNow();
  }

  private void runUploadTask() {
    Flux.interval(Duration.ofSeconds(5))
        .doOnNext(a -> log.info("started scheduled interval {}", a))
        .flatMap(this::downloadFiles)
        .flatMap(this::uploadFiles)
        .onErrorContinue((er, r) -> log.warn("error from the upload task, continue stream ", er))
        .subscribeOn(Schedulers.boundedElastic())
        .subscribe();
  }

  private Mono<Long> downloadFiles(Long aLong) {
    log.info("inside download files");
    return Mono.just(aLong)
        .publishOn(Schedulers.boundedElastic())
        .filter(this::readyForDownload)
        //        .delayElement(Duration.ofSeconds(1)) // let's slow down creating files a bit
        .flatMap(this::createFolderAndFiles)
        .onErrorContinue(
            (err, i) -> {
              log.info("error while creating folders or files {}", aLong);
            })
        .thenReturn(aLong);
  }

  private Mono<Void> uploadFiles(Long interval) {
    log.info("upload files stream");
    // here we need stream of upload
    return Mono.fromCallable(() -> this.shouldStartUpload())
        .filter(Boolean::booleanValue)
        .flatMapMany(this::listDir2Flux)
        .groupBy(p -> p.getFileName().toString())
        .concatMap(this::processFiles)
        .then(
            Mono.defer(
                () -> {
                  log.info("completed batch setting flag to false");
                  this.uploadInProgress.getAndSet(false);
                  return Mono.empty();
                }));
  }

  // This function tries to send each of the files under same directory
  private Flux<String> processFiles(GroupedFlux<String, Path> dirFlux) {
    log.info("processing directory {}", dirFlux.key());
    this.uploadInProgress.getAndSet(true);
    return dirFlux
        .publishOn(Schedulers.boundedElastic())
        .flatMap(this::listFiles)
        //            .limitRate(20)
        //        .delayElements(Duration.ofSeconds(2))
        .doOnNext(l -> log.info("processing file {} under dir {}", l.toString(), dirFlux.key()))
        .flatMap(this::readFile)
        .map(
            t -> {
              if (Strings.isNullOrEmpty(t.getT1()) && Strings.isNullOrEmpty(t.getT2())) {
                MediaUploadRequest emptyFolderRequest =
                    MediaUploadRequest.newBuilder().setId(dirFlux.key()).setOffset(-1).build();
                return emptyFolderRequest;
              } else {
                return toMediaRequest(dirFlux.key(), t.getT1(), t.getT2());
              }
            })
        .transform(client::uploadMedia)
        .doOnNext(this::cleanUpFiles)
        .map(MediaUploadResponse::getId);
  }

  private void cleanUpFiles(MediaUploadResponse response) {
    log.info("transferred was completed for file {} ", response.getId());
    try {
      // downloads/vide2
      // downloads/video/...txt
      Files.deleteIfExists(Paths.get(DOWNLOAD_PATH + "/" + response.getId()));
      log.info("file {} is deleted succesfully", response.getId());
    } catch (IOException e) {
      log.error("could not delete the file {}", response.getId(), e);
    }
  }

  private void removeFolder(MediaUploadRequest request) {
    try {
      String path = DOWNLOAD_PATH + "/" + request.getId();
      log.info("DELETE:folder {} as completed", path);
      Files.deleteIfExists(Paths.get(path));
    } catch (IOException e) {
      log.warn("error while deleting folder ", e);
    }
  }

  private MediaUploadRequest toMediaRequest(String dir, String fileName, String content) {
    String mediaId = String.format("%s/%s", dir, fileName);
    long offset = 0;
    try {
      offset = Long.valueOf(fileName.substring(fileName.indexOf("_") + 1, fileName.indexOf(".")));
    } catch (NumberFormatException e) {
      // do nothing keep 0
    }
    return MediaUploadRequest.newBuilder()
        .setId(mediaId)
        .setOffset(offset)
        .setContent(ByteString.copyFromUtf8(content))
        .build();
  }

  private Mono<Tuple2<String, String>> readFile(Optional<Path> path) {
    if (!path.isEmpty()) {
      String filePath = path.get().toAbsolutePath().toString();
      String fileName = path.get().getFileName().toString();
      log.info("reading from file {}", fileName);
      return Mono.using(
          () -> new BufferedReader(new FileReader(filePath)),
          reader ->
              Mono.just(reader.lines().collect(Collectors.joining("\n")))
                  .map(s -> Tuples.of(fileName, s)),
          reader -> {
            try {
              reader.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    }
    return Mono.just(Tuples.of("", ""));
  }

  private Flux<Optional<Path>> listFiles(Path dir) {
    try {
      return Flux.fromIterable(FileUtil.listFilesInDirectory(dir.toAbsolutePath().toString()))
          .map(Optional::of)
          .defaultIfEmpty(Optional.empty());
    } catch (IOException e) {
      log.error("error while listing files under dir {}", dir, e);
      return Flux.error(e);
    }
  }

  private Flux<Path> listDir2Flux(Boolean aBoolean) {
    try {
      return Flux.fromIterable(FileUtil.listDirs(DOWNLOAD_PATH));
    } catch (IOException e) {
      log.error("while listing folders under dir {}", DOWNLOAD_PATH, e);
      return Flux.error(e);
    }
  }

  private boolean shouldStartUpload() {
    // TODO:the circuit logic here
    boolean extraCircuitLogic = true;
    boolean shouldUpload = !this.uploadInProgress.get() && extraCircuitLogic;
    log.info("skipping the upload {}", !shouldUpload);
    return shouldUpload;
  }

  private boolean readyForDownload(Long aLong) {
    // TODO:add logic here for download starting decision
    return true;
  }

  private void startCreatingFiles() {
    Flux<Object> fileFlux =
        Flux.interval(Duration.ofSeconds(10))
            .publishOn(Schedulers.boundedElastic())
            .flatMap(this::createFolderAndFiles);
    fileFlux.subscribeOn(Schedulers.boundedElastic()).subscribe();
  }

  private Mono<Void> createFolderAndFiles(Long aLong) {
    log.info("creating folders and files");
    Set<Path> folderList = null;
    try {
      folderList = listDirs(DOWNLOAD_PATH);
    } catch (IOException e) {
      Mono.error(e);
    }
    int randValue = new Random().nextInt(5);
    if (randValue < 3) { // create files inside th existing folders
      folderList.forEach(
          f -> {
            try {
              createRandomFile(
                  DOWNLOAD_PATH + "/" + f.getFileName().toString(),
                  String.format("%d%d", aLong, randValue));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    }

    // create folder and files
    String folderPath = null;
    try {
      folderPath = createRandomFolder(DOWNLOAD_PATH);
      createRandomFile(folderPath, String.format("%d%d", aLong, randValue));
    } catch (IOException e) {
      System.err.println("error while creating random folder->" + e);
    }

    return Mono.empty();
  }

  private static class HelloServiceImpl extends ReactorGreeterGrpc.GreeterImplBase {
    @Override
    public Flux<HelloResponse> streamGreet(Flux<HelloRequest> request) {
      return request
          .doOnNext(r -> log.info("received message {}", r.getName()))
          .map(
              r ->
                  HelloResponse.newBuilder()
                      .setMessage("REPLY>" + r.getName().toUpperCase())
                      .build());
    }

    @Override
    public Flux<MediaUploadResponse> uploadMedia(Flux<MediaUploadRequest> request) {
      return request
          .<MediaUploadRequest>handle(
              (r, sink) -> {
                log.info(">received {}", r.getId());
                if (r.getOffset() == -1) {
                  log.info(">folder {} completed", r.getId());
                } else {
                  persistContent(r);
                }
                sink.next(r);
              })
          .map(r -> MediaUploadResponse.newBuilder().setId(r.getId()).build());
    }

    private void persistContent(MediaUploadRequest request) {
      Path filePath = Paths.get(UPLOAD_PATH + "/" + request.getId());
      String folderName = request.getId().split("/")[0];
      try {
        Files.createDirectories(Paths.get(UPLOAD_PATH + "/" + folderName));
      } catch (IOException e) {
        log.warn("folder could not be created", e);
        // if no folder then not persisting!
        return;
      }
      try {
        log.info(">saving file to {} and offset {}", filePath.toString(), request.getOffset());
        Files.write(filePath, request.getContent().toByteArray());
      } catch (IOException e) {
        log.error("error while writing file into upload", e);
      }
    }
  }
}

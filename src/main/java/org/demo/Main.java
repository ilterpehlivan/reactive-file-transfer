package org.demo;

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import demo.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.demo.FileUtil.*;

@Slf4j(topic = "HelloDemo")
public class Main {

  protected static ReactorGreeterGrpc.ReactorGreeterStub client;

  AtomicBoolean uploadInProgress = new AtomicBoolean(false);
  private DownloadCallback downloadListener;

  private void registerDownloadListener(DownloadCallback downloadCallback) {
    this.downloadListener = downloadCallback;
  }

  private void generateDownloadFiles() {
    new Thread(
            () -> {
              log.info("start generating downloadable files");
              Set<Path> generatedFiles = generateFiles(0);
              log.info("GENERATED files:{ {} }", generatedFiles);
              //              try {
              //                Thread.sleep(3000);
              //              } catch (InterruptedException e) {
              //                throw new RuntimeException(e);
              //              }
              // notify listener that some files ready in queue
              this.downloadListener.onFiles(
                  generatedFiles.stream()
                      .map(Path::toAbsolutePath)
                      .map(Path::toString)
                      .collect(Collectors.toList()));
            })
        .start();
  }

  private void runUploadTask(Sinks.Many<Flux<String>> filesSink) {
    Flux.interval(Duration.ZERO, Duration.ofSeconds(5))
        .doOnNext(a -> log.info("started scheduled interval {}", a))
        .flatMap(l -> this.downloadStreamOnCallback(l, filesSink))
        .flatMap(this::uploadFiles)
        .onErrorContinue((er, r) -> log.warn("error from the upload task, continue stream ", er))
        .subscribeOn(Schedulers.boundedElastic())
        .subscribe();
  }

  private Mono<Long> createFiles(Long aLong) {
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

  private Flux<Path> downloadStreamOnCallback(Long interval, Sinks.Many<Flux<String>> filesSink) {
    if (readyForDownload(interval)) {
      return filesSink
          .asFlux()
          .publishOn(Schedulers.boundedElastic())
          .flatMap(f -> f)
          .map(Paths::get)
          .flatMap(
              p ->
                  Mono.fromCallable(() -> this.preDownload(p))
                      .flatMapMany(chunkZero -> this.downloadSubFiles(p, chunkZero))
                      .doOnNext(fName -> log.info("downloaded the file {}", fName)))
          //        .delayElement(Duration.ofSeconds(1)) // let's slow down creating files a bit
          .onErrorResume(this::handleDownloadErrors);
    }
    return Flux.just(Paths.get("")); // empty file
  }

  private Publisher<? extends Path> handleDownloadErrors(Throwable error) {
    log.warn("error:while downloading the files, returning empty path", error);
    return Flux.just(Paths.get(""));
  }

  // Copy files from generated folder to download folder
  // Create folder name by parsing the filename
  // returns new path
  private Flux<Path> downloadSubFiles(Path origin, Path chunkZero) {
    Path target = chunkZero.getParent();
    log.info("downloading origin {} to target {}", origin, target);
    // then copy file to target dir
    try {
      Flux<List<Path>> splitFilesFlux =
          Flux.fromIterable(splitFileStream(origin))
              .delayElements(Duration.ofSeconds(1))
              // lets save file to target
              .map(p -> this.save2DownloadFolder(p, target))
              .buffer()
              .transform(transformCleanupMap(origin, target));
      return Flux.concat(Flux.just(chunkZero), splitFilesFlux.flatMap(Flux::fromIterable))
              .doOnNext(p->log.info("SOURCE:{}",p.getFileName().toString()))
              ;
    } catch (IOException e) {
      return Flux.error(e);
    }
  }

  private static Function<Flux<List<Path>>, Publisher<List<Path>>> transformCleanupMap(
      Path origin, Path target) {
    return listFlux ->
        listFlux
            .doOnNext(
                chunks -> {
                  log.info(
                      "completed collecting the splitted-files,now deleting generated file {}",
                      origin);
                  deleteFolder(origin.toAbsolutePath().toString());
                })
            .map(
                listFiles -> {
                  // add folder to the processing list as we need to send completion to remote
                  listFiles.add(target);
                  return listFiles;
                });
  }

  // create the target folder with initial 0 byte of content
  // returns the created target path i.e: /DOWNLOAD/video1
  private Path preDownload(Path origin) {
    // FILE-UUID.txt
    String fileName = origin.getFileName().toString();
    // FILE-UUID
    String folderName = fileName.split("\\.")[0];
    Path target = Paths.get(DOWNLOAD_PATH + "/" + folderName);
    try {
      // first create the container folder in target
      target = Files.createDirectories(target);
      // assign a unique id to the file
      String fileId = getFileNameFromOrigin(fileName);
      // let's first create empty file in target to be transferred right away
      // until full download finishes
      Path filePath = getChunkedPath(target, fileId, 0);
      return Files.createFile(filePath);
    } catch (IOException e) {
      log.error("error while creating dir {}", target.toString(), e);
      throw new RuntimeException(e);
    }
  }

  private static String getFileNameFromOrigin(String fileName) {
    return fileName.substring(fileName.indexOf("-") + 1, fileName.indexOf("."));
  }

  private static Path getChunkedPath(Path target, String fileId, int offset) {
    return Paths.get(target.toAbsolutePath() + "/" + fileId + "_" + offset + ".dat");
  }

  private Path save2DownloadFolder(ChunkRecord chunkRecord, Path target) {
    String fName = target.getFileName().toString();
    String chunkedId = fName.substring(fName.indexOf("-") + 1);
    // copy small file under the target path i.e: /download_folder/<parent_folder_name
    log.info(
        "saving file {} to target {} content size {}",
        chunkedId,
        target,
        chunkRecord.content().toString());
    try {
      return Files.write(
          getChunkedPath(target, chunkedId, chunkRecord.order()), chunkRecord.content());
    } catch (IOException e) {
      log.error("error while writing chunked file to target", e);
    }
    return Paths.get(""); // empty file
  }

  private Flux<ChunkRecord> splitFile(Path path) {

    return Flux.just(path)
        .flatMap(
            p -> {
              try {
                return Flux.fromIterable(FileUtil.splitFileStream(p));
              } catch (IOException e) {
                return Flux.error(e);
              }
            });
  }

  private Mono<Void> uploadFiles(Path uploadFilePath) {
    log.info("upload files stream");
    // if there is already files coming from download then follow this stream
    if (!Strings.isNullOrEmpty(uploadFilePath.toString())) {
      String containerName = uploadFilePath.getParent().getFileName().toString();
      return Mono.fromCallable(() -> this.shouldStartUpload())
          .filter(Boolean::booleanValue)
          .thenMany(Flux.just(Optional.ofNullable(uploadFilePath)))
          .flatMap(this::readFile)
          .map(
              t -> {
                if (Strings.isNullOrEmpty(t.getT1()) && Strings.isNullOrEmpty(t.getT2())) {
                  MediaUploadRequest emptyFolderRequest =
                      MediaUploadRequest.newBuilder().setId(containerName).setOffset(-1).build();
                  return emptyFolderRequest;
                } else {
                  return toMediaRequest(containerName, t.getT1(), t.getT2());
                }
              })
          .transform(client::uploadMedia)
          .doOnNext(this::cleanUpFiles)
          .subscribeOn(Schedulers.boundedElastic())
          .map(MediaUploadResponse::getId)
          .then(
              Mono.defer(
                  () -> {
                    log.info("completed batch setting flag to false");
                    this.uploadInProgress.getAndSet(false);
                    return Mono.empty();
                  }));
    } else { // no download files then check the queued one in directory
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
    String path = DOWNLOAD_PATH + "/" + request.getId();
    FileUtil.deleteFolder(path);
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
    if (!path.isEmpty() && !path.get().toFile().isDirectory()) {
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
      return Flux.fromIterable(this.getDownloadedFilesAndFolder(dir))
          .map(Optional::of)
          .defaultIfEmpty(Optional.empty());
    } catch (IOException e) {
      log.error("error while listing files under dir {}", dir, e);
      return Flux.error(e);
    }
  }

  private Set<Path> getDownloadedFilesAndFolder(Path dir) throws IOException {
    Set<Path> filesPath = listFilesInDirectory(dir.toAbsolutePath().toString());
    // let's add parent folder to send completion
    filesPath.add(dir);
    return filesPath;
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
                  // TODO:here we combine files in remote
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
    // register callbacks
    Sinks.Many<Flux<String>> filesSink = Sinks.many().multicast().onBackpressureBuffer();
    instance.registerDownloadListener(
        files -> {
          log.info("files are ready to download {}", files);
          Sinks.EmitResult emitResult;
          if (files.size() > 0) {
            emitResult = filesSink.tryEmitNext(Flux.fromIterable(files));
          } else {
            emitResult = filesSink.tryEmitError(new EmptyFilesError());
          }
          log.info("files emmited result {}", emitResult.isSuccess());
          return emitResult.isSuccess();
        });
    instance.runUploadTask(filesSink);
    log.info("start generate");
    instance.generateDownloadFiles();
    log.info("after:start generate");
    Thread.sleep(15000);
    channel.shutdownNow();
    server.shutdownNow();
  }

  private static class EmptyFilesError extends Exception {

    public EmptyFilesError() {
      super("No file to download");
    }

    public EmptyFilesError(String msg) {
      super(msg);
    }
  }
}

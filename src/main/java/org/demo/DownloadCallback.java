package org.demo;

import java.util.List;

public interface DownloadCallback {
    boolean onFiles(List<String> files);
}

package ru.nsu.fit.markelov;

import com.google.common.primitives.Bytes;
import lombok.Cleanup;
import lombok.SneakyThrows;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class XmlFileSegmentsDetector {

    private static final int BUFFER_SIZE = 1024;

    private final int segmentsCount;
    private final String fileName;
    private final byte[] tagBytes;

    private final List<Integer> segmentIndexes = new ArrayList<>();

    public XmlFileSegmentsDetector(int segmentsCount, String fileName, String tag) {
        this.segmentsCount = segmentsCount;
        this.fileName = fileName;
        tagBytes = ("<" + tag).getBytes(StandardCharsets.UTF_8);

        init();
    }

    public List<Integer> getSegmentIndexes() {
        return segmentIndexes;
    }

    @SneakyThrows
    private void init() {
        @Cleanup InputStream inputStream = new FileInputStream(fileName);

        int fileLength = inputStream.available();

        int currentPosition = 0;
        byte[] buffer = new byte[BUFFER_SIZE];

        currentPosition += inputStream.read(buffer, 0, BUFFER_SIZE);
        int tagPosition = Bytes.indexOf(buffer, tagBytes);
        if (tagPosition == -1) {
            throw new IOException("Cannot find tag");
        }
        segmentIndexes.add(tagPosition);

        int segmentLength = fileLength / segmentsCount;
        for (int i = 0; i < segmentsCount - 1; i++) {
            currentPosition += inputStream.skip(segmentLength);
            int bytesRead = inputStream.read(buffer, 0, BUFFER_SIZE);
            tagPosition = Bytes.indexOf(buffer, tagBytes);
            if (tagPosition == -1) {
                throw new IOException("Cannot find tag");
            }
            segmentIndexes.add(currentPosition + tagPosition);
            currentPosition += bytesRead;
        }

        segmentIndexes.add(fileLength - 14);
    }
}

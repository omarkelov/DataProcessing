package ru.nsu.fit.markelov;

import lombok.SneakyThrows;

import java.util.List;

public class Main {

    private static final String FILENAME = "src\\main\\resources\\people.xml";

    @SneakyThrows
    public static void main(String[] args) {
        int threadsCount = (int) (2.25f * Runtime.getRuntime().availableProcessors());
        System.out.println(threadsCount + " threads started");

        List<Integer> segmentIndexes = new XmlFileSegmentsDetector(
            threadsCount, FILENAME, "person").getSegmentIndexes();

        new ConcurrentXmlToDataBaseUploader(FILENAME, segmentIndexes).upload();
    }
}

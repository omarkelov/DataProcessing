package ru.nsu.fit.markelov;

import lombok.Cleanup;
import lombok.SneakyThrows;
import ru.nsu.fit.markelov.util.MySizeLimitInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConcurrentXmlToDataBaseUploader {

    private static final String PREFIX = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><ns2:people xmlns:ns2=\"http://fit.nsu.ru/people\">";
    private static final String POSTFIX = "</ns2:people>";

    private static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/people_db";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "admin";

    private static final Map<String, Integer> RELATION_TYPE_MAP = new HashMap<String, Integer>() {{
        put("spouse",   0);
        put("father",   1);
        put("mother",   1);
        put("son",      2);
        put("daughter", 2);
        put("brother",  3);
        put("sister",   3);
    }};

    private final String fileName;
    private final List<Integer> segmentIndexes;

    List<Thread> threads;

    public ConcurrentXmlToDataBaseUploader(String fileName, List<Integer> segmentIndexes) {
        this.fileName = fileName;
        this.segmentIndexes = segmentIndexes;

        recreateTables();
        initThreads();
    }

    @SneakyThrows
    public void upload() {
        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }
    }

    @SneakyThrows
    private void recreateTables() {
        @Cleanup Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        @Cleanup Statement statement = connection.createStatement();

        statement.execute(
            "DROP TABLE IF EXISTS people, relations;" +
            "CREATE TABLE people (" +
            "    id integer NOT NULL," +
            "    name character varying(32) NOT NULL," +
            "    gender boolean NOT NULL" +
            ");" +
            "CREATE TABLE relations (" +
            "    type integer NOT NULL," +
            "    from_id integer NOT NULL," +
            "    to_id integer NOT NULL" +
            ");"
        );
    }

    @SneakyThrows
    private void initThreads() {
        threads = new ArrayList<>();

        for (int i = 0; i < segmentIndexes.size() - 1; i++) {
            final int j = i;
            threads.add(new Thread(() -> runThread(j)));
        }
    }

    @SneakyThrows
    private void runThread(int threadNumber) {
        @Cleanup InputStream prefixInputStream = new ByteArrayInputStream(PREFIX.getBytes());
        @Cleanup InputStream postfixInputStream = new ByteArrayInputStream(POSTFIX.getBytes());
        @Cleanup InputStream peopleInputStream = new FileInputStream(fileName);
        @Cleanup InputStream peopleLimitedInputStream =
            new MySizeLimitInputStream(peopleInputStream, segmentIndexes.get(threadNumber + 1));
        @Cleanup InputStream sequenceInputStream = new SequenceInputStream(
            Collections.enumeration(new ArrayList<InputStream>() {{
                add(prefixInputStream);
                add(peopleLimitedInputStream);
                add(postfixInputStream);
            }})
        );

        if (peopleLimitedInputStream.skip(segmentIndexes.get(threadNumber)) != segmentIndexes.get(threadNumber)) {
            throw new IOException("Skip failed");
        }

        @Cleanup XMLStreamReader xmlStreamReader =
            XMLInputFactory.newInstance().createXMLStreamReader(sequenceInputStream);

        @Cleanup Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        @Cleanup Statement statement = connection.createStatement();

        runThread(xmlStreamReader, statement);
    }

    @SneakyThrows
    private void runThread(XMLStreamReader reader, Statement statement) {
        Integer personId = null;
        for (; reader.hasNext(); reader.next()) {
            if (reader.getEventType() == XMLStreamConstants.START_ELEMENT) {
                switch (reader.getLocalName()) {
                    case "person":
                        String name = null;
                        Boolean gender = null;
                        for (int j = 0; j < reader.getAttributeCount(); j++) {
                            String attrValue = reader.getAttributeValue(j);
                            switch (reader.getAttributeLocalName(j)) {
                                case "id":
                                    personId = Integer.parseInt(attrValue.substring(1));
                                    break;
                                case "name":
                                    name = attrValue;
                                    break;
                                case "gender":
                                    gender = attrValue.startsWith("M");
                                    break;
                            }
                        }
                        statement.execute(String.format(
                            "INSERT INTO people (id, name, gender)" +
                            "VALUES ('%d', '%s', '%b');",
                            personId, name, gender
                        ));
                        break;
                    case "spouse":
                    case "father":
                    case "mother":
                    case "son":
                    case "daughter":
                    case "brother":
                    case "sister":
                        Integer type = RELATION_TYPE_MAP.get(reader.getLocalName());
                        Integer to_id = Integer.parseInt(reader.getAttributeValue(0).substring(1));
                        statement.execute(String.format(
                            "INSERT INTO relations (type, from_id, to_id)" +
                            "VALUES ('%d', '%d', '%d');",
                            type, personId, to_id
                        ));
                        break;
                }
            }
        }
    }
}

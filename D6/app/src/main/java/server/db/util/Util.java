package server.db.util;

import com.google.common.io.Resources;
import lombok.SneakyThrows;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;

public class Util {
  static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/demo";
  static final String DB_USER = "postgres";
  static final String DB_PASSWORD = getPassword("db_password.txt");

  @SneakyThrows
  public static String getPassword(String path) {
    return Resources.toString(Resources.getResource(path), StandardCharsets.UTF_8);
  }

  @SneakyThrows
  public static Connection getConnection() {
    return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
  }
}

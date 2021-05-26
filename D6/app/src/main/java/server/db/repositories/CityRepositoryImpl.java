package server.db.repositories;

import server.db.entities.City;
import server.db.util.Util;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Component
public class CityRepositoryImpl implements CityRepository {

  @Override
  @SneakyThrows
  public List<City> getCities() {
    @Cleanup Connection connection = Util.getConnection();
    @Cleanup Statement statement = connection.createStatement();

    @Cleanup var resultSet = statement.executeQuery("""
        SELECT DISTINCT city FROM airports_data;
        """);
    List<City> list = new ArrayList<>();

    while (resultSet.next()) {
      list.add(new City(resultSet.getString("city")));
    }

    return list;
  }
}

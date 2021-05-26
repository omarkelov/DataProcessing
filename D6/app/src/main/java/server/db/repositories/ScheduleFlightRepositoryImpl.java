package server.db.repositories;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.Cleanup;
import lombok.SneakyThrows;
import server.db.entities.ScheduleFlight;
import server.db.util.Util;
import org.springframework.stereotype.Component;

@Component
public class ScheduleFlightRepositoryImpl implements ScheduleFlightRepository {

  @Override
  @SneakyThrows
  public List<ScheduleFlight> getArrivingFlights(String airport, int dayOfWeek) {
    @Cleanup Connection connection = Util.getConnection();
    @Cleanup Statement statement = connection.createStatement();

    @Cleanup var resultSet = statement.executeQuery(String.format("""
        select distinct flight_no,
        extract (ISODOW from scheduled_arrival) as dow,
        extract (HOUR from scheduled_arrival) as hour,
        extract (MINUTE from scheduled_arrival) as minute,
        extract (SECOND from scheduled_arrival) as second,
        departure_airport,
        (select city from airports_data where airport_code = departure_airport) as departure_city
        from flights
        where arrival_airport = '%s'
        and extract (ISODOW from scheduled_arrival) = %d
        group by flight_no,
        scheduled_arrival, departure_airport
        order by flight_no, dow
        ;
        """, airport, dayOfWeek));
    List<ScheduleFlight> list = new ArrayList<>();

    while (resultSet.next()) {
      ScheduleFlight flight = new ScheduleFlight();
      flight.setDay(resultSet.getInt("dow"));
      flight.setHour(resultSet.getInt("hour"));
      flight.setMinute(resultSet.getInt("minute"));
      flight.setSecond(resultSet.getInt("second"));
      flight.setOtherAirport(resultSet.getString("departure_airport"));
      flight.setNo(resultSet.getString("flight_no"));
      flight.setOtherCity(resultSet.getString("departure_city"));
      list.add(flight);
    }

    return list;
  }

  @Override
  @SneakyThrows
  public List<ScheduleFlight> getDepartingFlights(String airport, int dayOfWeek) {
    @Cleanup Connection connection = Util.getConnection();
    @Cleanup Statement statement = connection.createStatement();

    @Cleanup var resultSet = statement.executeQuery(String.format("""
        select distinct flight_no,
        extract (ISODOW from scheduled_departure) as dow,
        extract (HOUR from scheduled_departure) as hour,
        extract (MINUTE from scheduled_departure) as minute,
        extract (SECOND from scheduled_departure) as second,
        arrival_airport,
        (select city from airports_data where airport_code = arrival_airport)
        from flights
        where departure_airport = '%s'
        and scheduled_departure = %d
        group by flight_no,
        scheduled_departure, arrival_airport
        order by flight_no, dow
        ;
        """, airport, dayOfWeek));
    List<ScheduleFlight> list = new ArrayList<>();

    while (resultSet.next()) {
      ScheduleFlight flight = new ScheduleFlight();
      flight.setDay(resultSet.getInt("dow"));
      flight.setHour(resultSet.getInt("hour"));
      flight.setMinute(resultSet.getInt("minute"));
      flight.setSecond(resultSet.getInt("second"));
      flight.setOtherAirport(resultSet.getString("arrival_airport"));
      flight.setNo(resultSet.getString("flight_no"));
      flight.setOtherCity(resultSet.getString("arrival_city"));
      list.add(flight);
    }

    return list;
  }

}

package server.db.repositories;

import lombok.Cleanup;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;
import server.db.entities.Airport;
import server.db.entities.Path;
import server.db.util.Util;

import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Component
public class PathRepositoryImpl implements PathRepository {

  @Override
  @SneakyThrows
  public List<Path> getPaths(String departurePoint, String arrivalPoint, Date departureDate,
                             int connections, String fareConditions) {
    @Cleanup Connection connection = Util.getConnection();
    @Cleanup Statement statement = connection.createStatement();

    @Cleanup var resultSet = statement.executeQuery(String.format("""
            with recursive r (
            	departure_airport,
            	arrival_airport,
            	flight_path,
            	scheduled_departure,
            	scheduled_arrival,
            	flight_numbers,
              departures,
              arrivals,
              arrival_city,
              amount)
            as (
            	select departure_airport,
            	arrival_airport,
            	array[ad.city ->> 'en'],
            	scheduled_departure,
            	scheduled_arrival,
            	array[flight_id] :: int[],
              array[departure_airport]::char(3)[],
             	array[arrival_airport]::char(3)[],
             	ad2.city ->> 'en',
             	(select amount from flight_price where departure_airport = ad.airport_code and arrival_airport = ad2.airport_code and fare_conditions = '%s')
            	from flights
            	join airports_data ad
            	on departure_airport = ad.airport_code
            	join airports_data ad2
            	on arrival_airport = ad2.airport_code
            	where
            	(departure_airport = '%s' or ad.city ->> 'en' = '%s')
            	and scheduled_departure::date = to_date('%s', 'YYYY-MM-DD')
            	and status = 'Scheduled'
            	and (select count(*) from flight_price where departure_airport = ad.airport_code and arrival_airport = ad2.airport_code and fare_conditions = '%s' ) > 0
                    
            	union all
                    
            	select rec.departure_airport,
            	f.arrival_airport,
            	(rec.flight_path || (ad2.city ->> 'en')),
            	rec.scheduled_departure,
            	f.scheduled_arrival,
            	(rec.flight_numbers || f.flight_id) :: int[],
            	(rec.departures || f.departure_airport)::char(3)[],
             	(rec.arrivals || f.arrival_airport)::char(3)[],
             	ad.city ->> 'en',
             	rec.amount + (select amount from flight_price where departure_airport = ad2.airport_code and arrival_airport = ad.airport_code and fare_conditions = '%s')
            	from flights f
            	join airports_data ad on f.arrival_airport = ad.airport_code
            	join airports_data ad2 on f.departure_airport = ad2.airport_code
            	join r rec on ad2.city ->> 'en' = rec.arrival_city
            	where
            	not ad.city ->> 'en' = any(rec.flight_path)
            	and f.scheduled_departure > rec.scheduled_arrival
            	and f.scheduled_departure - rec.scheduled_arrival < interval '1 day'
            	and cardinality(rec.flight_path) <= %d
            	and (select count(*) from flight_price where departure_airport = ad2.airport_code and arrival_airport = ad.airport_code and fare_conditions = '%s' ) > 0
            ) select * from r
            join airports_data ad
            on arrival_airport = ad.airport_code
            where arrival_airport = '%s' or ad.city ->> 'en' = '%s';
            """, fareConditions, departurePoint, departurePoint,
        new SimpleDateFormat("yyyy-MM-dd").format(departureDate), fareConditions, fareConditions,
        connections, fareConditions, arrivalPoint, arrivalPoint));

    List<Path> res = new ArrayList<>();

    while (resultSet.next()) {
      Path path = new Path();
      path.setFlights(Arrays.asList((Integer[]) resultSet.getArray("flight_numbers").getArray()));

      path.setDepartureAirports(
          Arrays.asList((String[]) resultSet.getArray("departures").getArray()));

      path.setArrivalAirports(
          Arrays.asList((String[]) resultSet.getArray("arrivals").getArray()));

      path.setPrice(resultSet.getInt("amount"));

      List<Airport> points = new ArrayList<>();
      String[] cities = (String[]) resultSet.getArray("flight_path").getArray();
      String[] airports = (String[]) resultSet.getArray("departures").getArray();
      for (int i = 0; i < cities.length; ++i) {
        points.add(new Airport(airports[i], cities[i]));
      }

      points.add(new Airport(resultSet.getString("arrival_airport"), resultSet.getString("arrival_city")));
      path.setPoints(points);

      res.add(path);
    }

    return res;
  }
}

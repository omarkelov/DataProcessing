package server.db.repositories;

import java.util.List;
import server.db.entities.Airport;

public interface AirportRepository {
  List<Airport> getAirports();

  List<Airport> getAirports(String city);
}

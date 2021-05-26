package server.db.repositories;

import java.util.List;
import server.db.entities.ScheduleFlight;

public interface ScheduleFlightRepository {
  List<ScheduleFlight> getArrivingFlights(String airport, int dayOfWeek);
  List<ScheduleFlight> getDepartingFlights(String airport, int dayOfWeek);
}

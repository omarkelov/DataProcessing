package server.db.repositories;

import java.util.Date;
import java.util.List;
import server.db.entities.Path;

public interface PathRepository {
  List<Path> getPaths(String departurePoint, String arrivalPoint, Date departureDate, int connections, String fareConditions);
}

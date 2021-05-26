package server.db.entities;

import lombok.Data;

import java.io.InvalidObjectException;
import java.util.List;

@Data
public class Path {
  List<Integer> flights;
  List<String> departureAirports;
  List<String> arrivalAirports;
  List<Airport> points;
  int price;

  public void validate() throws InvalidObjectException {
      if (flights == null)
          throw new InvalidObjectException("'flights' parameter is not present");
      if (departureAirports == null)
          throw new InvalidObjectException("'departureAirports' parameter is not present");
      if (arrivalAirports == null)
          throw new InvalidObjectException("'arrivalAirports' parameter is not present");
  }
}

package server.db.entities;

import lombok.Data;

import java.io.InvalidObjectException;

@Data
public class Booking {

  private Path path;
  private String passengerId;
  private String passengerName;
  private Contact contact;
  private String fareConditions;

  public void validate() throws InvalidObjectException {
    if (path == null)
      throw new InvalidObjectException("'path' parameter is not present");
    if (passengerId == null)
      throw new InvalidObjectException("'passengerId' parameter is not present");
    if (passengerName == null)
      throw new InvalidObjectException("'passengerName' parameter is not present");
    if (contact == null)
      throw new InvalidObjectException("'contact' parameter is not present");
    if (fareConditions == null)
      throw new InvalidObjectException("'fareConditions' parameter is not present");

    path.validate();
  }
}

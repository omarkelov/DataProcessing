package server.db.entities;

import lombok.Data;
import org.postgresql.geometric.PGpoint;

@Data
public class Airport {

  private String code;
  private String name;
  private String city;
  private String timezone;
  private PGpoint coordinates;

  public Airport() {}

  public Airport(String code, String city) {
    this.code = code;
    this.city = city;
  }
}

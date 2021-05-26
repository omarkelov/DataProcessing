package server.db.repositories;

import server.db.entities.City;

import java.util.List;

public interface CityRepository {
  List<City> getCities();
}

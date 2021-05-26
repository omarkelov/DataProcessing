package server.db.repositories;

import server.db.entities.Booking;

public interface BookingRepository {

  String saveBooking(Booking booking);
}

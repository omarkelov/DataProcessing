package server.db.repositories;

import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.Cleanup;
import lombok.SneakyThrows;
import server.db.entities.Booking;
import server.db.util.Util;
import org.springframework.stereotype.Component;

@Component
public class BookingRepositoryImpl implements BookingRepository {

  @Override
  @SneakyThrows
  public String saveBooking(Booking booking) {
    @Cleanup Connection connection = Util.getConnection();
    try {
      connection.setAutoCommit(false);
      @Cleanup Statement statement = connection.createStatement();

      List<Integer> prices = new ArrayList<>();

      for (int i = 0; i < booking.getPath().getFlights().size(); ++i) {
        @Cleanup var priceResultSet = statement.executeQuery(String.format("""
              select amount from flight_price
              where departure_airport = '%s'
              and arrival_airport = '%s'
              and fare_conditions = '%s';
              """, booking.getPath().getDepartureAirports().get(i), booking.getPath().getArrivalAirports().get(i),
            booking.getFareConditions()));
        if (priceResultSet.next()) {
          int price = priceResultSet.getInt("amount");
          prices.add(price);
        } else {
          // exception?
        }
      }

      @Cleanup PreparedStatement preparedStatement = connection.prepareStatement(String.format("""
        insert into bookings1 (book_ref, book_date, total_amount) values (
        	generate_id(),
        	now(),
        	%d
        );
        """, prices.stream().reduce(0, Integer::sum)), new String[]{"book_ref"});

      preparedStatement.executeUpdate();

      @Cleanup var bookRefResultSet = preparedStatement.getGeneratedKeys();

      String id;
      if (bookRefResultSet.next()) {
        id = bookRefResultSet.getString("book_ref");
      } else {
        return "-1"; // exception?
      }

      // System.out.println(id);

      @Cleanup PreparedStatement preparedStatement1 = connection.prepareStatement(String.format("""
            insert into tickets1 (ticket_no, book_ref, passenger_id, passenger_name, contact_data)
            values (
             	(select lpad(to_char(ticket_no::numeric + 1, 'FM9999999999999'), 13, '0') from tickets1
             	order by ticket_no desc
             	limit 1),
            	'%s',
            	'%s',
            	'%s',
            	'%s'
            );
            """, id, booking.getPassengerId(), booking.getPassengerName(),
          new Gson().toJson(booking.getContact())
      ), new String[]{"ticket_no"});

      preparedStatement1.executeUpdate();

      @Cleanup var ticketResultSet = preparedStatement1.getGeneratedKeys();

      String ticketId;
      if (ticketResultSet.next()) {
        ticketId = ticketResultSet.getString("ticket_no");
      } else {
        return "-1"; // exception?
      }

      // System.out.println(ticketId);
      for (int i = 0; i < booking.getPath().getFlights().size(); ++i) {

        statement.execute(String.format("""
              insert into ticket_flights1 (ticket_no, flight_id, fare_conditions, amount) values (
              	'%s',
              	%d,
              	'%s',
              	%d
              );
              """, ticketId, booking.getPath().getFlights().get(i), booking.getFareConditions(),
            prices.get(i)));
      }

      connection.commit();

      return ticketId;
    } catch (Exception e) {
      e.printStackTrace();
      connection.rollback();
    }

    return null;
  }
}

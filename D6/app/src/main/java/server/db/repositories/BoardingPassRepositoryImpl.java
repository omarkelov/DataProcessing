package server.db.repositories;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import lombok.Cleanup;
import lombok.SneakyThrows;
import server.db.entities.BoardingPass;
import server.db.util.Util;
import org.springframework.stereotype.Component;

@Component
public class BoardingPassRepositoryImpl implements BoardingPassRepository {

  @Override
  @SneakyThrows
  public List<BoardingPass> generateBoardingPasses(String ticketNo) {
    @Cleanup Connection connection = Util.getConnection();

    @Cleanup Statement flightIdsStatement = connection.createStatement();
    @Cleanup var flightIdsResultSet = flightIdsStatement.executeQuery(String.format("""
        select flight_id from ticket_flights1
        where ticket_no = '%s';
        """, ticketNo));

    List<BoardingPass> res = new ArrayList<>();

    while (flightIdsResultSet.next()) {
      int flightId = flightIdsResultSet.getInt("flight_id");

      System.out.println("Flight id: " + flightId);

      @Cleanup PreparedStatement statement = connection.prepareStatement(String.format("""
                insert into boarding_passes1 (ticket_no, flight_id, boarding_no, seat_no) values (
                	'%s',
                	%d,
                	(select case
                         when (select count(boarding_no) from boarding_passes1 where flight_id = %d) = 0
                         then 1
                         else (select boarding_no + 1 from boarding_passes1
                                       where flight_id = %d
                                       order by boarding_no desc
                                       limit 1)
                         end
                ),
                	(select seat_no from seats
                where aircraft_code = (select aircraft_code from flights where flight_id = %d limit 1) and fare_conditions =
                (select fare_conditions from ticket_flights1 tf where tf.ticket_no = '%s' limit 1)
                and not exists (
                select 1 from boarding_passes1 b where seats.seat_no = b.seat_no
                and flight_id = '%s') limit 1)
                )""", ticketNo, flightId, flightId, flightId, flightId, ticketNo, flightId),
          new String[]{"boarding_no", "seat_no"});

      statement.executeUpdate();

      @Cleanup var resultSet = statement.getGeneratedKeys();

      BoardingPass boardingPass = new BoardingPass();
      if (resultSet.next()) {
        boardingPass.setId(resultSet.getInt("boarding_no"));
        boardingPass.setSeat(resultSet.getString("seat_no"));
      } else {
        return null; // exception?
      }

      res.add(boardingPass);
    }

    return res;
  }
}

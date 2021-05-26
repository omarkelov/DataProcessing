package server.db.repositories;

import server.db.entities.BoardingPass;

import java.util.List;

public interface BoardingPassRepository {
  List<BoardingPass> generateBoardingPasses(String ticketNo);
}

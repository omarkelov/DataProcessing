package server.controllers;

import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import server.constants.Mappings;
import server.db.repositories.AirportRepository;

@RestController
@RequestMapping(value = "/", produces = "application/json")
public class AirportRestController {

    @Autowired private AirportRepository airportRepository;

    @GetMapping(Mappings.AIRPORTS)
    public String getAirportsByCity(@RequestParam String city) {
        return new Gson().toJson(city == null ?
            airportRepository.getAirports() :
            airportRepository.getAirports(city));
    }
}

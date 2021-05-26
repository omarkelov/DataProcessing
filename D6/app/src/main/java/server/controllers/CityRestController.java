package server.controllers;

import com.google.gson.Gson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import server.constants.Mappings;
import server.db.repositories.CityRepository;

@RestController
@RequestMapping(value = "/", produces = "application/json")
public class CityRestController {

    @Autowired private CityRepository cityRepository;

    @GetMapping(Mappings.CITIES)
    public String getCities() {
        return new Gson().toJson(cityRepository.getCities());
    }
}

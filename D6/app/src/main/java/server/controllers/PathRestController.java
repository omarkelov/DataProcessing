package server.controllers;

import com.google.gson.Gson;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import server.constants.Mappings;
import server.db.repositories.PathRepository;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

@RestController
@RequestMapping(value = "/", produces = "application/json")
public class PathRestController {

    private static final DateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    @Autowired private PathRepository pathRepository;

    @GetMapping(Mappings.PATHS)
    @SneakyThrows
    public String getPaths(@RequestParam String departurePoint,
                           @RequestParam String arrivalPoint,
                           @RequestParam String departureDate,
                           @RequestParam int connections,
                           @RequestParam String fareConditions) {

        return new Gson().toJson(pathRepository.getPaths(
            departurePoint, arrivalPoint, FORMAT.parse(departureDate), connections, fareConditions));
    }
}

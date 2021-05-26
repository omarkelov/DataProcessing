package server.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.view.RedirectView;
import server.constants.Mappings;

@Controller
@RequestMapping(value = "/", produces = "text/html")
public class MainController {

    @GetMapping("/")
    public RedirectView main() {
        return new RedirectView(Mappings.SCHEDULE_FLIGHTS);
    }

    @GetMapping(Mappings.SCHEDULE_FLIGHTS)
    public String getScheduleFlights() {
        return "schedule-flights";
    }

    @GetMapping(Mappings.PATHS)
    public String getPaths() {
        return "paths";
    }

    @GetMapping(Mappings.BOARDING_PASSES)
    public String getBoardingPasses() {
        return "boarding-passes";
    }
}

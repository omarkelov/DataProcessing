### RESTful API:

* **GET** /cities
  * *status code* **200 OK**
  * *returns* a list of cities

* **GET** /airports?city=`<city>`
  * *status code* **200 OK**
  * *returns* a list of airports for this city

* **GET** /scheduleFlights?arriving=`<true/false>`&airport=`<airport>`&day=`<[1-7]>`
  * *status code* **200 OK**
  * *returns* a list of flights for this airport

* **GET** /paths?departurePoint=`<city/airport>`&arrivalPoint=`<city/airport>`&departureDate=`<yyyy-MM-dd>`&connections=`<[0-]>`&fareConditions=`<Economy/Comfort/Business>`
  * *status code* **200 OK**
  * *returns* a list of paths

* **POST** /booking
  * body: { path, passengerId, passengerName, contact, fareConditions }
  * *status code* **200 OK**
  * *returns* ticket number for the booking

* **GET** /boardingPasses?ticketNo=`<ticketNumber>`
  * *status code* **200 OK**
  * *returns* a list of boarding passes

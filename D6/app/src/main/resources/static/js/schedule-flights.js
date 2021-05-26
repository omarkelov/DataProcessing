let citiesSelect = document.getElementById('cities');
let airportsSelect = document.getElementById('airports');
let typeSelect = document.getElementById('type');
let daySelect = document.getElementById('day');

let flightsNumberH1 = document.getElementById('flights-number');
let flightsOl = document.getElementById('flights');

fetch('/cities', {method: 'GET'})
    .then(response => response.json())
    .then(cities => {
        cityNames = [];
        cities.forEach(city => cityNames.push(JSON.parse(city.name).en));
        cityNames.sort();
        cityNames.forEach(cityName => {
            citiesSelect.appendChild(createOption(cityName));
        });
        fetchAirports();
    });

citiesSelect.onchange = function() {
    fetchAirports();
}

document.getElementById('show-schedule').onclick = function() {
    fetch('/scheduleFlights?arriving=' + typeSelect.value +
                          '&airport=' + airportsSelect.value +
                          '&day=' + daySelect.value,
            {method: 'GET'})
        .then(response => response.json())
        .then(scheduleFlights => {
            flightsNumberH1.innerHTML = 'Found ' + scheduleFlights.length + ' flights';
            flightsOl.innerHTML = '';
            scheduleFlights.forEach(scheduleFlight => {
                console.log(scheduleFlight);
                let li = document.createElement('li');
                li.innerHTML += `${lpadTime(scheduleFlight.hour)}:${lpadTime(scheduleFlight.minute)}`
                li.innerHTML += ` - ${scheduleFlight.no}`
                li.innerHTML += ` - ${JSON.parse(scheduleFlight.otherCity).en} (${scheduleFlight.otherAirport})`
                flightsOl.appendChild(li);
            });
        });

    return false;
};

function fetchAirports() {
    fetch('/airports?city=' + citiesSelect.value, {method: 'GET'})
        .then(response => response.json())
        .then(airports => {
            airportsSelect.innerHTML = '';
            airports.forEach(airport => airportsSelect.appendChild(createOption(airport.code)));
        });
}

function createOption(value) {
    let opt = document.createElement('option');
    opt.value = value;
    opt.innerHTML = value;
    return opt;
}

function lpadTime(n) {
    if (n < 10) {
        return '0' + n;
    }

    return n;
}

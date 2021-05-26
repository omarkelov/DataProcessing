let citiesFromSelect = document.getElementById('cities-from');
let citiesToSelect = document.getElementById('cities-to');
let departureDateInput = document.getElementById('departure-date');
let maxConnectionsInput = document.getElementById('max-connections');
let fareConditionsSelect = document.getElementById('fare-conditions');

let pathsNumberH1 = document.getElementById('paths-number');
let pathsOl = document.getElementById('paths');

let passengerIdInput = document.getElementById('passenger-id');
let passengerNameInput = document.getElementById('passenger-name');
let passengerPhoneInput = document.getElementById('passenger-phone');
let passengerEmailInput = document.getElementById('passenger-email');

fetch('/cities', {method: 'GET'})
    .then(response => response.json())
    .then(cities => {
        cityNames = [];
        cities.forEach(city => cityNames.push(JSON.parse(city.name).en));
        cityNames.sort();
        cityNames.forEach(cityName => {
            citiesFromSelect.appendChild(createOption(cityName));
            citiesToSelect.appendChild(createOption(cityName));
        });
    });

document.getElementById('find-paths').onclick = function() {
    fetch('/paths?departurePoint=' + citiesFromSelect.value +
                '&arrivalPoint=' + citiesToSelect.value +
                '&departureDate=' + departureDateInput.value +
                '&connections=' + maxConnectionsInput.value +
                '&fareConditions=' + fareConditionsSelect.value,
            {method: 'GET'})
        .then(response => response.json())
        .then(paths => {
            pathsNumberH1.innerHTML = 'Found ' + paths.length + ' paths';
            pathsOl.innerHTML = '';
            paths.forEach(path => {
                let li = document.createElement('li');
                for (let i = 0; i < path.points.length; i++) {
                    if (i > 0) li.innerHTML += ' -> ';
                    li.innerHTML += path.points[i].city + ' (' + path.points[i].code + ')';
                }
                li.innerHTML += ' [' + (path.points.length - 2) + ' connections]';
                let buyButton = document.createElement('button');
                buyButton.innerHTML = 'Buy ticket for $' + path.price;
                buyButton.onclick = function() {
                    delete path.points;
                    let booking = {
                        path: path,
                        passengerId: passengerIdInput.value,
                        passengerName: passengerNameInput.value,
                        contact: {
                            phone: passengerPhoneInput.value,
                            email: passengerEmailInput.value
                        },
                        fareConditions: fareConditionsSelect.value
                    };
                    fetch('/booking', {method: 'POST', body: JSON.stringify(booking)})
                        .then(response => response.json())
                        .then(booking => alert('Your ticket number: ' + booking.bookingId));
                }
                li.appendChild(buyButton);
                pathsOl.appendChild(li);
            });
        });

    return false;
};

function createOption(value) {
    let opt = document.createElement('option');
    opt.value = value;
    opt.innerHTML = value;
    return opt;
}

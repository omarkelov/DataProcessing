let bookingCode = prompt('Enter your ticket number:');

if (bookingCode) {
    fetch('/boardingPasses?ticketNo=' + bookingCode, {method: 'GET'})
        .then(response => response.json())
        .then(boardingPasses => {
            let message = '';
            for (let i = 0; i < boardingPasses.length; i++) {
                boardingPass = boardingPasses[i];
                message += 'Flight ' + (i+1) + ':\nYour number: ' + boardingPass.id + '\nYour seat: ' + boardingPass.seat + '\n';
            }
            alert(message);
        });
}

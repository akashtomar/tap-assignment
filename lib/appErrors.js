
//generic error to reject processes
function AppError(message){
    this.message = message;
    this.occuredOn = new Date();
}


module.exports = {AppError};
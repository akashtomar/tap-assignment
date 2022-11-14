const {FtpService} = require('./lib/ftpService');
const {FileService} = require('./lib/fileService');
const {TapProcessor} = require('./lib/tapProcessor');
const {SqlService} = require('./lib/sqlService');

async function start(){
    try{
        const ftpService = new FtpService();
        const fileService = new FileService();
        const sqlService = new SqlService();
        await sqlService.establishConnection();
        await ftpService.connect();
        const tapProcessor = new TapProcessor(ftpService, fileService, sqlService);
        await tapProcessor.processAllFiles();

        ftpService.closeConnection();
        sqlService.closeConnection();
    }catch(ex){
        console.error(ex);
    }
}

start();
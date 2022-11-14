const ftp = require("basic-ftp");
const { AppError } = require("./appErrors");

function FtpService(){
    
    const _client = new ftp.Client();
    
    this.connect = async () => {
        try{
            await _client.access({
                host: "ftp_hostname",
                user: "ftp_username",
                password: "ftp_pass"
            });
            return {success: true};
        } catch(ex) {
            console.error(ex);
            _client = null;
            throw new AppError(ex.message);
        }
    }

    this.listFiles = async () => {
        if(_client){
            return await _client.list("data_files/");
        }else{
            throw new AppError("Not connected to Ftp");
        }
    }
    
    
    this.importFile = async (filename) => {
        if(_client){
            return await _client.downloadTo(`./downloads/${filename}`, `data_files/${filename}`);
        }else{
            throw new AppError("Not connected to Ftp");
        }
    }

    this.closeConnection = () =>{
        if(_client){
            _client.close();
        }else{
            console.warn("Not connected to Ftp");
        }
    }
}

module.exports = {FtpService};

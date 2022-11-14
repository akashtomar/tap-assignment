const fs = require('fs');
const { parse } = require('csv-parse/sync');


function FileService(){
    this.csvToList = (filename)=>{
        return new Promise((resolve, reject)=>{
            fs.readFile(`./downloads/${filename}`, "utf-8", (err, data)=>{
                if(err){
                    return reject({message: err.message});
                }else{
                    let records = parse(data, {
                        columns: true,
                        skip_empty_lines: true
                    });
                    return resolve({message: "success", records});
                }
            });
        });
    }
    
}

module.exports = {FileService};

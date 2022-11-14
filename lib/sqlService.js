const mysql = require('mysql2/promise');
const { AppError } = require('./appErrors');

function SqlService(){
    let _connection;
    this.establishConnection = async ()=>{
        try{
            _connection = await mysql.createConnection({
                host: 'localhost',
                user: 'root',
                password: 'mypass',
                database: 'test'
            });
        }catch(ex){
            _connection = null;
            console.error(ex);
            throw new AppError("Couldn't establish connection");
        }
    }
    this.insertCampaign = async (d) =>{
        if(_connection){
            return await _connection.query("insert into yashi_cgn set ?", {...d});
        }else{
            throw new AppError("No DB connection");
        }
    }
    this.insertCampaignMapData = async (d) =>{
        if(_connection){
            let pList =[]; 
            d.forEach((element) => {
                pList.push(_connection.query("insert into yashi_cgn_data set ?", element));
            });
            return await Promise.allSettled(pList);
        }else{
            throw new AppError("No DB connection");
        }
    }
    this.insertOrder = async (d)=>{
        if(_connection){
            return await _connection.query("insert into yashi_order set ?", {...d});
        }else{
            throw new AppError("No DB connection");
        }
    }
    this.insertOrderMapData = async (d)=>{
        if(_connection){
            let pList = [];
            d.forEach((element)=>{
                pList.push(_connection.query("insert into yashi_order_data set ?", element));
            });
            return await Promise.allSettled(pList);
        }else{
            throw new AppError("No DB connection");
        }
    }
    this.insertCreative = async (d)=>{
        if(_connection){
            return await _connection.query("insert into yashi_creative set ?", {...d});
        }else{
            throw new AppError("No DB connection");
        }
    }
    this.insertCreativeMapData = async (d)=>{
        if(_connection){
            let pList = [];
            d.forEach((element)=>{
                pList.push(_connection.query("insert into yashi_creative_data set ?", element));
            });
            return await Promise.allSettled(pList);
        }else{
            throw new AppError("No DB connection");
        }
    }
    this.closeConnection = () =>{
        if(_connection){
            _connection.close();
        }else{
            console.warn("No DB Connection");
        }
    }
}

module.exports = {SqlService};

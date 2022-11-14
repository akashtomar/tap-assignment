const { AppError } = require('./appErrors');

function TapProcessor(ftp, file, sql){
    
    const createAdvertMap = (list) =>{
        let aMap = new Map();
        for(x of list){
            aMap.set(x['Advertiser ID'], x['Advertiser Name']);
        }
        return aMap;
    }
    
    
    const processFile = async (fileName, advertMap) =>{
        let cgnMap = new Map(), cgnDataMap = new Map();
        let orderMap = new Map(), orderDataMap = new Map();
        let creativeMap = new Map(), creativeDataMap = new Map();
        try{            
            let dataList = await file.csvToList(fileName);
            for(row of dataList['records']){
                if(advertMap.has(row['Advertiser ID'])){ //present in the list of advert
                    //console.log(row);
                    if(!cgnMap.has(row['Campaign ID'])){
                        //new Campaign
                        let [l1res] = await sql.insertCampaign({
                            yashi_campaign_id: row['Campaign ID'], 
                            name: row['Campaign Name'], 
                            yashi_advertiser_id: row['Advertiser ID'], 
                            advertiser_name: row['Advertiser Name']});
                        cgnMap.set(row['Campaign ID'], l1res.insertId);
                        
                        //data
                        let campaignKey = `${parseInt(Date.parse(row['Date']) / 1000)}_${row['Campaign ID']}`;
                        cgnDataMap.set(campaignKey, {
                            campaign_id: l1res.insertId,
                            log_date: parseInt(Date.parse(row['Date']) / 1000),
                            impression_count: parseInt(row['Impressions']), click_count:parseInt( row['Clicks']),
                            "25viewed_count": parseInt(row['25% Viewed']), "50viewed_count": parseInt(row["50% Viewed"]),
                            "75viewed_count": parseInt(row["75% Viewed"]), "100viewed_count": parseInt(row["100% Viewed"])
                        });

                    }else{
                        let campaignKey = `${parseInt(Date.parse(row['Date']) / 1000)}_${row['Campaign ID']}`;
                        cgnDataMap.set(campaignKey, {
                            campaign_id: cgnDataMap.get(campaignKey)['campaign_id'],
                            log_date: parseInt(Date.parse(row['Date']) / 1000),
                            impression_count: parseInt(row['Impressions']) + cgnDataMap.get(campaignKey)['impression_count'], 
                            click_count: parseInt(row['Clicks']) + cgnDataMap.get(campaignKey)['click_count'],
                            "25viewed_count": parseInt(row['25% Viewed']) + cgnDataMap.get(campaignKey)['25viewed_count'], 
                            "50viewed_count": parseInt(row["50% Viewed"]) + cgnDataMap.get(campaignKey)['50viewed_count'],
                            "75viewed_count": parseInt(row["75% Viewed"]) + cgnDataMap.get(campaignKey)['75viewed_count'], 
                            "100viewed_count": parseInt(row["100% Viewed"]) + cgnDataMap.get(campaignKey)['100viewed_count']
                        });
                    }
                    
                    if(!orderMap.has(row['Order ID'])){
                        //new order
                        let [l2res] = await sql.insertOrder({
                            campaign_id: cgnMap.get(row['Campaign ID']),
                            yashi_order_id: row['Order ID'],
                            name: row['Order Name']
                        });
                        orderMap.set(row['Order ID'], l2res.insertId);

                        //data
                        let orderKey = `${parseInt(Date.parse(row['Date']) / 1000)}_${row['Order ID']}`;
                        orderDataMap.set(orderKey, {
                            order_id: l2res.insertId,
                            log_date: parseInt(Date.parse(row['Date']) / 1000),
                            impression_count: parseInt(row['Impressions']), 
                            click_count: parseInt(row['Clicks']),
                            "25viewed_count": parseInt(row['25% Viewed']), 
                            "50viewed_count": parseInt(row["50% Viewed"]), 
                            "75viewed_count": parseInt(row["75% Viewed"]), 
                            "100viewed_count": parseInt(row["100% Viewed"])
                        });
                    }else{
                        let orderKey = `${parseInt(Date.parse(row['Date']) / 1000)}_${row['Order ID']}`;
                        orderDataMap.set(orderKey, {
                            order_id: orderDataMap.get(orderKey)['order_id'],
                            log_date: parseInt(Date.parse(row['Date']) / 1000),
                            impression_count: parseInt(row['Impressions']) + orderDataMap.get(orderKey)['impression_count'], 
                            click_count: parseInt(row['Clicks']) + orderDataMap.get(orderKey)['click_count'],
                            "25viewed_count": parseInt(row['25% Viewed']) + orderDataMap.get(orderKey)['25viewed_count'], 
                            "50viewed_count": parseInt(row["50% Viewed"]) + orderDataMap.get(orderKey)['50viewed_count'],
                            "75viewed_count": parseInt(row["75% Viewed"]) + orderDataMap.get(orderKey)['75viewed_count'], 
                            "100viewed_count": parseInt(row["100% Viewed"]) + orderDataMap.get(orderKey)['100viewed_count']
                        });
                    }

                    if(!creativeMap.has(row['Creative ID'])){
                        //new creative
                        let [l3res] = await sql.insertCreative({
                            order_id: orderMap.get(row['Order ID']),
                            yashi_creative_id: row['Creative ID'],
                            name: row['Creative Name'], preview_url: row['Creative Preview URL']
                        });
                        creativeMap.set(row['Creative ID'], l3res.insertId);

                        //data
                        //let [l3resData] = await sql.insertCreativeData();
                        let creativeKey = `${parseInt(Date.parse(row['Date']) / 1000)}_${row['Creative ID']}`;
                        creativeDataMap.set(creativeKey, {
                            creative_id: l3res.insertId,
                            log_date: parseInt(Date.parse(row['Date']) / 1000),
                            impression_count: parseInt(row['Impressions']), 
                            click_count: parseInt(row['Clicks']),
                            "25viewed_count": parseInt(row['25% Viewed']), 
                            "50viewed_count": parseInt(row["50% Viewed"]), 
                            "75viewed_count": parseInt(row["75% Viewed"]), 
                            "100viewed_count": parseInt(row["100% Viewed"])
                        });

                    }else{
                        let creativeKey = `${parseInt(Date.parse(row['Date']) / 1000)}_${row['Creative ID']}`;
                        creativeDataMap.set(creativeKey, {
                            creative_id: creativeDataMap.get(creativeKey)['creative_id'],
                            log_date: parseInt(Date.parse(row['Date']) / 1000),
                            impression_count: parseInt(row['Impressions']) + creativeDataMap.get(creativeKey)['impression_count'], 
                            click_count: parseInt(row['Clicks']) + creativeDataMap.get(creativeKey)['click_count'],
                            "25viewed_count": parseInt(row['25% Viewed']) + creativeDataMap.get(creativeKey)['25viewed_count'], 
                            "50viewed_count": parseInt(row["50% Viewed"]) + creativeDataMap.get(creativeKey)['50viewed_count'],
                            "75viewed_count": parseInt(row["75% Viewed"]) + creativeDataMap.get(creativeKey)['75viewed_count'], 
                            "100viewed_count": parseInt(row["100% Viewed"]) + creativeDataMap.get(creativeKey)['100viewed_count']
                        });
                    }
                }//endof advert if
            }//endof for loop
            await sql.insertCampaignMapData(cgnDataMap);
            await sql.insertOrderMapData(orderDataMap);
            await sql.insertCreativeMapData(creativeDataMap);
        }catch(ex){
            console.error(ex);
            throw new AppError(ex.message);
        }

    }

    this.processAllFiles = async ()=>{
        try{
            await ftp.importFile("Yashi_Advertisers.csv");
            let advertList = await file.csvToList("Yashi_Advertisers.csv");
            let advertMap = createAdvertMap(advertList['records']);
            let fileList = await ftp.listFiles();
            //console.log(fileList);
            for(f of fileList){
                if(f.name.includes("Yashi_2016-05")){
                    console.log(f.name);
                    await ftp.importFile(f.name);
                    await processFile(f.name, advertMap);
                }
            }
        }catch(ex){
            console.error(ex);
            throw new AppError(ex.message);
        }
    }
}

module.exports = {TapProcessor};
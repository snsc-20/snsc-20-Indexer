const mysql = require('mysql');
const { Provider, constants, RpcProvider, Contract, Account, ec, json, num, hash, encode, SequencerProvider, CallData, shortString, uint256} = require("starknet");


const providerRPC = new RpcProvider({ nodeUrl: "https://..." });

const ContractAddress = "0x0600386e4cd85d7bb925892b61b14ff019d3dd8e31432f4b97c8ee2462e0375d";

let docontinue = true;   

let db;

// async function dbconnection(){
  db = mysql.createPool({
    host: "127.0.0.1",
    user: "root",
    password: "....",
    database: "snsc"
  });
// }
 
async function dbdisconnection(){
  db.end(function(err){  
  if(err){ 
      console.log(err.toString());
      return;  
  }  
  console.log('[connection end] succeed!');  
  });

}


async function query(querys) {
  return new Promise((resolve, reject) => {
    try {
      db.query(querys, function (err, result) {
        if (err) {
          return reject(err)
        }

        return resolve(result);
      });
    }
    catch (e) {
      reject(e)
    }
  });
}



async function doConnection(sql1, param1, sql2, param2, dotype){

  let res =  await new Promise((resolve, reject) => {

    db.getConnection(function(err, connection){
      if (err) { 
         connection.rollback(function() { 
          docontinue = false;
          reject(err);
      }); } 
      connection.beginTransaction(function(err) { 
        if (err) { 
            connection.rollback(function() { 
              docontinue = false;
              reject(err);
          }); } 
          
            connection.query(sql1, param1, (err, results) => {
              if (err) { 
                connection.rollback(function() { 
                  docontinue = false;
                  reject(err);
              }); } 
              
              connection.query(sql2, param2, (err, results) => {
                if (err) { 
                  connection.rollback(function() { 
                    docontinue = false;
                    reject(err);
                }); } 
                  connection.commit(function(err) { 
                    if (err) { 
                      connection.rollback(function() { 
                        docontinue = false;
                        reject(err);
                    }); } 
                    console.log(dotype + ' data insert success!'); 
                    connection.release();  
                    // return {results, success: true}
                    resolve('success');
                  });
                });
              });
            });
          });
        });

    return res;

}

function PrefixStr(str, length) {
  return (Array(length).join('0') + str).slice(-length);
}

function hasUpperCase(str) {
  return /[A-Z]/.test(str);
}

function isLowerCase(str) {
  var regex = /^[a-z]+$/;
  return regex.test(str);
}

function isNumeric(str) {
  var reg = /^\d+$/;
  return reg.test(str);
}

function CheckTick(str){
  if(str.length >=4 && str.length <=10 && isLowerCase(str)){
    return true;
  }else{
    return false;
  }
}
function CheckJson(str_json){
    if(str_json.op == 'deploy'){
      if(str_json.p == 'snsc-20' && CheckTick(str_json.tick) && isNumeric(str_json.max) && isNumeric(str_json.limit)){
        return true
      }else{
        console.log('content wrong')
        return false
      }
    }else if(str_json.op == 'mint'){
      if(str_json.p == 'snsc-20' && CheckTick(str_json.tick) && isNumeric(str_json.amt)){
        return true
      }else{
        console.log('content wrong')
        return false
      }

    }else{
      console.log('content wrong')
      return false
    }
}

const keyFilter = [num.toHex(hash.starknetKeccak("Ins"))];

async function updata_info() {

  let _block_num = 0;
  let _tick_num;
  let querySql_blocknum = 'SELECT * from starknet_blocknum where id =1';
  let rs = await query(querySql_blocknum);
  if(rs.length > 0){
    _block_num = rs[0].blocknum;
    _tick_num = rs[0].ticknum;
  }else{
    return
  }
  console.log('Scanned block num:  ', _block_num)

  // let eventsList;
  let current_block_num = 0;
  try {
    current_block_num = await providerRPC.getBlockNumber()
    console.log('Current block num:', current_block_num)
    if(current_block_num - _block_num > 100){
       current_block_num = _block_num + 100
    }
    console.log('Will scan block num:', current_block_num)

  }catch(e){
    console.log(e);
  }
  if(current_block_num > _block_num){
      // let continuationToken = (_block_num +1).toString() + "-0";
      let continuationToken = "0";
      while (continuationToken) {
        
          try{
              let eventsList;
              if(continuationToken == "0"){
                  eventsList = await providerRPC.getEvents({
                  address: ContractAddress,
                  from_block: {block_number: _block_num +1},
                  to_block: {block_number: current_block_num},
                  keys: [keyFilter],
                  chunk_size: 100
                });
              }else{
                  eventsList = await providerRPC.getEvents({
                  address: ContractAddress,
                  from_block: {block_number: _block_num +1},
                  to_block: {block_number: current_block_num},
                  keys: [keyFilter],
                  chunk_size: 100,
                  continuation_token: continuationToken
                });

              }
              continuationToken=eventsList.continuation_token;
              // console.log(continuationToken);

              if(eventsList == null){
                console.log('wrong read data or block num not reach, return');
                return;
              }
              
              let len = eventsList.events.length;
              if(len == 0){
                console.log('no log on chain');
                // if(current_block_num > _block_num +1){
                if(current_block_num > _block_num){
                  await updata_block_num(current_block_num);
                }
                return;
              }
              for (let i = 0; i < len; i++) {
                  let str_num = parseInt(eventsList.events[i].data[0], 16);
                  let from_address;
                  let to_address;
                  let str;
                  if(str_num == 2){
                      if(eventsList.events[i].data[3] == '0x0'){
                        from_address = encode.sanitizeHex(eventsList.events[i].data[3]);
                      }else{
                        from_address = encode.sanitizeHex(PrefixStr(eventsList.events[i].data[3].substr(2, eventsList.events[i].data[3].length -2), 64));
                      }
                      if(eventsList.events[i].data[4] == '0x0'){
                        to_address = encode.sanitizeHex(eventsList.events[i].data[4]);
                      }else{
                        to_address = encode.sanitizeHex(PrefixStr(eventsList.events[i].data[4].substr(2, eventsList.events[i].data[4].length -2), 64));
                      }
    
                      str = shortString.decodeShortString(eventsList.events[i].data[1]) + shortString.decodeShortString(eventsList.events[i].data[2]);
                  }else if(str_num == 3){
                      if(eventsList.events[i].data[4] == '0x0'){
                        from_address = encode.sanitizeHex(eventsList.events[i].data[4]);
                      }else{
                        from_address = encode.sanitizeHex(PrefixStr(eventsList.events[i].data[4].substr(2, eventsList.events[i].data[4].length -2), 64));
                      }
                      if(eventsList.events[i].data[5] == '0x0'){
                        to_address = encode.sanitizeHex(eventsList.events[i].data[5]);
                      }else{
                        to_address = encode.sanitizeHex(PrefixStr(eventsList.events[i].data[5].substr(2, eventsList.events[i].data[5].length -2), 64));
                      }
    
                      str = shortString.decodeShortString(eventsList.events[i].data[1]) + shortString.decodeShortString(eventsList.events[i].data[2]) + shortString.decodeShortString(eventsList.events[i].data[3]);
                  }
                  
                  
                  let _blocknum_event = eventsList.events[i].block_number;
                  let tx_hash = eventsList.events[i].transaction_hash;
                  let str_js;
                  try{
                    str_js = JSON.parse(str);
                  }catch(e){
                    // console.log(e)
                  }
                  if(str_js){
                    if(CheckJson(str_js)){
                      // if(str_js.p == "snsc-20" && CheckTick(str_js.tick)){
                        if(str_js.p == "snsc-20"){
                          if(str_js.op == 'deploy'){
                            let querySql_txhash = `SELECT * from starknet_ins_info where deploy_tx_hash = '` + tx_hash + `' or tick_name = '` + str_js.tick + `'`;
                            // console.log(querySql_txhash);
                            let rs = await query(querySql_txhash);
                            if(rs.length == 0){
                              let blcok_time;
                              let blockinfo ;
                              try{
                                blockinfo = await providerRPC.getBlockWithTxHashes(_blocknum_event);
                              }catch(e){
                                console.log(e)
                              }
                              // console.log(str_js.tick)
                              let blimit = 0;
                              let decimal = 18;
                              if(str_js.blimit){
                                if(isNumeric(str_js.blimit)){
                                  blimit = str_js.blimit;
                                }
                              }
                              if(str_js.dec){
                                if(isNumeric(str_js.dec)){
                                  decimal = str_js.dec;
                                }
                              }
                              if(blockinfo){
                                blcok_time  = blockinfo.timestamp;
                              }else{
                                blcok_time = 0;
                              }

                              _tick_num = _tick_num +1;
                               
                              let sql1 = 'insert into starknet_ins_info (tick_name, tick_type, token_total_amount, tick_achieve_num,token_achieve_amount,hold_num,tx_max_token,block_tx_max_perone,deploy_address,deploy_tx_hash,deploy_time,complete_time,deploy_blocknum,token_decimal,status) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)';
                              var param1 = [str_js.tick, _tick_num, (parseInt(str_js.max)*parseInt(str_js.limit)).toString(),0,0,0,str_js.limit,blimit,from_address,tx_hash,blcok_time,0,_blocknum_event,decimal,0];
                              let sql2 = 'update starknet_blocknum set ticknum = ? where id =1';
                              let param2 = [_tick_num];
                              console.log('do deploy', continuationToken, i);
                              
                              let resul = await doConnection(sql1, param1, sql2, param2, 'deploy'+i);
                              // console.log(resul);
                              if(resul !='success'){
                                console.log('do sql fail, return to repair');
                                console.log(continuationToken);
                                return
                              }
                              
                            }else{
                              console.log('skip duplicate deploy');
                            }
                            
                          }else if(str_js.op == 'mint'){
                            
                            let querySql_txhash = `SELECT * from starknet_ins_info where tick_name = '` + str_js.tick + `'`;
                            let rs = await query(querySql_txhash);
                            if(rs.length >0 && parseInt(str_js.amt)<= rs[0].tx_max_token){//has record, and amt <= limt per mint
                              if(rs[0].deploy_blocknum<=parseInt(_blocknum_event) && rs[0].status<2){//judge block, judge mint status
                                
                                let add_num = parseInt(str_js.amt);
                                let conplete_time;
                                if(parseInt(rs[0].token_total_amount) - parseInt(rs[0].token_achieve_amount) < str_js.amt){
                                  add_num = parseInt(rs[0].token_total_amount) - parseInt(rs[0].token_achieve_amount);
                                  let blockinfo;
                                  try{
                                      blockinfo = await providerRPC.getBlockWithTxHashes(_blocknum_event);
                                    }catch(e){
                                      console.log(e)
                                    }
                                  if(blockinfo){
                                    conplete_time  = blockinfo.timestamp;
                                  }else{
                                    conplete_time = 0;
                                  }
                                }
                                let tick_types = rs[0].tick_type;
                                let querySql_txhash2 = `SELECT * from starknet_ins where tick_type = '` + tick_types + `' and owner = '` + from_address + `'`;
                                let rs2 = await query(querySql_txhash2);
                                if(rs2.length > 0){//has record
                                  //judge mutilcall, block num , block limit
                                  if(rs2[0].latest_tx_hash != tx_hash ){
                                    let sql1;
                                    let param1;
                                        if(parseInt(_blocknum_event) == rs2[0].latest_blocknum){//same block num，judge tx max
                                            if(rs[0].block_tx_max_perone == 0 || rs[0].block_tx_max_perone > rs2[0].latest_tx_tick_total){//no limit or not achieve limit
                                              
                                              sql1 = 'update starknet_ins set latest_tx_hash=?, latest_tx_tick_total=?, total_tick_num=?, amount=? where tick_type=? and owner=?';
                                              param1 = [tx_hash, (rs2[0].latest_tx_tick_total + 1), (rs2[0].total_tick_num + 1), (parseInt(rs2[0].amount) + add_num).toString(), tick_types, from_address];
                                              console.log('same block num，not achieve limit')
                                            }
                                        }else if(parseInt(_blocknum_event) > rs2[0].latest_blocknum){
                                              sql1 = 'update starknet_ins set latest_tx_hash=?, latest_blocknum=?, latest_tx_tick_total=?, total_tick_num=?, amount=? where tick_type=? and owner=?';
                                              param1 = [tx_hash, parseInt(_blocknum_event), 1, (rs2[0].total_tick_num + 1), (parseInt(rs2[0].amount) + add_num).toString(), tick_types, from_address];
                                              // console.log('new block')
                                        }
                                        if(param1){
                                          let sql2;
                                          let param2;
                                          if(add_num < parseInt(str_js.amt)){//if the last mint, update mint status
                                            sql2 = 'update starknet_ins_info set token_achieve_amount = ?, tick_achieve_num=?, complete_time=?, status=? where tick_name =?';
                                            param2 = [rs[0].token_total_amount, (rs[0].tick_achieve_num+1), conplete_time,2, str_js.tick];
                                            console.log('last mint');
                                          }else{
                                            sql2 = 'update starknet_ins_info set token_achieve_amount = ?, tick_achieve_num=? where tick_name =?';
                                            param2 = [(parseInt(rs[0].token_achieve_amount) +add_num).toString(), (rs[0].tick_achieve_num+1), str_js.tick];
                                            
                                          }
                                          
                                          console.log('do mint', continuationToken, i);
                                          let resul = await doConnection(sql1,param1,sql2,param2,'mint'+i);
                                          // console.log(resul);
                                          if(resul !='success'){
                                            console.log('do sql fail, return to repair');
                                            console.log(continuationToken);
                                            return
                                        }
                                          
                                      }
                                  }
                                }else{//no record, insert new user
                                  let sql1 = 'insert into starknet_ins (tick_type, owner, latest_tx_hash, latest_blocknum, latest_tx_tick_total, total_tick_num, amount) value(?,?,?,?,?,?,?)';
                                  let param1 = [tick_types, from_address, tx_hash, parseInt(_blocknum_event), 1, 1, add_num.toString()];
                                  let sql2;
                                  let param2;
                                  if(add_num < parseInt(str_js.amt)){//if the last mint, update mint status
                                    sql2 = 'update starknet_ins_info set token_achieve_amount = ?, tick_achieve_num=?, hold_num=?, complete_time=?, status=? where tick_name =?';
                                    param2 = [rs[0].token_total_amount, (rs[0].tick_achieve_num+1), (rs[0].hold_num+1), conplete_time, 2, str_js.tick];

                                  }else{
                                    sql2 = 'update starknet_ins_info set token_achieve_amount = ?, tick_achieve_num=?, hold_num=? where tick_name =?';
                                    param2 = [(parseInt(rs[0].token_achieve_amount) +add_num).toString(), (rs[0].tick_achieve_num+1), (rs[0].hold_num+1), str_js.tick];
                                  }

                                  console.log('do mint new user', continuationToken, i);
                                  let resul = await doConnection(sql1,param1,sql2,param2,'mint'+i);
                                  // console.log(resul);
                                  if(resul !='success'){
                                    console.log('do sql fail, return to repair');
                                    console.log(continuationToken);
                                    return
                                  }
                                }
                              }
                            }


                          }else if(str_js.op == 'transfer'){

                          }
                      }
                    }else{
                      console.log('wrong content, need to skip');
                    }
                  }else{
                    console.log('wrong json, need to skip');
                    // continue
                  }
                  
                }
          }catch(e){
            console.log(e);
          }
    }//while
    await updata_block_num(current_block_num);
    return
  }else{
    console.log('block num not high enough');
    return
  }

}


async function updata_block_num(current_block_num){
  let upgradeSql = 'update starknet_blocknum set blocknum=?';
      let params = [current_block_num];

      db.query(upgradeSql, params, (err, results) => {
        if (err) return console.log(err.message) 
        if (results.affectedRows === 1) {
          console.log('block num update success')    
        }
      });
}

let doing = async () => {
  if(docontinue){
    await updata_info();
  }else{
    console.log('worng happen，stop do');
  }
  
  console.log(new Date(Date.now()));
}

// doing();
setInterval(doing, 120 * 1000); 




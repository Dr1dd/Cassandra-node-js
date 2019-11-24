const cassandra = require('cassandra-driver');

const client = new cassandra.Client({ 
  contactPoints: ['192.168.0.105'],
  localDataCenter: 'datacenter1',
  keyspace: 'parking'
});

client.connect(function (err) {
  console.log(err);
  console.log("success");
});




//createTables();
//insertData();
//getAllTableData();

//getTicketsByCustomerId(1);
//getTicketsByCustomerIdAndTicketId(1, 3);
//getTicketsByTicketId(1);

//getCustomerByParkingLotId(1);
//getCustomerbyCustId(1);

//getParkingLotInfoById(1);


function createTables(){
  var createCustomersTableByPId = 'CREATE TABLE IF NOT EXISTS parking.customers_by_parkingId (p_lot_id decimal, id decimal, name text, PRIMARY KEY (p_lot_id, id))';
  var createCustomersTableByCustId= 'CREATE TABLE IF NOT EXISTS parking.customers_by_custId (id decimal, p_lot_id decimal, name text, PRIMARY KEY (id, name))';
 
  var createTicketsTableByCustId = 'CREATE TABLE IF NOT EXISTS parking.parking_ticket_by_customerid (customer_id decimal, id decimal, car_brand text, car_model text, ticket_cost decimal, checkin_time decimal, validity_duration decimal, PRIMARY KEY (customer_id, id))';
  var createTicketsTableByCustIdandTicketId = 'CREATE TABLE IF NOT EXISTS parking.parking_ticket_by_customerid_and_ticketId (customer_id decimal, id decimal, car_brand text, car_model text, ticket_cost decimal, checkin_time decimal, validity_duration decimal, PRIMARY KEY ((customer_id, id), ticket_cost))';
  var createTicketsTableByTicketId = 'CREATE TABLE IF NOT EXISTS parking.parking_ticket_by_ticketid (id decimal, customer_id decimal, car_brand text, car_model text, ticket_cost decimal, checkin_time decimal, validity_duration decimal, PRIMARY KEY (id, customer_id))';
 
  var createParkingLotTable ='CREATE TABLE IF NOT EXISTS parking.parking_lot_by_id (p_lot_id decimal, city text, street text, PRIMARY KEY (p_lot_id))';
  
  client.execute(createCustomersTableByPId);
  client.execute(createCustomersTableByCustId);

  client.execute(createTicketsTableByCustId);
  client.execute(createTicketsTableByCustIdandTicketId);
  client.execute(createTicketsTableByTicketId);

  client.execute(createParkingLotTable);

}
function insertData(query, params){
  //FOR CUSTOMERS TABLE
  const queryInsertCustomers = 'INSERT INTO parking.customers_by_parkingId (p_lot_id, id, name) VALUES (?, ?, ?) IF NOT EXISTS';
  const queryInsertCustomersByCustId = 'INSERT INTO parking.customers_by_custId (p_lot_id, id, name) VALUES (?, ?, ?) IF NOT EXISTS';
  
   //FOR TICKET TABLE
  const queryInsertTicket = 'INSERT INTO parking.parking_ticket_by_customerid (customer_id, id, car_brand, car_model, ticket_cost, checkin_time, validity_duration) VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS';
  const queryInsertTicketByCIandTId = 'INSERT INTO parking.parking_ticket_by_customerid_and_ticketId(customer_id, id, car_brand, car_model, ticket_cost, checkin_time, validity_duration) VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS';
  const queryInsertTicketByTId= 'INSERT INTO parking.parking_ticket_by_ticketid(id, customer_id, car_brand, car_model, ticket_cost, checkin_time, validity_duration) VALUES (?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS';
  
   //FOR PARKING LOT TABLE
  const queryInsertParking = 'INSERT INTO parking.parking_lot_by_id (p_lot_id, city, street) VALUES (?, ?, ?) IF NOT EXISTS';

   //PARAMETERS FOR CUSTOMERS TABLE
  const paramsCustomers = [1, 1, 'John'];
  const paramsCustomersByCustId = [1, 1, 'John'];

   //PARAMETERS FOR TICKET TABLE
  const paramsTicket = [1, 3, 'Volkswagen', 'Jetta', 315, 25, 15];
  const paramsTicketIdsSwapped = [3, 1, 'Volkswagen', 'Jetta', 315, 25, 15];

   //PARAMETERS FOR PARKING LOT TABLE
  const paramsParkingLot = [1, 'Vilnius', 'Naugarduko 24'];

  //FOR CUSTOMERS TABLE
  client.execute(queryInsertCustomers, paramsCustomers, { prepare: true }).then(result => console.log(result));
  client.execute(queryInsertCustomersByCustId, paramsCustomersByCustId, { prepare: true }).then(result => console.log(result));
  
  //FOR TICKET TABLE
  client.execute(queryInsertTicket, paramsTicket, { prepare: true }).then(result => console.log(result));
  client.execute(queryInsertTicketByCIandTId, paramsTicket, { prepare: true }).then(result => console.log(result));
  client.execute(queryInsertTicketByTId, paramsTicketIdsSwapped, { prepare: true }).then(result => console.log(result));
    
  //FOR PARKING LOT TABLE
  client.execute(queryInsertParking, paramsParkingLot, { prepare: true }).then(result => console.log(result));

}

//TICKET QUERIES
function getTicketsByCustomerId(customer_id){
  var query = 'SELECT * FROM parking.parking_ticket_by_customerid WHERE customer_id = '+customer_id+'ORDER BY id';
  client.execute(query).then(result => {
       console.log("Customer id | Ticket id | car_brand | car_model | checkin_time | ticket_cost | validity_duration");
      for(let i = 0; i < result.rowLength;i++){
        console.log(result.rows[i].customer_id + " | "+result.rows[i].id + " | " +result.rows[i].car_brand + " | " + result.rows[i].car_model + " | " + result.rows[i].checkin_time + " | " + result.rows[i].ticket_cost + " | " + result.rows[i].validity_duration);
     }
  });

}
function getTicketsByTicketId(ticket_id){
   var query = 'SELECT * FROM parking.parking_ticket_by_ticketid WHERE id = '+ticket_id+'ORDER BY customer_id';
  client.execute(query).then(result => {
       console.log("Ticket id | Customer id | car_brand | car_model | checkin_time | ticket_cost | validity_duration");
      for(let i = 0; i < result.rowLength;i++){
        console.log(result.rows[i].id + " | "+result.rows[i].customer_id + " | " +result.rows[i].car_brand + " | " + result.rows[i].car_model + " | " + result.rows[i].checkin_time + " | " + result.rows[i].ticket_cost + " | " + result.rows[i].validity_duration);
     }
  });
}
function getTicketsByCustomerIdAndTicketId(customer_id, ticket_id){
  var query = 'SELECT * FROM parking.parking_ticket_by_customerid_and_ticketId WHERE customer_id = '+customer_id+' AND id = '+ticket_id+' ORDER BY ticket_cost';
  client.execute(query).then(result => {
       console.log("Customer id | Ticket id | car_brand | car_model | checkin_time | ticket_cost | validity_duration");
      for(let i = 0; i < result.rowLength;i++){
        console.log(result.rows[i].customer_id + " | "+result.rows[i].id + " | " +result.rows[i].car_brand + " | " + result.rows[i].car_model + " | " + result.rows[i].checkin_time + " | " + result.rows[i].ticket_cost + " | " + result.rows[i].validity_duration);
     }
  });
}

//CUSTOMER QUERIES
function getCustomerByParkingLotId(p_lot_id){
  var query = 'SELECT * FROM parking.customers_by_parkingId WHERE p_lot_id = '+p_lot_id+' ORDER BY id';
  client.execute(query).then(result => {
      console.log("Parking lot id | Customer id |  Name");
      for(let i = 0; i < result.rowLength;i++){
        console.log(result.rows[i].p_lot_id + " | "+result.rows[i].id + " | " + result.rows[i].name);
     }
  });
}
function getCustomerbyCustId(customer_id){
   var query = 'SELECT * FROM parking.customers_by_custId WHERE id = '+customer_id+' ORDER BY name';
  client.execute(query).then(result => {
      console.log("Customer ID | Parking lot id |  Name");
      for(let i = 0; i < result.rowLength;i++){
        console.log(result.rows[i].id + " | "+result.rows[i].p_lot_id + " | " + result.rows[i].name);
     }
  });
}

//PARKING LOT QUERIES
function getParkingLotInfoById(p_lot_id) {
    var query = 'SELECT * FROM parking.parking_lot_by_id WHERE p_lot_id = '+p_lot_id;
  client.execute(query).then(result => {
      console.log("Parking lot id | City |  Street");
      for(let i = 0; i < result.rowLength;i++){
        console.log(result.rows[i].p_lot_id + " | "+result.rows[i].city + " | " + result.rows[i].street);
     }
  });
}


//OTHER
function getAllTableData() {
  var queryCust = 'SELECT * FROM parking.customers_by_parkingId';
  var queryTicket = 'SELECT * FROM parking.parking_ticket_by_customerid';
  var queryParking = 'SELECT * FROM parking.parking_lot_by_id';
  client.execute(queryCust).then(result=>{
  console.log("Customers Table");
  console.log("Parking lot id | customer id | name");
    for(let i = 0 ; i < result.rowLength; i++){
      var jsonCustomers = { parking_lot_id: result.rows[i].p_lot_id, customer_id: result.rows[i].id, name: result.rows[i].name };
      var str = JSON.stringify(jsonCustomers, null, 2);
      console.log(str);
    }
    console.log("");

  });
    client.execute(queryTicket).then(result=>{
    console.log("Ticket Table");
    console.log("customer id | ticked id | car brand | car model | checkin time | validityduration");
    for(let i = 0 ; i < result.rowLength; i++){
      var jsonTicket = { customer_id: result.rows[i].customer_id, ticket_id: result.rows[i].id, car_brand: result.rows[i].ticket_id, car_model: result.rows[i].name, checkin_time: result.rows[i].checkin_time, ticket_cost: result.rows[i].ticket_cost, validity_duration: result.rows[i].validity_duration };
      var str = JSON.stringify(jsonTicket, null, 2);
      console.log(str);
    }
    console.log("");

  });
    client.execute(queryParking).then(result=>{
    console.log("Parking Lot Table");
    console.log("Parking lot id | city | street");
    for(let i = 0 ; i < result.rowLength; i++){
      var jsonParking = { p_lot_id: result.rows[i].id, city: result.rows[i].city, street: result.rows[i].street };
      var str = JSON.stringify(jsonParking, null, 2);
      console.log(str);
    }
    console.log("");
  });


}

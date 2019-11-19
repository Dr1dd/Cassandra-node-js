const cassandra = require('cassandra-driver');

const client = new cassandra.Client({ 
  contactPoints: ['172.24.2.87'],
  localDataCenter: 'datacenter1',
  keyspace: 'parking'
});

client.connect(function (err) {
  console.log(err);
  console.log("success");
});

const queryInsertCustomers = 'INSERT INTO parking.customers (id, ticket_id, name) VALUES (?, ?, ?) IF NOT EXISTS';
const queryInsertTicket = 'INSERT INTO parking.parking_ticket (id, car_brand, car_model, ticket_cost, checkin_time, validity_duration) VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS';
const queryInsertParking = 'INSERT INTO parking.parking_lot (id, customer_id, city, street) VALUES (?, ?, ?, ?)';

const paramsCustomers = [2, 4, 'John'];
const paramsTicket = [4, 'BMW', '520', 35, 245, 80];
const paramsParkingLot = [1, 2, 'Vilnius', 'Naugarduko 24'];

var querySelect = 'SELECT * FROM parking.customers';
var querySelectParkingLot = 'SELECT * FROM parking.parking_lot WHERE id = 1';


// client.execute(querySelect).then(result=>console.log(result));
//createTables();
//insertData(queryInsertTicket, paramsTicket);

//getCustomerById(1);
//getTicketDataFromCustomers(querySelect);
//getDataLotsToCustomers(querySelectParkingLot);
getAllTableData();

//truncateTables();
//dropTables();
function createTables(){
  var createCustomersTable = 'CREATE TABLE IF NOT EXISTS parking.customers (id decimal, ticket_id decimal, name text, PRIMARY KEY (id, ticket_id))';
  var createTicketsTable = 'CREATE TABLE IF NOT EXISTS parking.parking_ticket (id decimal, car_brand text, car_model text, ticket_cost decimal, checkin_time decimal, validity_duration decimal, PRIMARY KEY (id))';
  var createParkingLotTable ='CREATE TABLE IF NOT EXISTS parking.parking_lot (id decimal, customer_id decimal, city text, street text, PRIMARY KEY (id, customer_id))';

  client.execute(createCustomersTable);
  client.execute(createTicketsTable);
  client.execute(createParkingLotTable);

}
function insertData(query, params){
    client.execute(query, params, { prepare: true }).then(result => console.log(result));

}
// function getData(query, ticket_id){
//   client.execute(query)
//     .then(result => {

//       console.log("id  car_brand  car_model  checkin_time  ticket_cost  validity_duration");

//         //console.log('User has ticket: %s', result.rows[i].ticket_id);
//         var queryTicket = 'SELECT * FROM parking.parking_ticket WHERE id = ?';
//         var queryParam = [result.rows[ticket_id].ticket_id];
//         client.execute(queryTicket, queryParam).then(result => {
//           console.log(result.rows[0].id + " " +result.rows[0].car_brand + " " + result.rows[0].car_model + " " + result.rows[0].checkin_time + " " + result.rows[0].ticket_cost + " " + result.rows[0].validity_duration);
//         });
//     }
// }
function getCustomerById(customer_id){
  var query = 'SELECT * FROM parking.customers WHERE id = '+customer_id;
  client.execute(query).then(result => {
    var ticketArray = [];
    for(let i = 0; i < result.rowLength;i++){
      ticketArray.push(result.rows[i].ticket_id);
    }
      console.log("id | Ticket ids |  name");
      console.log(result.rows[0].id + " | "+ticketArray + " | " + result.rows[0].name);
  });
}
function getTicketDataFromCustomers(query){
  client.execute(query)
    .then(result => {
      console.log("id  car_brand  car_model  checkin_time  ticket_cost  validity_duration");
      for (let i = 0; i < result.rowLength; i++){

        //console.log('User has ticket: %s', result.rows[i].ticket_id);
        var queryTicket = 'SELECT * FROM parking.parking_ticket WHERE id = ?';
        var queryParam = [result.rows[i].ticket_id];
        client.execute(queryTicket, queryParam).then(result => {
          console.log(result.rows[0].id + " " +result.rows[0].car_brand + " " + result.rows[0].car_model + " " + result.rows[0].checkin_time + " " + result.rows[0].ticket_cost + " " + result.rows[0].validity_duration);
        });
      }
    });
}
function getDataLotsToCustomers(query){
  client.execute(query)
    .then(result => {
      console.log("id  customer_id  city  street");
      for (let i = 0; i < result.rowLength; i++){
        console.log(result.rows[i].id + " "+result.rows[i].customer_id + " " + result.rows[i].city+ " "+ result.rows[i].street);
        var queryCustomer = 'SELECT * FROM parking.customers WHERE id = ?';
        var queryParam = [result.rows[i].customer_id];
        client.execute(queryCustomer, queryParam).then(result => {
           console.log(result.rows[0].id + " " +result.rows[0].ticket_id + " " + result.rows[0].name);

      });
    }
  });
}
function getAllTableData() {
  var queryCust = 'SELECT * FROM parking.customers';
  var queryTicket = 'SELECT * FROM parking.parking_ticket';
  var queryParking = 'SELECT * FROM parking.parking_lot';
  client.execute(queryCust).then(result=>{
  console.log("Customers Table");
  console.log("id | ticket id | name");
    for(let i = 0 ; i < result.rowLength; i++){
      var jsonCustomers = { id: result.rows[i].id, ticketId: result.rows[i].ticket_id, name: result.rows[i].name };
      var str = JSON.stringify(jsonCustomers, null, 2);
      console.log(str);
    }
    console.log("");

  });
    client.execute(queryTicket).then(result=>{
    console.log("Ticket Table");
    console.log("id | car bran | car model | checkin time | validityduration");
    for(let i = 0 ; i < result.rowLength; i++){
      var jsonTicket = { id: result.rows[i].id, car_brand: result.rows[i].ticket_id, car_model: result.rows[i].name, checkin_time: result.rows[i].checkin_time, ticket_cost: result.rows[i].ticket_cost, validity_duration: result.rows[i].validity_duration };
      var str = JSON.stringify(jsonTicket, null, 2);
      console.log(str);
    }
    console.log("");

  });
      client.execute(queryParking).then(result=>{
    console.log("Parking Lot Table");
    console.log("id | customer id | city | street");
    for(let i = 0 ; i < result.rowLength; i++){
      var jsonParking = { id: result.rows[i].id, customer_id: result.rows[i].customer_id, city: result.rows[i].city, street: result.rows[i].street };
      var str = JSON.stringify(jsonParking, null, 2);
      console.log(str);
      //console.log(result.rows[i].id + " | " + result.rows[i].customer_id + " | " + result.rows[i].city + " | "+ result.rows[i].street);
    }
    console.log("");
  });


}
function truncateTables(){
  var truncateCustomers = 'TRUNCATE TABLE parking.customers';
  var truncateTickets = 'TRUNCATE TABLE parking.parking_ticket';
  var truncateParkingLot = 'TRUNCATE TABLE parking.parking_lot';

  client.execute(truncateCustomers);
  client.execute(truncateTickets);
  client.execute(truncateParkingLot);

}
function dropTables(){
  var dropCustomers = 'DROP TABLE IF EXISTS parking.customers';
  var dropTickets = 'DROP TABLE IF EXISTS parking.parking_ticket';
  var dropParkingLot = 'DROP TABLE IF EXISTS parking.parking_lot';

  client.execute(dropCustomers);
  client.execute(dropTickets);
  client.execute(dropParkingLot);

}
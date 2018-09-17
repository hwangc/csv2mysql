var mysql = require("promise-mysql")
var csv = require("fast-csv")
var fs = require("fs")

var queryHandle = (connection, setData, dbTable) => {
  for (let i = 0, p = Promise.resolve(0); i < setData.length; i++) {
    p = p.then(row => new Promise(resolve => {
      connection.query("INSERT INTO " + dbTable + " SET ?", setData[i], (error, results, fields) => {
        if (results && results.affectedRows >= 1) {
          resolve(++row)
        } else if (error) {
          console.log("Duplication: ", error)
          resolve(row)
        }
      })
    }))

    if (i == setData.length - 1)
      return p
  }
}

function csv2stream(file, csvConfig) {
  return new Promise((resolve, reject) => {  // create stream
    var header = []
    var totalSetValues = []
    var isHeader = true
    fs.createReadStream(file, { autoClose: true })
      .pipe(csv(csvConfig.csv))
      .on("data", function (row) {
        if (isHeader) {
          header = row
          isHeader = false
        }
        else {
          var setValue = {}
          header.forEach((column, c) => {
            setValue[column] = row[c]
          })
          totalSetValues.push(setValue)
        }
      })
      .on("end", function () {
        if (totalSetValues <= 0)
          reject("No data to insert")
        resolve(totalSetValues)
      })
  })
}

function csv2mysql(options, file) {
  return new Promise((resolve, reject) => {
    let result = optionValidatation(options, file);
    (result != "success") ? reject(result) : resolve();
  })
    .then(() => {
      return mysql.createConnection({
        host: options.host,
        user: options.user,
        password: options.password,
        database: options.database,
        multipleStatements: true
      }).catch((e) => {
        return "MySql Connection Error: \n" + e
      })
    })
    .then((conn) => {
      return csv2stream(file, options)
        .then((data) => {
          return { data, conn }
        })
        .catch((e) => {
          return "csv2stream cannot get the data: \n" + e
        })
    })
    .then((streamResult) => {
      return queryHandle(streamResult.conn, streamResult.data, options.table)
        .then((rows) => {
          return rows
        })
    })
    .catch((e) => {
      console.log(e)
    })
}

function optionValidatation(options, file) {
  var msg = "success"

  if (!fs.existsSync(file))
    msg = 'file "' + file + '" not found'
  if (!options.hasOwnProperty('host'))
    msg = 'need config param "host"'
  if (!options.hasOwnProperty('user'))
    msg = 'need config param "user"'
  if (!options.hasOwnProperty('password'))
    msg = 'need config param "password"'
  if (!options.hasOwnProperty('database'))
    msg = 'need config param "database"'
  if (!options.hasOwnProperty('table'))
    msg = 'need config param "table"'
  if (!options.hasOwnProperty('csv'))
    msg = 'need config param "csv"'

  return msg
}

module.exports = csv2mysql

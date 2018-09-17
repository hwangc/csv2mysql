const expect = require('chai').expect
const csv2mysql = require('../src')
const fs = require('fs')
const mysql = require('mysql')

describe("Insert 5 sample data to mysql database", () => {
  it('should delete 5 sample data before the test', () => {
    let rows = {}
    const connection = mysql.createConnection({
      host: 'localhost',
      user: 'root',
      password: 'passwrod',
      database: 'database'
    })
    rows = connection.query('DELETE FROM gep.serial_table WHERE regdate > "2018-09-14"')
    //console.log(rows._results)
  })

  it('should find test file test.csv', () => {
    expect(fs.existsSync(__dirname + '/test.csv')).to.be.true
  })

  /* When there is no duplicate data, it will insert them all */
  it('should return 5 affected rows', async () => {
    const testCSVFilePath = __dirname + '/test.csv'
    let rows = 0
    rows = await csv2mysql({
      host: 'localhost',
      user: 'root',
      password: 'passwrod',
      database: 'database',
      table: 'table',
      csv: {
        delimiter: ','
      }
    }, testCSVFilePath)
    expect(rows).to.equal(5)
  })

  /* When there are any duplicate data, it won't insert them */
  it('If it tries to insert same data, it should return 0 affected rows', async () => {
    const testCSVFilePath = __dirname + '/test.csv'
    let rows = 0
    rows = await csv2mysql({
      host: 'localhost',
      user: 'root',
      password: 'passwrod',
      database: 'database',
      table: 'table',
      csv: {
        delimiter: ','
      }
    }, testCSVFilePath)
    expect(rows).to.equal(0)
  })
})

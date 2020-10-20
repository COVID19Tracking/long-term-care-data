require('dotenv').config()
const { BigQuery } = require('@google-cloud/bigquery')
const { Parser } = require('json2csv')
const fs = require('fs')

const credentials = JSON.parse(process.env.BIGQUERY_CREDENTIALS)
const client = new BigQuery({
  projectId: credentials.project_id,
  credentials,
  scopes: ['https://www.googleapis.com/auth/drive'],
})
const dataset = 'long_term_care_export'

const parser = new Parser()

const run = async () => {
  const [tables] = await client.dataset(dataset).getTables()
  const queue = []
  tables.forEach(async (table) => {
    const query = `SELECT * FROM \`${dataset}.${table.id}\``
    const [job] = await client.createQueryJob({
      query,
      location: 'US',
    })
    const [rows] = await job.getQueryResults()
    console.log(`📦 Fetched ${rows.length} rows from bigquery ${table.id}`)

    fs.writeFileSync(`../${table.id}.csv`, parser.parse(rows))
    console.log(`🏓 Saved table ${table.id}`)
  })
}

run()

async function queryMdb(mdbPath, sql) {
  let ADODB;
  try {
    // node-adodb can only be installed on Windows hosts.
    ADODB = require('node-adodb'); // eslint-disable-line global-require
  } catch (error) {
    error.status = 500;
    error.message = 'node-adodb is unavailable on this host. Install 32-bit Access Database Engine on Windows.';
    throw error;
  }

  const connection = ADODB.open(`Provider=Microsoft.Jet.OLEDB.4.0;Data Source=${mdbPath};`);
  const rows = await connection.query(sql);
  return Array.isArray(rows) ? rows : [];
}

module.exports = { queryMdb };

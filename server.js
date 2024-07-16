const express = require('express');
const mysql = require('mysql2');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const port = 3000;

// Middleware
app.use(cors({ origin: '*', methods: ['GET', 'POST', 'PUT', 'DELETE'], allowedHeaders: ['Content-Type', 'Authorization'] }));
app.use(express.json());

// MySQL connection
const connection = mysql.createConnection({
  host: '127.0.0.1',
  port: 3308,
  user: 'root',
  password: '',
  database: 'neepco',
  timezone: '+05:30'
});

connection.connect(err => {
  if (err) {
    console.error('Error connecting to the database:', err);
    return;
  }
  console.log('Connected to MySQL database');
});

// Supabase client
const supabase = createClient('https://robxcaglqhdfnyzibxdc.supabase.co', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJvYnhjYWdscWhkZm55emlieGRjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcyMDQ0NTc2NSwiZXhwIjoyMDM2MDIxNzY1fQ.KbF4QDojUSz-diD31LKc1Y3BjTz1dVprfk_jdro_MT8');

// Create editors_timestamp table
const createEditorsTimestampTable = `
  CREATE TABLE IF NOT EXISTS editors_timestamp (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    admin_name VARCHAR(255),
    table_name VARCHAR(255),
    column_name VARCHAR(255),
    row_id VARCHAR(255),
    logs TEXT
  )
`;

connection.query(createEditorsTimestampTable, (err) => {
  if (err) {
    console.error('Error creating editors_timestamp table:', err);
  } else {
    console.log('editors_timestamp table created or already exists');
  }
});

// Configure multer for file uploads
const upload = multer({ dest: 'uploads/' });

// Helper functions
async function getEmployeeNameFromToken(token) {
  try {
    const { data: { user }, error } = await supabase.auth.getUser(token);
    if (error) throw error;

    const { data, error: fetchError } = await supabase.from('users').select('employee_name').eq('id', user.id).single();
    if (fetchError) throw fetchError;

    return data.employee_name;
  } catch (error) {
    console.error('Error fetching employee name:', error);
    return 'Unknown';
  }
}

function logEdit(adminName, tableName, columnName, rowId, action) {
  const logQuery = `
    INSERT INTO editors_timestamp (timestamp, admin_name, table_name, column_name, row_id, logs)
    VALUES (CONVERT_TZ(NOW(), '+00:00', '+05:30'), ?, ?, ?, ?, ?)
  `;
  const logMessage = `${action}`;
  connection.query(logQuery, [adminName, tableName, columnName, rowId, logMessage], (err) => {
    if (err) {
      console.error('Error logging edit:', err);
    } else {
      console.log('Log entry added successfully');
    }
  });
}
// Middleware
async function authMiddleware(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) {
    return res.status(401).json({ error: 'No token provided' });
  }

  try {
    req.employeeName = await getEmployeeNameFromToken(token);
    next();
  } catch (error) {
    return res.status(401).json({ error: 'Invalid token' });
  }
}

// Routes
app.get('/', (req, res) => {
  res.send('Server is running. Use the API endpoints to interact with the application.');
});

app.post('/upload', upload.single('file'), authMiddleware, (req, res) => {
  const filePath = req.file.path;
  const originalFileName = req.file.originalname;
  const fileNameWithoutExtension = path.parse(originalFileName).name;
  const tableName = fileNameWithoutExtension.replace(/\s+/g, '').replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
  const results = [];

  fs.createReadStream(filePath)
    .pipe(csv())
    .on('data', (data) => {
      const sanitizedData = {};
      for (const [key, value] of Object.entries(data)) {
        const sanitizedKey = key.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '_');
        sanitizedData[sanitizedKey] = value.toString().slice(0, 255);
      }
      results.push(sanitizedData);
    })
    .on('end', () => {
      if (results.length === 0) {
        return res.status(400).send('CSV file is empty or could not be read');
      }

      const keys = Object.keys(results[0]);
      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS \`${tableName}\` (
          ${keys.map(key => `\`${key}\` VARCHAR(500)`).join(',')}
        )
      `;

      connection.query(createTableQuery, (err) => {
        if (err) {
          console.error('Error creating table:', err);
          return res.status(500).send('Error creating table');
        }

        results.forEach(row => {
          const sanitizedRow = {};
          for (const [key, value] of Object.entries(row)) {
            sanitizedRow[key] = value.toString().slice(0, 255);
          }
          const values = Object.values(sanitizedRow);
          const placeholders = values.map(() => '?').join(',');
          const insertQuery = `
            INSERT INTO \`${tableName}\` (${Object.keys(sanitizedRow).map(key => `\`${key}\``).join(',')})
            VALUES (${placeholders})
          `;

          connection.query(insertQuery, values, (err) => {
            if (err) {
              console.error('Error inserting row:', err);
            }
          });
        });

        logEdit(req.employeeName, tableName, 'ALL', 'ALL', `Created table and inserted ${results.length} rows`);
        fs.unlinkSync(filePath);
        res.send('CSV file processed and data inserted into MySQL');
      });
    });
});

app.get('/tables', (req, res) => {
  const query = 'SHOW TABLES';
  connection.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching table names:', err);
      return res.status(500).send('Error fetching table names');
    }
    const tableNames = results.map(row => Object.values(row)[0]);
    res.json(tableNames);
  });
});

app.get('/tables/:tableName', (req, res) => {
  const tableName = req.params.tableName;
  const query = `SELECT * FROM \`${tableName}\``;
  connection.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching data:', err);
      return res.status(500).send('Error fetching data');
    }
    res.json(results);
  });
});

app.put('/tables/:tableName/row/:rowId', authMiddleware, async (req, res) => {
  const { tableName, rowId } = req.params;
  const updatedData = req.body;
  const adminName = req.employeeName;

  try {
    // Get the primary key column name
    const [keyResult] = await connection.promise().query('SHOW KEYS FROM ?? WHERE Key_name = "PRIMARY"', [tableName]);
    const primaryKeyColumn = keyResult[0]?.Column_name || 'id';

    // Fetch the current row data
    const [currentRowResult] = await connection.promise().query(`SELECT * FROM ?? WHERE ?? = ?`, [tableName, primaryKeyColumn, rowId]);
    const currentRow = currentRowResult[0];

    if (!currentRow) {
      return res.status(404).send('Row not found');
    }

    // Compare and update only changed columns
    const changedColumns = [];
    const updateValues = [];
    const updateColumns = [];

    for (const [column, value] of Object.entries(updatedData)) {
      if (currentRow[column] !== value) {
        changedColumns.push({ column, oldValue: currentRow[column], newValue: value });
        updateColumns.push(`?? = ?`);
        updateValues.push(column, value);
      }
    }

    if (changedColumns.length === 0) {
      return res.status(200).json({ message: 'No changes detected' });
    }

    // Construct and execute the update query
    const updateQuery = `UPDATE ?? SET ${updateColumns.join(', ')} WHERE ?? = ?`;
    updateValues.unshift(tableName);
    updateValues.push(primaryKeyColumn, rowId);

    await connection.promise().query(updateQuery, updateValues);

    // Log each changed column separately
    for (const change of changedColumns) {
      logEdit(adminName, tableName, change.column, rowId, `Updated ${change.column} in row ${rowId} from ${change.oldValue} to ${change.newValue}`);
    }

    res.json({ message: 'Row updated successfully', changedColumns });
  } catch (error) {
    console.error('Error updating row:', error);
    res.status(500).send('Error updating row');
  }
});

app.post('/tables/:tableName/column', authMiddleware, (req, res) => {
  const { tableName } = req.params;
  const { columnName } = req.body;
  const adminName = req.employeeName;

  const sanitizedColumnName = columnName.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '_').replace(/^(\d)/, '_$1').replace(/\./g, '_');

  if (sanitizedColumnName.length === 0) {
    return res.status(400).send('Invalid column name');
  }

  const alterQuery = 'ALTER TABLE ?? ADD COLUMN ?? VARCHAR(255)';
  connection.query(alterQuery, [tableName, sanitizedColumnName], (err) => {
    if (err) {
      console.error('Error adding column:', err);
      return res.status(500).send('Error adding column');
    }

    logEdit(adminName, tableName, sanitizedColumnName, 'ALL', `Added new column ${sanitizedColumnName}`);
    res.json({ message: 'Column added successfully', columnName: sanitizedColumnName });
  });
});

app.put('/tables/:tableName/column/:oldColumnName', authMiddleware, (req, res) => {
  const { tableName, oldColumnName } = req.params;
  const { newColumnName } = req.body;
  const adminName = req.employeeName;

  const sanitizedNewColumnName = newColumnName.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '_').replace(/^(\d)/, '_$1').replace(/\./g, '_');
  const alterQuery = 'ALTER TABLE ?? CHANGE COLUMN ?? ?? VARCHAR(500)';

  connection.query(alterQuery, [tableName, oldColumnName, sanitizedNewColumnName], (err) => {
    if (err) {
      console.error('Error renaming column:', err);
      return res.status(500).send('Error renaming column');
    }

    logEdit(adminName, tableName, `${oldColumnName} to ${sanitizedNewColumnName}`, 'ALL', `Renamed column ${oldColumnName} to ${sanitizedNewColumnName}`);
    res.json({ message: 'Column renamed successfully' });
  });
});

app.delete('/tables/:tableName', authMiddleware, (req, res) => {
  const { tableName } = req.params;
  const adminName = req.employeeName;
  const dropQuery = 'DROP TABLE IF EXISTS ??';

  connection.query(dropQuery, [tableName], (err) => {
    if (err) {
      console.error('Error deleting table:', err);
      return res.status(500).send('Error deleting table');
    }

    logEdit(adminName, tableName, 'ALL', 'ALL', `Deleted table ${tableName}`);
    res.json({ message: 'Table deleted successfully' });
  });
});

app.put('/tables/:tableName', authMiddleware, (req, res) => {
  const { tableName } = req.params;
  const { data: updatedData, columnOrder } = req.body;
  const adminName = req.employeeName;

  connection.beginTransaction((err) => {
    if (err) {
      console.error('Error starting transaction:', err);
      return res.status(500).send('Error saving table');
    }

    connection.query(`DESCRIBE ??`, [tableName], (describeErr, columns) => {
      if (describeErr) {
        return connection.rollback(() => {
          console.error('Error describing table:', describeErr);
          res.status(500).send('Error saving table');
        });
      }

      const existingColumns = columns.map(col => col.Field);
      const newColumns = columnOrder || Object.keys(updatedData[0] || {});
      const columnsToDelete = existingColumns.filter(col => !newColumns.includes(col));

      const deleteColumnPromises = columnsToDelete.map(col => new Promise((resolve, reject) => {
        const alterQuery = `ALTER TABLE ?? DROP COLUMN ??`;
        connection.query(alterQuery, [tableName, col], (alterErr) => {
          if (alterErr) reject(alterErr);
          else resolve();
        });
      }));

      Promise.all(deleteColumnPromises)
        .then(() => {
          if (columnOrder) {
            const rearrangePromises = columnOrder.map((col, index) => new Promise((resolve, reject) => {
              const sanitizedCol = col.replace(/\./g, '_');
              const alterQuery = `ALTER TABLE ?? MODIFY COLUMN ?? VARCHAR(500) ${index === 0 ? 'FIRST' : `AFTER ??`}`;
              const queryParams = index === 0 ? [tableName, sanitizedCol] : [tableName, sanitizedCol, columnOrder[index - 1].replace(/\./g, '_')];
              connection.query(alterQuery, queryParams, (alterErr) => {
                if (alterErr) {
                  console.error('Error altering column:', alterErr);
                  reject(alterErr);
                } else resolve();
              });
            }));

            return Promise.all(rearrangePromises);
          }
        })
        .then(() => {
          const deleteQuery = 'DELETE FROM ??';
          connection.query(deleteQuery, [tableName], (deleteErr) => {
            if (deleteErr) {
              return connection.rollback(() => {
                console.error('Error deleting existing rows:', deleteErr);
                res.status(500).send('Error saving table');
              });
            }

            if (updatedData.length === 0) {
              return connection.commit((commitErr) => {
                if (commitErr) {
                  return connection.rollback(() => {
                    console.error('Error committing transaction:', commitErr);
                    res.status(500).send('Error saving table');
                  });
                }

                logEdit(adminName, tableName, 'ALL COLUMNS', 'ALL ROWS', deleteQuery);
                res.json({ message: 'Table saved successfully (all rows deleted)' });
              });
            }

            const insertQuery = 'INSERT INTO ?? (??) VALUES ?';
            const insertPromises = updatedData.map(row => new Promise((resolve, reject) => {
              const sanitizedRow = Object.fromEntries(
                Object.entries(row).map(([key, value]) => [key.replace(/\./g, '_'), value === '' || value === null || (typeof value === 'string' && value.trim() === '') ? null : value])
              );
              const columns = columnOrder ? columnOrder.map(col => col.replace(/\./g, '_')) : Object.keys(sanitizedRow);
              const values = columns.map(col => sanitizedRow[col]);

              connection.query(insertQuery, [tableName, columns, [values]], (insertErr) => {
                if (insertErr) reject(insertErr);
                else resolve();
              });
            }));

            Promise.all(insertPromises)
              .then(() => {
                connection.commit((commitErr) => {
                  if (commitErr) {
                    return connection.rollback(() => {
                      console.error('Error committing transaction:', commitErr);
                      res.status(500).send('Error saving table');
                    });
                  }

                  logEdit(adminName, tableName, 'ALL', 'ALL', `Updated entire table ${tableName} with ${updatedData.length} rows`);
                  res.json({ message: 'Table saved successfully' });
                });
              })
              .catch((err) => {
                return connection.rollback(() => {
                  console.error('Error inserting rows:', err);
                  res.status(500).send('Error saving table');
                });
              });
          });
        })
        .catch((err) => {
          return connection.rollback(() => {
            console.error('Error deleting columns:', err);
            res.status(500).send('Error saving table');
          });
        });
    });
  });
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
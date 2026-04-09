'use strict';

require('dotenv').config();
const AppServer = require('./AppServer');

const port = parseInt(process.env.PORT ?? '3000', 10);
const app = new AppServer();
app.start(port);

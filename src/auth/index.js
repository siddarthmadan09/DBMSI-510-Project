const express = require('express')
const bodyParser = require('body-parser')
const exphbs = require("express-handlebars");
const passport     = require('passport');
const flash        = require('connect-flash');
const cookieParser = require('cookie-parser');
const session      = require('express-session'); 

const app = express();
var db = require("./models");
require('./config/passport')(passport)
app.get('/' , (req,res) => {
    res.send('Hello World!')
})

app.listen(8000, () => {
    console.log('Example app listening on port 8000!')
})
const winston = require('winston');
const moment = require('moment')
require('winston-daily-rotate-file');

const transportDebug = new (winston.transports.DailyRotateFile)({
  filename: 'logs/' + moment().format('YYYY') + '-' + moment().format('MM') + '/debug/application-%DATE%-debug.log',
  datePattern: 'YYYY-MM-DD',
});

const transportInfo = new (winston.transports.DailyRotateFile)({
  filename: 'logs/' + moment().format('YYYY') + '-' + moment().format('MM') + '/info/application-%DATE%-info.log',
  datePattern: 'YYYY-MM-DD',
});

const transportError = new (winston.transports.DailyRotateFile)({
  filename: 'logs/' + moment().format('YYYY') + '-' + moment().format('MM') + '/error/application-%DATE%-error.log',
  datePattern: 'YYYY-MM-DD',
  level: 'error',

});

// ADD DEBUG LOG
const loggerDebug = winston.createLogger(
  {
    level: 'debug',
    format: winston.format.combine(
      winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
      winston.format.printf(
        msg => `${msg.timestamp} ${msg.level} ${msg.message}`,
      ),
    ),
    transports: [
      new winston.transports.Console(),
      transportDebug
    ]
  }
);

const loggerInfo = winston.createLogger(
  {
    level: 'info',
    format: winston.format.combine(
      winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
      winston.format.printf(
        msg => `${msg.timestamp} ${msg.level} ${msg.message}`,
      ),
    ),
    transports: [
      new winston.transports.Console(),
      transportInfo
    ]
  }
);

const loggerError = winston.createLogger(
  {
    level: 'error',
    format: winston.format.combine(
      winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
      winston.format.printf(
        msg => `${msg.timestamp} ${msg.level} ${msg.message}`,
      ),
    ),
    transports: [
      new winston.transports.Console(),
      transportError
    ]
  })

const logger = {
  info: (params) => {
    return loggerInfo.info(params)
  },
  error: (params) => {
    return loggerError.error(params)
  },
  debug: (params) => {
    return loggerDebug.debug(params)
  }
}

module.exports = logger

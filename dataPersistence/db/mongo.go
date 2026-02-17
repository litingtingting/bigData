package db

import (
	"gopkg.in/mgo.v2"
	. "spark-collector/config"
	// "log"
)

var mgoConn *mgo.Session

func GetMgoConn(force bool) *mgo.Session {
	if mgoConn == nil || force == true {
		var err error
		mgoConn, err = mgo.Dial(Cfg.MongoDbHost)
		if err != nil {
			panic(err)
		}
	}
	return mgoConn
}

func CheckConn(conn *mgo.Session) error {
	return conn.Ping()
}

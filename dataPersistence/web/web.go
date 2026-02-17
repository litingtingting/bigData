package web

import (
	"fmt"
	"github.com/lessos/lessgo/encoding/json"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net/http"
	"spark-collector/db"
	"strconv"
	// "strings"
	// "time"
)

func show(rw http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	dbName := req.Form.Get("d")
	collectionName := req.Form.Get("c")
	startDate := req.Form.Get("startDate")
	endDate := req.Form.Get("endDate")
	sort := req.Form.Get("sort")
	sortBy := req.Form.Get("sortBy")
	// callback := req.Form.Get("callback")
	q := req.Form.Get("q")
	p := req.Form.Get("p")
	limit, _ := strconv.Atoi(req.Form.Get("limit"))
	page, _ := strconv.Atoi(req.Form.Get("page"))

	// isJsonp := false
	// if callback != "" {
	// 	isJsonp = true
	// }

	if dbName == "" {
		dbName = "stats"
	}

	if collectionName == "" {
		collectionName = "sharepage"
	}

	if sort == "" || sort == "-1" {
		sort = "-"
	} else {
		sort = ""
	}

	if sortBy == "" {
		sortBy = "date"
	}

	if limit == 0 {
		limit = 7
	}

	if page == 0 {
		page = 1
	}

	// q = "{\"date\":{\"$lte\":\"2018-09-20\"}}"

	query := make(map[string]interface{})
	if q != "" {
		json.Decode([]byte(q), &query)
	}

	projection := make(map[string]interface{})
	if p != "" {
		json.Decode([]byte(p), &projection)
	}

	skip := (page - 1) * limit

	s := sort + sortBy

	dateQuery := make(bson.M)
	// query["date"] = make(bson.M)

	if startDate != "" {
		dateQuery["$gte"] = startDate
	}

	if endDate != "" {
		dateQuery["$lte"] = endDate
	}

	if len(dateQuery) > 0 {
		query["date"] = dateQuery
	}

	// if date == "" {
	// respJson(rw, 1, "date is missing", nil)
	// return
	// }

	// dateAry := strings.Split(date, ",")

	mongoClient := db.GetMgoConn(false)

	var res []interface{}
	// mongoClient.DB(dbName).C(collectionName).Find(bson.M{"date": bson.M{"$in": dateAry}}).All(&res)
	mongoClient.DB(dbName).C(collectionName).Find(query).Select(projection).Skip(skip).Limit(limit).Sort(s).All(&res)
	if len(res) == 0 {
		respJson(req, rw, 2, "no data", nil)
		return
	}

	data := make(map[string]interface{})
	data["data"] = res
	data["total"] = len(res)
	data["page"] = page

	respJson(req, rw, 0, "", data)
	return
}

func respJson(req *http.Request, rw http.ResponseWriter, code int, msg string, data map[string]interface{}) {
	rw.Header().Add("Content-type", "application/json; charset=UTF-8")

	callback := req.Form.Get("callback")

	resp := make(map[string]interface{})
	resp["errcode"] = code
	if msg != "" {
		resp["errmsg"] = msg
	}
	if data != nil {
		resp["data"] = data["data"]
		resp["total"] = data["total"]
		resp["page"] = data["page"]
	}
	res, _ := json.Encode(resp, "")
	ret := string(res)
	if callback != "" {
		ret = callback + "(" + ret + ")"
	}
	fmt.Fprintf(rw, ret)
}

func Listen(addr string) {
	http.HandleFunc("/stat/show", show)
	log.Println("Listening ", addr)
	err := http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

package handler

import (
	"encoding/json"
	"net/http"

	"github.com/yuuki/dynamond/log"
)

func JSON(w http.ResponseWriter, status int, v interface{}) error {
	res, err := json.Marshal(v)
	if err != nil {
		return err
	}

	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/json")
	w.Write(res)
	return nil
}

func BadRequest(w http.ResponseWriter, msg string) {
	log.Println(msg)

	var data struct {
		Error  string `json:"error"`
	}
	data.Error = msg
	JSON(w, http.StatusBadRequest, data)
	return
}

func NotFound(w http.ResponseWriter) {
	http.Error(w, "404 Not Found", http.StatusNotFound)
}

func ServerError(w http.ResponseWriter, msg string) {
	log.Println(msg)

	var data struct {
		Error  string `json:"error"`
	}
	data.Error = msg
	JSON(w, http.StatusInternalServerError, data)
	return
}


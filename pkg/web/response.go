package web

import (
	"encoding/json"
	"log"
	"net/http"
)

func renderJSON(w http.ResponseWriter, status int, v interface{}) {
	_renderJSON(w, status, v, false)
}

func renderJSONIndent(w http.ResponseWriter, status int, v interface{}) {
	_renderJSON(w, status, v, true)
}

func _renderJSON(w http.ResponseWriter, status int, v interface{}, indent bool) {
	var (
		data []byte
		err  error
	)
	if indent {
		data, err = json.MarshalIndent(v, "", "\t")
	} else {
		data, err = json.Marshal(v)
	}
	if err != nil {
		serverError(w, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(data); err != nil {
		log.Println(err)
		return
	}
	return
}

func ok(w http.ResponseWriter, msg string) {
	var data struct {
		Msg string `json:"message"`
	}
	data.Msg = msg
	renderJSON(w, http.StatusOK, data)
	return
}

func badRequest(w http.ResponseWriter, msg string) {
	var data struct {
		Error string `json:"error"`
	}
	data.Error = msg
	renderJSON(w, http.StatusBadRequest, data)
	return
}

func serverError(w http.ResponseWriter, msg string) {
	var data struct {
		Error string `json:"error"`
	}
	data.Error = msg
	renderJSON(w, http.StatusInternalServerError, data)
	return
}

func unavaliableError(w http.ResponseWriter, msg string) {
	var data struct {
		Error string `json:"error"`
	}
	data.Error = msg
	renderJSON(w, http.StatusServiceUnavailable, data)
	return
}

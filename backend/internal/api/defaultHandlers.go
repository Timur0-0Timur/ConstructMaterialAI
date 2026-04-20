package httpapi

import (
	"fmt"
	"net/http"
)

func DefaultHandler(w http.ResponseWriter, r *http.Request) {
	//чтоб проверять, что запросы на бэк вообще приходят
	_, err := w.Write([]byte("Ok"))
	if err != nil {
		fmt.Printf("Error : %s\n", err.Error())
	} else {
		fmt.Println("Default request made successfully")
	}
}

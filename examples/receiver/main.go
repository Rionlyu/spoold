package main

import (
	"encoding/json"
	"log"
	"net/http"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		var event any
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		log.Printf("delivery=%s attempt=%s event=%v",
			r.Header.Get("X-Spoold-Delivery-ID"),
			r.Header.Get("X-Spoold-Attempt"),
			event,
		)
		w.WriteHeader(http.StatusNoContent)
	})

	log.Println("receiver listening on 127.0.0.1:9090")
	log.Fatal(http.ListenAndServe("127.0.0.1:9090", mux))
}

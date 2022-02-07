package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"
)

var port = flag.String("port", "8080", "port to start")

func main() {
	flag.Parse()
	fmt.Printf("Starting server at port %s\n", *port)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := &Service{
		ctx:     ctx,
		watcher: make(chan string),
		clients: make(map[string]*ClientQueue),
		values:  make(map[string]*ValuesQueue),
	}
	go service.RunWatcher()

	http.HandleFunc("/", service.handler)

	server := &http.Server{Addr: fmt.Sprintf(":%s", *port)}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}

type Response struct {
	http.ResponseWriter
}

func (r *Response) Text(code int, body string) {
	r.Header().Set("Content-Type", "text/plain")
	r.WriteHeader(code)

	_, err := io.WriteString(r, fmt.Sprintf("%s\n", body))
	if err != nil {
		log.Print("resp write err", err)
		return
	}
}

func (s *Service) handler(w http.ResponseWriter, r *http.Request) {
	resp := Response{w}

	keys := strings.Split(r.URL.Path, "/")
	if len(keys) != 2 {
		resp.Text(http.StatusBadRequest, "")
	}

	key := keys[1]

	switch r.Method {
	case "GET":
		timeouts, ok := r.URL.Query()["timeout"]
		timeout := "0"
		if ok {
			timeout = timeouts[0]
		}

		value, err := s.ExtractValue(key)
		switch err {
		case nil:
			resp.Text(http.StatusOK, value)
		case ErrEmptyQueue:
			if timeout != "" {
				dur, err := strconv.Atoi(timeout)
				if err != nil {
					resp.Text(http.StatusBadRequest, "")
				}

				dd := time.Duration(dur)
				clientCtx, cancel := context.WithTimeout(s.ctx, dd*time.Second)
				defer cancel()
				notify := s.AddWaiting(key, clientCtx)
				select {
				case <-clientCtx.Done():
					resp.Text(http.StatusNotFound, "")
				case <-notify:
					value, err := s.ExtractValue(key)
					switch err {
					case nil:
						resp.Text(http.StatusOK, value)
					default:
						resp.Text(http.StatusNotFound, "")
					}
				}
			}
			resp.Text(http.StatusNotFound, "")
		case ErrNoQueueForKey:
			resp.Text(http.StatusNotFound, "")
		default:
			resp.Text(http.StatusInternalServerError, "")
			log.Fatal(err)
		}
	case "PUT":
		values, ok := r.URL.Query()["v"]
		value := "default message"
		if ok {
			value = values[0]
		}

		s.InsertValue(key, value)
	default:
		if len(keys) != 2 {
			resp.Text(http.StatusNotFound, "")
		}
	}
}

type Client struct {
	ctx      context.Context
	Response chan struct{}
}

type ClientQueue struct {
	clients []*Client
}

func (q *ClientQueue) Push(v *Client) {
	q.clients = append(q.clients, v)
}

func (q *ClientQueue) Pop() (*Client, bool) {
	if len(q.clients) > 0 {
		result := q.clients[0]
		q.clients = q.clients[1:]

		return result, true
	}

	return nil, false
}

type ValuesQueue struct {
	values []string
}

func (q *ValuesQueue) Push(v string) {
	q.values = append(q.values, v)
}

func (q *ValuesQueue) Pop() (string, bool) {
	if len(q.values) > 0 {
		result := q.values[0]
		q.values = q.values[1:]

		return result, true
	}

	return "", false
}

type Service struct {
	sync.Mutex
	ctx     context.Context
	watcher chan string
	clients map[string]*ClientQueue
	values  map[string]*ValuesQueue
}

func (s *Service) InsertValue(key, value string) {
	queue, ok := s.values[key]
	if !ok {
		s.values[key] = &ValuesQueue{}
		queue = s.values[key]
	}

	queue.Push(value)

	s.watcher <- key
}

var ErrNoQueueForKey = errors.New("ErrNoQueueForKey")
var ErrEmptyQueue = errors.New("ErrEmptyQueue")

func (s *Service) ExtractValue(key string) (string, error) {
	queue, ok := s.values[key]
	if !ok {
		return "", ErrNoQueueForKey
	}

	results, ok := queue.Pop()
	if !ok {
		return "", ErrEmptyQueue
	}

	return results, nil
}

func (s *Service) AddWaiting(key string, ctx context.Context) chan struct{} {
	s.Lock()
	defer s.Unlock()
	queue, ok := s.clients[key]
	if !ok {
		s.clients[key] = &ClientQueue{}
		queue = s.clients[key]
	}

	notify := make(chan struct{}, 2)
	queue.Push(&Client{
		ctx:      ctx,
		Response: notify,
	})

	return notify
}

func (s *Service) NotifyWaiter(key string) {
	queue, ok := s.clients[key]
	if !ok {
		return
	}

	client, ok := queue.Pop()
	if !ok {
		return
	}

	select {
	case <-client.ctx.Done():
		return
	case client.Response <- struct{}{}:
		close(client.Response)
	}
	return
}

func (s *Service) RunWatcher() {
	for {
		select {
		case q := <-s.watcher:
			s.NotifyWaiter(q)
		case <-s.ctx.Done():
			return
		}
	}
}

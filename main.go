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

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		resp := Response{w}

		keys := strings.Split(r.URL.Path, "/")
		if len(keys) != 2 {
			resp.Text(http.StatusBadRequest, "")
		}

		switch r.Method {
		case "GET":
			service.Get(w, r)
		case "PUT":
			service.Put(w, r)
		default:
			resp.Text(http.StatusNotFound, "")
		}
	})

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

func (s *Service) Put(w http.ResponseWriter, r *http.Request) {
	resp := Response{w}

	keys := strings.Split(r.URL.Path, "/")
	if len(keys) != 2 {
		resp.Text(http.StatusBadRequest, "")
	}

	key := keys[1]

	values, ok := r.URL.Query()["v"]
	value := "default message"
	if ok {
		value = values[0]
	}

	s.InsertValue(key, value)
}

func (s *Service) Get(w http.ResponseWriter, r *http.Request) {
	resp := Response{w}

	keys := strings.Split(r.URL.Path, "/")
	if len(keys) != 2 {
		resp.Text(http.StatusBadRequest, "")
	}

	key := keys[1]

	timeouts, ok := r.URL.Query()["timeout"]
	timeout := "0"
	if ok {
		timeout = timeouts[0]
	}

	extractFromQueue := func () {
		value, err := s.ExtractValue(key)
		switch err {
		case nil:
			resp.Text(http.StatusOK, value)
		default:
			resp.Text(http.StatusNotFound, "")
		}
	}

	collect := func() {
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
			case <-r.Context().Done():
				resp.Text(http.StatusNotFound, "")
				cancel()
			case <-clientCtx.Done():
				resp.Text(http.StatusNotFound, "")
			case <-notify:
				extractFromQueue()
			}
		}
	}

	value, err := s.TryRead(key)
	switch err {
	case nil:
		resp.Text(http.StatusOK, value)
	case ErrEmptyQueue:
		collect()
	case ErrNoQueueForKey:
		resp.Text(http.StatusNotFound, "")
	default:
		resp.Text(http.StatusInternalServerError, "")
		log.Fatal(err)
	}
}

type Chunk struct {
	Data interface{}
	Next *Chunk
}

type Queue struct {
	len int
	sync.Mutex
	head, tail *Chunk
}

func (q *Queue) L() (int, ) {
	q.Lock()
	l := q.len
	q.Unlock()

	return l
}

func (q *Queue) Push(v interface{}) {
	q.Lock()

	chunk := &Chunk{
		Data: v,
	}

	if q.tail != nil {
		q.tail.Next = chunk
	}
	q.tail = chunk

	if q.len == 0 {
		q.head = q.tail
	}

	q.len += 1
	q.Unlock()
}

type Client struct {
	ctx      context.Context
	Response chan struct{}
}

type ClientQueue struct {
	Queue
}

func (q *ClientQueue) Pop() (*Client, bool) {
	q.Lock()


	if q.head == nil {
		q.Unlock()

		return nil, false
	}

	client, ok := q.head.Data.(*Client)
	if q.head.Next != nil {
		q.head = q.head.Next
	} else {
		q.head = nil
	}

	q.len -= 1

	q.Unlock()
	return client, ok
}

type ValuesQueue struct {
	Queue
}

func (q *ValuesQueue) Pop() (*string, bool) {
	q.Lock()

	if q.head == nil {
		q.Unlock()

		return nil, false
	}

	client, ok := q.head.Data.(*string)
	if q.head.Next != nil {
		q.head = q.head.Next
	} else {
		q.head = nil
	}

	q.len -= 1

	q.Unlock()
	return client, ok
}

type Service struct {
	sync.RWMutex
	ctx     context.Context
	watcher chan string
	clients map[string]*ClientQueue
	values  map[string]*ValuesQueue
}

func (s *Service) InsertValue(key, value string) {
	s.Lock()
	queue, ok := s.values[key]
	if !ok {
		s.values[key] = &ValuesQueue{}
		queue = s.values[key]
	}
	s.Unlock()

	queue.Push(&value)

	s.watcher <- key
}

var ErrNoQueueForKey = errors.New("ErrNoQueueForKey")
var ErrEmptyQueue = errors.New("ErrEmptyQueue")

func (s *Service) TryRead(key string) (string, error) {
	s.RLock()
	client, ok := s.clients[key]
	s.RUnlock()
	if !ok || client.L() == 0 {
		return s.ExtractValue(key)
	}

	return "", ErrEmptyQueue
}
func (s *Service) ExtractValue(key string) (string, error) {
	s.RLock()
	queue, ok := s.values[key]
	if !ok {
		s.RUnlock()

		return "", ErrNoQueueForKey
	}
	s.RUnlock()

	results, ok := queue.Pop()
	if !ok {
		return "", ErrEmptyQueue
	}

	return *results, nil
}

func (s *Service) AddWaiting(key string, ctx context.Context) chan struct{} {
	s.Lock()
	queue, ok := s.clients[key]
	if !ok {
		s.clients[key] = &ClientQueue{}
		queue = s.clients[key]
	}
	s.Unlock()

	notify := make(chan struct{}, 2)
	queue.Push(&Client{
		ctx:      ctx,
		Response: notify,
	})

	return notify
}

func (s *Service) NotifyWaiter(key string) {
	s.RLock()
	queue, ok := s.clients[key]
	if !ok {
		s.RUnlock()

		return
	}
	s.RUnlock()

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

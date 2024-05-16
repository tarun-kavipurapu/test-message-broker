package main

import (
	"fmt"
	"sync"
	"time"
)

//basically i am going to make a publisher subsceriber model

// there is a publisher  and a subscriber

// publisher has topics inside the

type Message struct {
	Topic   string
	Payload interface{}
}

type Subscriber struct {
	channel     chan interface{}
	Unsubscribe chan bool //to infoorm whhen the subsriber want to unsubscribe
}

type Broker struct {
	subscribers map[string][]*Subscriber
	mutex       sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[string][]*Subscriber),
	}
}
func (b *Broker) Subscribe(topic string) *Subscriber {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	subscriber := &Subscriber{
		channel:     make(chan interface{}),
		Unsubscribe: make(chan bool),
	}

	b.subscribers[topic] = append(b.subscribers[topic], subscriber)

	return subscriber
}
func (b *Broker) Unsubscribe(topic string, subscriber *Subscriber) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if subscribers, found := b.subscribers[topic]; found {
		for i, sub := range subscribers {
			if sub == subscriber {
				close(sub.channel)
				b.subscribers[topic] = append(subscribers[:i], subscribers[i+1:]...)
				// sub.Unsubscribe = true
				return

			}
		}
	}
}

func (b *Broker) Publish(topic string, payload interface{}) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if subscribers, found := b.subscribers[topic]; found {

		for _, sub := range subscribers {

			select {
			case sub.channel <- payload:
			case <-time.After(time.Second):
				fmt.Printf("Subscriber slow. Unsubscribing from topic: %s\n", topic)
				b.Unsubscribe(topic, sub)

			}
		}
	}

}

func main() {

	broker := NewBroker()

	subscriber := broker.Subscribe("test")

	go func() {
		for {
			select {
			case msg, ok := <-subscriber.channel:
				if !ok {
					fmt.Println("Subscriber channel closed")
					return
				}
				fmt.Printf("Received: %v\n", msg)

			case <-subscriber.Unsubscribe:
				fmt.Println("Unsubscribed")
				return
			}
		}
	}()

	broker.Publish("test", "Hello world!")
	broker.Publish("test", "Hello world!")

	time.Sleep(2 * time.Second)

	broker.Unsubscribe("test", subscriber)

	broker.Publish("test", "This message will not be received")

	time.Sleep(time.Second)

}

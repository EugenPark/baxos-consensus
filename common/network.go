package common

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

type Message struct {
	From int
	To int
	RpcPair *RPCPair
}

type RPCConfig struct {
	MsgObj Serializable
	Code uint8
}

type Node struct {
	address string
	outgoingWriter *bufio.Writer
	outgoingWriterMutex *sync.Mutex
	incomingReader *bufio.Reader
}

type Application struct {
	id int
	incomingChan chan<- Message
	outgoingChan <-chan Message
}

type Network struct {
	app Application
	artificialLatency time.Duration // Artificial latency to simulate network delay
	nodes map[int]*Node
	rpcTable map[uint8]*RPCPair
	debugLevel int
}

/*
	Fill the RPC table by assigning a unique id to each message type
*/

func (n *Network) RegisterRPC(msgObj Serializable, code uint8) {
	n.debug(fmt.Sprintf("Registering RPC %d", code))
	n.rpcTable[code] = &RPCPair{Code: code, Obj: msgObj}
}

func NewNetwork(id int, config *Config, outgoingChan <-chan Message, incomingChan chan<- Message) *Network {
	return &Network{
		app: Application{
			id: id,
			incomingChan: incomingChan,
			outgoingChan: outgoingChan,
		},
		artificialLatency: time.Duration(config.Flags.NetworkFlags.ArtificialLatency) * time.Millisecond,
		nodes: make(map[int]*Node),
		rpcTable: make(map[uint8]*RPCPair),
		debugLevel: config.Flags.NetworkFlags.DebugLevel,
	}
}

func (n *Network) Init(rpcToRegister []RPCConfig, config *Config)  {
	for i := range config.Clients {
		id, _ := strconv.ParseInt(config.Clients[i].Id, 10, 32)
		n.nodes[int(id)] = &Node{
			address: config.GetAddress(int(id)),
			outgoingWriterMutex: &sync.Mutex{},
		}
	}

	for i := range config.Replicas {
		id, _ := strconv.ParseInt(config.Replicas[i].Id, 10, 32)
		n.nodes[int(id)] = &Node{
			address: config.GetAddress(int(id)),
			outgoingWriterMutex: &sync.Mutex{},
		}
	}

	for _, rpc := range rpcToRegister {
		n.RegisterRPC(rpc.MsgObj, rpc.Code)
	}
}

func (n *Network) Run() {
	go n.StartListenServer()
	time.Sleep(5 * time.Second)
	n.ConnectToAllNodes()
	go n.StartSendServer()
}

func (n *Network) ConnectToNode(receiver *Node) {
	var b [4]byte
	bs := b[:4]

	conn, err := net.Dial("tcp", receiver.address)
	if err != nil {
		panic("Error while connecting to node " + receiver.address + " " + err.Error())
	}

	receiver.outgoingWriter = bufio.NewWriter(conn)
	binary.LittleEndian.PutUint16(bs, uint16(n.app.id))
	_, err = conn.Write(bs)
	if err != nil {
		panic("Error while writing to node " + receiver.address)
	}
}

func (n *Network) ConnectToAllNodes() {
	for _, receiver := range n.nodes {
		n.ConnectToNode(receiver)
	}
}

/*
	listen to a given connection reader. Upon receiving any message, put it into the central incoming buffer
*/

func (n *Network) ConnectionHandler(reader *bufio.Reader, from int) {

	var msgType uint8
	var err error = nil

	for {
		msgType, err = reader.ReadByte()
		if err != nil {
			n.debug(fmt.Sprintf("Error while reading message code: connection broken from  %d %v", from, err.Error()))
			return
		}

		rpair, present := n.rpcTable[msgType];
		if !present {
			n.debug(fmt.Sprintf("Error received unknown message type %d from %d", msgType, from))
			return
		}

		obj := rpair.Obj.New()
		err = obj.Unmarshal(reader)
		if err != nil {
			n.debug(fmt.Sprintf("Error while unmarshalling from %d %v", from, err.Error()))
			return
		}

		msg := Message {
			From: from,
			To: n.app.id,
			RpcPair: &RPCPair{
				Code: msgType,
				Obj:  obj,
			},
		}
		
		n.app.incomingChan <- msg
		n.debug(fmt.Sprintf("Received a message %v from %d", *msg.RpcPair, from))
	}
}

func (n *Network) getOwnAddress() string {
	return n.nodes[n.app.id].address
}

/*
	listen on the port for new connections from clients and replicas
*/

func (n *Network) StartListenServer() {
	var b [4]byte
	bs := b[:4]
	Listener, err := net.Listen("tcp", n.getOwnAddress())

	if err != nil {
		panic("Error while listening to incoming connections")
	}

	n.debug("Listening to incoming connections in " + n.getOwnAddress())

	for {
		conn, err := Listener.Accept()
		if err != nil {
			panic(fmt.Sprintf("%v", err.Error()))
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			panic(fmt.Sprintf("%v", err.Error()))
		}
		id := int(binary.LittleEndian.Uint16(bs))
		n.debug(fmt.Sprintf("Received incoming connection from %d", id))
		n.nodes[id].incomingReader = bufio.NewReader(conn)

		go n.ConnectionHandler(n.nodes[id].incomingReader, id)
		n.debug(fmt.Sprintf("Started listening to %d", id))
	}
}

func (n *Network) StartSendServer() {
	for {
		message := <-n.app.outgoingChan
		go n.SendMessage(message)
	}
}

/*
	Write a message to the wire, first the message type is written and then the actual message
*/

func (n *Network) SendMessage(message Message) {
	to := n.nodes[message.To]

	// simulate network delay
	if message.From == message.To {
		time.Sleep(0)
	} else {
		time.Sleep(n.artificialLatency)
	}

	w := to.outgoingWriter
	if w == nil {
		n.debug(fmt.Sprintf("Outgoing Writer %v", to.outgoingWriter))
		panic(fmt.Sprintf("Node not found %d", message.To))
	}

	to.outgoingWriterMutex.Lock()
	defer to.outgoingWriterMutex.Unlock()
	err := w.WriteByte(message.RpcPair.Code)
	if err != nil {
		n.debug("Error writing message code byte: " + err.Error())
		return
	}
	err = message.RpcPair.Obj.Marshal(w)
	if err != nil {
		n.debug("Error while marshalling: " + err.Error())
		return
	}
	err = w.Flush()
	if err != nil {
		n.debug("Error while flushing: " + err.Error())
		return
	}
	n.debug(fmt.Sprintf("Sent a message %v from %d to %d", *(message.RpcPair), message.From, message.To))
}

func (n *Network) debug(s string) {
	if n.debugLevel > 0 {
		fmt.Print(s + "\n")
	}
}
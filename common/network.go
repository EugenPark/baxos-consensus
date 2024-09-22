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
	From int32
	To int32
	RpcPair *RPCPair
}

type RPCConfig struct {
	MsgObj Serializable
	Code uint8
}

type Node struct {
	address string
	region  string
	outgoingWriter *bufio.Writer
	outgoingWriterMutex *sync.Mutex
	incomingReader *bufio.Reader
}

type Application struct {
	id int32
	incomingChan chan<- Message
	outgoingChan <-chan Message
}

type Network struct {
	app Application
	artificialLatency int
	artificialLatencyMultiplier int
	nodes map[int32]*Node
	rpcTable map[uint8]*RPCPair
	debugOn bool
}

/*
	Fill the RPC table by assigning a unique id to each message type
*/

func (n *Network) RegisterRPC(msgObj Serializable, code uint8) {
	n.debug("Registering RPC "+strconv.Itoa(int(code)))
	n.rpcTable[code] = &RPCPair{Code: code, Obj: msgObj}
}

func NewNetwork(id int32, debugOn bool, artificialLatency int, artificialLatencyMultiplier int, outgoingChan <-chan Message, incomingChan chan<- Message) *Network {
	return &Network{
		app: Application{
			id: id,
			incomingChan: incomingChan,
			outgoingChan: outgoingChan,
		},
		artificialLatency: artificialLatency,
		artificialLatencyMultiplier: artificialLatencyMultiplier,
		nodes: make(map[int32]*Node),
		rpcTable: make(map[uint8]*RPCPair),
		debugOn: debugOn,
	}
}

func (n *Network) Init(rpcToRegister []RPCConfig, config *InstanceConfig)  {
	for i := 0; i < len(config.Clients); i++ {
		id, _ := strconv.ParseInt(config.Clients[i].Id, 10, 32)
		n.nodes[int32(id)] = &Node{
			address: config.Clients[i].Domain + ":" + config.Clients[i].Port,
			region:  config.Clients[i].Region,
			outgoingWriterMutex: &sync.Mutex{},
		}
	}

	for i := 0; i < len(config.Replicas); i++ {
		id, _ := strconv.ParseInt(config.Replicas[i].Id, 10, 32)
		n.nodes[int32(id)] = &Node{
			address: config.Replicas[i].Domain + ":" + config.Replicas[i].Port,
			region:  config.Replicas[i].Region,
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

func (n *Network) ConnectToNode(sender int32, receiver *Node) {
	var b [4]byte
	bs := b[:4]

	conn, err := net.Dial("tcp", receiver.address)
	if err != nil {
		panic("Error while connecting to node " + receiver.address + " " + err.Error())
	}

	receiver.outgoingWriter = bufio.NewWriter(conn)
	binary.LittleEndian.PutUint16(bs, uint16(sender))
	_, err = conn.Write(bs)
	if err != nil {
		panic("Error while writing to node " + receiver.address)
	}
}

func (n *Network) ConnectToAllNodes() {
	for _, receiver := range n.nodes {
		n.ConnectToNode(n.app.id, receiver)
	}
}

/*
	listen to a given connection reader. Upon receiving any message, put it into the central incoming buffer
*/

func (n *Network) ConnectionHandler(reader *bufio.Reader, from int32) {

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
			n.debug("Error received unknown message type " + strconv.Itoa(int(msgType)) + " from "+strconv.Itoa(int(from)))
			return
		}

		obj := rpair.Obj.New()
		err = obj.Unmarshal(reader)
		if err != nil {
			n.debug(fmt.Sprintf("Error while unmarshalling from %d %v", from, err.Error()))
			return
		}

		rpcPair := &RPCPair{
			Code: msgType,
			Obj:  obj,
		}

		msg := Message {
			From: from,
			To: n.app.id,
			RpcPair: rpcPair,
		}
		
		n.app.incomingChan <- msg
		n.debug(fmt.Sprintf("Received a message %v from %d", *msg.RpcPair, from))
	}
}

/*
	listen on the port for new connections from clients and replicas
*/

func (n *Network) StartListenServer() {
	var b [4]byte
	bs := b[:4]
	Listener, err := net.Listen("tcp", n.nodes[n.app.id].address)

	if err != nil {
		panic("Error while listening to incoming connections")
	}

	n.debug("Listening to incoming connections in " + n.nodes[n.app.id].address)

	for {
		conn, err := Listener.Accept()
		if err != nil {
			panic(fmt.Sprintf("%v", err.Error()))
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			panic(fmt.Sprintf("%v", err.Error()))
		}
		id := int32(binary.LittleEndian.Uint16(bs))
		n.debug("Received incoming connection from "+strconv.Itoa(int(id)))
		n.nodes[id].incomingReader = bufio.NewReader(conn)

		go n.ConnectionHandler(n.nodes[id].incomingReader, id)
		n.debug("Started listening to "+ strconv.Itoa(int(id)))
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
	latency := time.Duration(n.artificialLatency) * time.Microsecond
	increasedLatency := time.Duration(n.artificialLatency * n.artificialLatencyMultiplier) * time.Microsecond

	from := n.nodes[message.From]
	to := n.nodes[message.To]

	// artificial latency to simulate network delay in geo-distributed systems
	if message.From == message.To {
		time.Sleep(0)
	} else if from.region == to.region {
		time.Sleep(latency)
	} else {
		time.Sleep(increasedLatency)
	}

	w := to.outgoingWriter
	if w == nil {
		n.debug(fmt.Sprintf("Outgoing Writer %v", to.outgoingWriter))
		panic("Node not found " + strconv.Itoa(int(message.To)))
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
		n.debug("Error while marshalling:"+err.Error())
		return
	}
	err = w.Flush()
	if err != nil {
		n.debug("Error while flushing:"+err.Error())
		return
	}
	n.debug(fmt.Sprintf("Sent a message %v from %d to %d",*(message.RpcPair), message.From, message.To))
}

func (n *Network) debug(s string) {
	if n.debugOn {
		fmt.Print(s + "\n")
	}
}